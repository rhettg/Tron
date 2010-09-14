import sys
import re
import logging
import weakref
import datetime
import os

import yaml
from twisted.conch.client import options

from tron import action, job, node, scheduler, monitor, emailer, command_context

log = logging.getLogger("tron.config")

class Error(Exception):
    pass
    
class ConfigError(Exception):
    pass

# If a configuration is not provided when trond starts, here is what we begin with.
# The user can then use tronfig command to customize their installation.
DEFAULT_CONFIG = """--- !TronConfiguration

ssh_options:
    ## Tron needs SSH keys to allow the effective user to login to each of the nodes specified
    ## in the "nodes" section. You can choose to use either an SSH agent or list 
    # identities:
    #     - /home/tron/.ssh/id_dsa
    agent: true
    

# notification_options:
      ## In case of trond failures, where should we send notifications to ?
      # smtp_host: localhost
      # notification_addr: nobody@localhost

nodes:
    ## You'll need to list out all the available nodes for doing work.
    # - &node
    #     hostname: 'localhost'
    
    ## Optionally you can list 'pools' of nodes where selection of a node will be randomly
    ## determined or jobs can be configured to be run on all nodes in the pool
    # - &all_nodes !NodePool
    #     nodes: [*node]

jobs:
    ## Configure your jobs here by specifing a name, node, schedule and the work flow that should executed.
    # - &sample_job
    #     name: "sample_job"
    #     node: *node
    #     schedule: "daily"
    #     actions:
    #         -
    #             name: "uname"
    #             command: "uname -a"

services:
    ## Configure services here. Services differ from jobs in that they are expected to have an enable/disable and monitoring
    ## phase.
    # - &sample_service
    #     name: "sample_service"
    #     node: *node
    #     enable:
    #         command: "echo 'enabled'"
    #     disable:
    #         command: "echo 'disabled'"
    #     monitor:
    #         schedule: "interval 10 mins"
    #         actions:
    #             -
    #                 name: "monitor"
    #                 command: "uptime"

"""

class FromDictBuilderMixin(object):
    """Mixin class for building YAMLObjects from dictionaries"""
    @classmethod
    def from_dict(cls, obj_dict):
        # We just assume we want to make all the dictionary values into attributes
        new_obj = cls()
        new_obj.__dict__.update(obj_dict)
        return new_obj


def default_or_from_tag(value, cls):
    """Construct a YAMLObject unless it already is one
    
    We use this for providing default config types so it isn't required to "tag" everything with !MyConfigClass
    Since YAML may present us the same dictionary a few times thanks to references any actual operations we do
    in this function will be *persisted* by adding a key to the base dictionary with the instance we create
    """
    if not isinstance(value, yaml.YAMLObject):
        # First we check if we've already defaulted this instance before
        if '__obj__' in value:
            classified = value['__obj__']
            if classified:
                return classified
        
        classified = cls.from_dict(value)
        value['__obj__'] = classified
        return classified

    return value

    
class _ConfiguredObject(yaml.YAMLObject, FromDictBuilderMixin):
    """Base class for common configured objects where the configuration generates one actualized 
    object in the app that may be referenced by other objects.
    
    The reason for all this actualization stuff is that we have be able to handle reconfigurations.
    Meaning sometimes we'll be building an object from scratch, sometimes we'll be creating new ones.

    Generally it's up to the caller to find existing instances, as well as do something with actualized object.
    The general flow would be:

        obj = find_existing()
        if obj:
            config_object.update(obj)

        add_new_object(obj.actualized)
    """
    actual_class = None     # Redefined to indicate the type of object this configuration will build
    def update(self, obj):
        """Set and configure the specified existing object"""
        if self._ref:
            raise Error("We already think we have the actualized object")

        self.actualized = obj
        self._apply(obj)
    
    def create(self):
        """Create and configure new instance of the actualized object"""
        obj = self._build()
        self._apply(obj)
        return obj

    def _build(self):
        """Build a new instance of the configured object"""
        return self.actual_class()

    def _apply(self, obj):
        """Apply configuration to the actualized object"""
        raise NotImplementedError

    def __cmp__(self, other):
        if not isinstance(other, self.__class__):
            return -1

        our_dict = [(key, value) for key, value in self.__dict__.iteritems() if not key.startswith('_')]
        other_dict = [(key, value) for key, value in other.__dict__.iteritems() if not key.startswith('_')]
        
        c = cmp(our_dict, other_dict)
        return c

    def __hash__(self):
        raise Exception('hashing')

    def _get_actualized(self):
        if not hasattr(self, '_ref'):
            self._ref = self.create()

        return self._ref

    def _set_actualized(self, val):
        self._ref = obj

    actualized = property(_get_actualized, _set_actualized)


class TronConfiguration(yaml.YAMLObject):
    yaml_tag = u'!TronConfiguration'
    def __init__(self, *args, **kwargs):
        super(TronConfiguration, self).__init__(*args, **kwargs)
        self.nodes = {}

    def _apply_jobs(self, mcp):
        """Configure actions"""
        found_jobs = []

        # Check for duplicates before we start editing jobs
        def check_dup(dic, nex):
            if nex.name in dic:
                raise yaml.YAMLError("%s is previously defined" % nex.name)
            dic[nex.name] = 1
            return dic
        
        jobs = []
        if getattr(self, 'jobs', None):
            jobs = [default_or_from_tag(job_val, Job) for job_val in self.jobs]
        if getattr(self, 'services', None):
            jobs.extend([default_or_from_tag(job_val, Service) for job_val in self.services])

        found_jobs = reduce(check_dup, jobs, {})

        # HEREIAM: Need to find existing jobs and update them
        for job_config in jobs:
            new_job = job_config.actualized
            log.debug("Building new job %s", job_config.name)
            
            # During a reconfig, we are still calling 'add_job' which is going to have intelligence about
            # how to merge to previous and current version of the job. I think might need a little bit of
            # refactoring as reconfig logic should really belong to 'config'.
            mcp.add_job(new_job)

        for job_name in mcp.jobs.keys():
            if job_name not in found_jobs:
                log.debug("Removing job %s", job_name)
                del mcp.jobs[job_name]

    def _get_working_dir(self, mcp):
        if mcp.state_handler.working_dir:
            return mcp.state_handler.working_dir
        if hasattr(self, 'working_dir'):
            return self.working_dir
        if 'TMPDIR' in os.environ:
            return os.environ['TMPDIR']
        return '/tmp'
   
    def apply(self, mcp):
        """Apply the configuration to the specified master control program"""
        working_dir = self._get_working_dir(mcp)
        if not os.path.isdir(working_dir):
            raise ConfigError("Specified working directory \'%s\' is not a directory" % working_dir)
        if not os.access(working_dir, os.W_OK):
            raise ConfigError("Specified working directory \'%s\' is not writable" % working_dir)
        
        mcp.state_handler.working_dir = working_dir

        if hasattr(self, 'command_context'):
            mcp.context = command_context.CommandContext(self.command_context)
        
        self._apply_jobs(mcp)

        if hasattr(self, 'ssh_options'):
            self.ssh_options = default_or_from_tag(self.ssh_options, SSHOptions)
            self.ssh_options.apply(mcp)
        
        if hasattr(self, 'notification_options'):
            self.notification_options = default_or_from_tag(self.notification_options, NotificationOptions)
            self.notification_options.apply(mcp)
        

class SSHOptions(yaml.YAMLObject, FromDictBuilderMixin):
    yaml_tag = u'!SSHOptions'
    
    def _build_conch_options(self):
        """Verify and construct the ssh (conch) option object
        
        This is just a dictionary like object that options the twisted ssh implementation uses.
        """
        ssh_options = options.ConchOptions()
        if not hasattr(self, 'agent'):
            ssh_options['noagent'] = True
        else:
            if 'SSH_AUTH_SOCK' in os.environ:
                ssh_options['agent'] = True
            else:
                raise Error("No SSH Agent available ($SSH_AUTH_SOCK)")

        if hasattr(self, "identities"):
            for file_name in self.identities:
                file_path = os.path.expanduser(file_name)
                if not os.path.exists(file_path):
                    raise Error("Private key file %s doesn't exist" % file_name)
                if not os.path.exists(file_path + ".pub"):
                    raise Error("Public key %s doesn't exist" % (file_name + ".pub"))
            
                ssh_options.opt_identity(file_name)
        
        return ssh_options
        
    def apply(self, mcp):
        options = self._build_conch_options()

        for node in mcp.nodes:
            node.conch_options = options


class NotificationOptions(yaml.YAMLObject, FromDictBuilderMixin):
    yaml_tag = u'!NotificationOptions'
    def apply(self, mcp):
        if not hasattr(self, 'smtp_host'):
            raise Error("smtp_host required")
        if not hasattr(self, 'notification_addr'):
            raise Error("notification_addr required")
        
        em = emailer.Emailer(self.smtp_host, self.notification_addr)
        mcp.monitor = monitor.CrashReporter(em)
        mcp.monitor.start()

 
class Job(_ConfiguredObject):
    yaml_tag = u'!Job'
    actual_class = job.Job

    def _match_name(self, real_job):
        if not re.match(r'[a-z_]\w*$', self.name, re.I):
            raise yaml.YAMLError("Invalid job name '%s' - not a valid identifier" % self.name)
        if real_job.name and real_job.name != self.name:
            raise Error("Can't change job names !?")

        real_job.name = self.name
        
    def _match_schedule(self, real_job):
        # Schedulers have no state, so we won't bother trying to preserv any previously configured one
        if isinstance(schedule, basestring):
            # This is a short string
            real_job.scheduler = Scheduler.from_string(self.schedule)
        else:
            # This is a scheduler instance, which has more info
            real_job.scheduler = self.schedule.actualized

        real_job.scheduler.job_setup(real_job)

    def _match_actions(self, real_job):
        # Store previous action configuration in case of problems
        prev_topo_actions = real_job.topo_actions
        prev_registry = real_job.action_registry

        try:
            real_job.reset_actions()
            
            for action_conf in self.actions:
                action = default_or_from_tag(action_conf, Action)

                # Let's see if we can find this existing action
                prev_action = prev_registry.get(action.name)
                if prev_action:
                    action.update(prev_action)

                real_action = action.actualized
                if not real_job.node_pool and not real_action.node_pool:
                    raise yaml.YAMLError("Either job '%s' or its action '%s' must have a node" 
                       % (real_job.name, action_action.name))

                real_job.add_action(real_action)
        except Exception:
            # Restore previous action config
            real_job.topo_actions = prev_topo_actions
            real_job.action_registry = prev_registry
            raise

    def _match_node(self, real_job):
        node = default_or_from_tag(self.node, Node)
        if not isinstance(node, NodePool):
            node_pool = NodePool()
            node_pool.nodes.append(node)
        else:
            node_pool = node
            
        real_job.node_pool = node_pool.actualized
                    
    def _apply(self, real_job):
        self._match_name(real_job)
        self._match_node(real_job)
        self._match_schedule(real_job)
        self._match_actions(real_job)

        if hasattr(self, "queueing"):
            real_job.queueing = self.queueing

        if hasattr(self, "run_limit"):
            real_job.run_limit = self.run_limit

        if hasattr(self, "all_nodes"):
            real_job.all_nodes = self.all_nodes


class Service(Job):
    yaml_tag = u'!Service'
    actual_class = job.Job

    def _apply(self, real_service):
        self.schedule = self.monitor['schedule']
        self.actions = self.monitor['actions']

        self._match_name(real_service)
        self._match_node(real_service)
        self._match_schedule(real_service)
        self._match_actions(real_service)

        if hasattr(self, "enable"):
            enable = default_or_from_tag(self.enable, Action)
            enable.name = enable.name or "enable"
            if real_service.enable_act:
                enable.update(real_service.enable_act)
            else:
                real_service.set_enable_action(enable.actualized)
        else:
            real_service.enable_act = None
            
        if hasattr(self, "disable"):
            disable = default_or_from_tag(self.disable, Action)
            disable.name = disable.name or "disable"
            
            if real_service.disable_act:
                disable.update(real_service.disable_act)
            else:
                real_service.set_disable_action(disable.actualized)
        else:
            real_service.disable_act = None

class Action(_ConfiguredObject):
    yaml_tag = u'!Action'
    actual_class = action.Action

    def __init__(self, *args, **kwargs):
        super(Action, self).__init__(*args, **kwargs)
        self.name = None
        self.command = None
    
    def _apply_requirements(self, real_action, requirements):
        if not isinstance(requirements, list):
            requirements = [requirements]

        requirements = [default_or_from_tag(req, Action) for req in requirements]
        for req in requirements:
            real_action.required_actions.append(req.actualized)

    def _apply(self, real_action):
        """Configured the specific action instance"""
        if not re.match(r'[a-z_]\w*$', self.name, re.I):
            raise yaml.YAMLError("Invalid action name '%s' - not a valid identifier" % self.name)

        real_action.name = self.name
        if real_action.name != self.name:
            raise Error("Can't change name of an action")

        real_action.command = self.command

        if hasattr(self, "node"):
            # TODO: This node matching code is going to be the same as for a job, figure out how to refactor
            node = default_or_from_tag(self.node, Node)
            
            if not isinstance(node, NodePool):
                node_pool = NodePool()
                node_pool.nodes.append(node)
            else:
                node_pool = node
                
            real_action.node_pool = node_pool.actualized
        else:
            real_action.node_pool = None
        
        if hasattr(self, "requires"):
            self._apply_requirements(real_action, self.requires)
        else:
            real_action.required_actions = None

class NodePool(_ConfiguredObject):
    yaml_tag = u'!NodePool'
    actual_class = node.NodePool

    def _apply(self, real_node_pool):
        existing_nodes = getattr(real_node_pool, "nodes", [])
        try:

            real_node_pool.nodes = []
            for node in self.nodes:
                # All these nodes should already be created, so we don't need to update with the
                # real versions here
                real_node = default_or_from_tag(node, Node).actualized
                real_node_pool.nodes.append(real_node)

        except Exception:
            real_node_pool.nodes = existing_nodes
            raise
        
class Node(_ConfiguredObject):
    yaml_tag = u'!Node'
    actual_class = node.Node
    
    def _apply(self, real_node):
        if real_node.hostname and real_node.hostname != self.hostname:
            raise Error("Can't change hostname !?")

        real_node.hostname = self.hostname


class NodeResource(yaml.YAMLObject):
    yaml_tag = u'!NodeResource'


class ActionResource(yaml.YAMLObject):
    yaml_tag = u'!ActionResource'


class FileResource(yaml.YAMLObject):
    yaml_tag = u'!FileResource'


class Scheduler(object):
    @classmethod
    def from_string(self, scheduler_str):
        scheduler_args = scheduler_str.split()
        
        scheduler_name = scheduler_args.pop(0)
        
        if scheduler_name == "constant":
            return ConstantScheduler().actualized
        if scheduler_name == "daily":
            return DailyScheduler(*scheduler_args).actualized
        if scheduler_name == "interval":
            return IntervalScheduler(''.join(scheduler_args)).actualized

        raise Error("Unknown scheduler %r" % scheduler_str)



class ConstantScheduler(_ConfiguredObject):
    yaml_tab = u'!ConstantScheduler'
    actual_class = scheduler.ConstantScheduler
    
    def _apply(self):
        sched = self._ref()


# Shortcut values for intervals
TIME_INTERVAL_SHORTCUTS = {
    'hourly': dict(hours=1),
}

# Translations from possible configuration units to the argument to datetime.timedelta
TIME_INTERVAL_UNITS = {
    'months': ['month', 'months'],
    'days': ['d', 'day', 'days'],
    'hours': ['h', 'hr', 'hrs', 'hour', 'hours'],
    'minutes': ['m', 'min', 'mins', 'minute', 'minutes'],
    'seconds': ['s', 'sec', 'secs', 'second', 'seconds']
}

class IntervalScheduler(_ConfiguredObject):
    yaml_tag = u'!IntervalScheduler'
    actual_class = scheduler.IntervalScheduler
    def __init__(self, *args, **kwargs):
        if len(args) > 0:
            self.interval = args[0]

        super(IntervalScheduler, self).__init__(*args, **kwargs)

    def _apply(self, sched):
        # Now let's figure out the interval
        if self.interval in TIME_INTERVAL_SHORTCUTS:
            kwargs = TIME_INTERVAL_SHORTCUTS[self.interval]
        else:
            # We want to split out digits and characters into tokens
            interval_tokens = re.compile(r"\d+|[a-zA-Z]+").findall(self.interval)
            if len(interval_tokens) != 2:
                raise Error("Invalid interval specification: %r", self.interval)

            value, units = interval_tokens
        
            kwargs = {}
            for key, unit_set in TIME_INTERVAL_UNITS.iteritems():
                if units in unit_set:
                    kwargs[key] = int(value)
                    break
            else:
                raise Error("Invalid interval specification: %r", self.interval)
                
        sched.interval = datetime.timedelta(**kwargs)
        

class DailyScheduler(_ConfiguredObject):
    yaml_tag = u'!DailyScheduler'
    actual_class = scheduler.DailyScheduler
    def __init__(self, *args, **kwargs):

        if len(args) > 0:
            self.start_time = args[0]

        if len(args) > 1:
            self.days = args[1]
        if 'days' in kwargs:
            self.days = kwargs['days']

        super(DailyScheduler, self).__init__(*args, **kwargs)

    def _apply(self, sched):
        if hasattr(self, 'start_time'):
            if not isinstance(self.start_time, basestring):
                raise ConfigError("Start time must be in string format HH:MM:SS")

            hour, minute, second = [int(val) for val in self.start_time.strip().split(':')]
            sched.start_time = datetime.time(hour=hour, minute=minute, second=second)

        if hasattr(self, 'days'):
            sched.wait_days = sched.get_daily_waits(self.days)


def load_config(config_file):
    """docstring for load_config"""
    config = yaml.load(config_file)
    if not isinstance(config, TronConfiguration):
        raise ConfigError("Failed to find a configuration document in specified file")
    
    return config

def configure_daemon(path, daemon):
    config = load_config(path)
    config.apply(daemon)

