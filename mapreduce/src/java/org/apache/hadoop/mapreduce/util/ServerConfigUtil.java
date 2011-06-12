package org.apache.hadoop.mapreduce.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;
import org.apache.hadoop.NodeHealthCheckerService;

/**
 * Place holder for deprecated keys in the framework 
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ServerConfigUtil {


  /**
   * Adds all the deprecated keys. Loads mapred-default.xml and mapred-site.xml
   */
  public static void loadResources() {
    addDeprecatedKeys();
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }
  
  /**
   * Adds deprecated keys and the corresponding new keys to the Configuration
   */
  private static void addDeprecatedKeys()  {
    Configuration.addDeprecation("mapred.temp.dir", 
      new String[] {MRConfig.TEMP_DIR});
    Configuration.addDeprecation("mapred.local.dir", 
      new String[] {MRConfig.LOCAL_DIR});
    Configuration.addDeprecation("mapred.cluster.map.memory.mb", 
      new String[] {MRConfig.MAPMEMORY_MB});
    Configuration.addDeprecation("mapred.cluster.reduce.memory.mb", 
      new String[] {MRConfig.REDUCEMEMORY_MB});
    Configuration.addDeprecation("mapred.acls.enabled", 
        new String[] {MRConfig.MR_ACLS_ENABLED});

    Configuration.addDeprecation("mapred.cluster.max.map.memory.mb", 
      new String[] {JTConfig.JT_MAX_MAPMEMORY_MB});
    Configuration.addDeprecation("mapred.cluster.max.reduce.memory.mb", 
      new String[] {JTConfig.JT_MAX_REDUCEMEMORY_MB});

    Configuration.addDeprecation("mapred.cluster.average.blacklist.threshold", 
      new String[] {JTConfig.JT_AVG_BLACKLIST_THRESHOLD});
    Configuration.addDeprecation("hadoop.job.history.location", 
      new String[] {JTConfig.JT_JOBHISTORY_LOCATION});
    Configuration.addDeprecation(
      "mapred.job.tracker.history.completed.location", 
      new String[] {JTConfig.JT_JOBHISTORY_COMPLETED_LOCATION});
    Configuration.addDeprecation("mapred.jobtracker.job.history.block.size", 
      new String[] {JTConfig.JT_JOBHISTORY_BLOCK_SIZE});
    Configuration.addDeprecation("mapred.job.tracker.jobhistory.lru.cache.size", 
      new String[] {JTConfig.JT_JOBHISTORY_CACHE_SIZE});
    Configuration.addDeprecation("mapred.hosts", 
      new String[] {JTConfig.JT_HOSTS_FILENAME});
    Configuration.addDeprecation("mapred.hosts.exclude", 
      new String[] {JTConfig.JT_HOSTS_EXCLUDE_FILENAME});
    Configuration.addDeprecation("mapred.system.dir", 
      new String[] {JTConfig.JT_SYSTEM_DIR});
    Configuration.addDeprecation("mapred.max.tracker.blacklists", 
      new String[] {JTConfig.JT_MAX_TRACKER_BLACKLISTS});
    Configuration.addDeprecation("mapred.job.tracker.http.address", 
      new String[] {JTConfig.JT_HTTP_ADDRESS});
    Configuration.addDeprecation("mapred.job.tracker.handler.count", 
      new String[] {JTConfig.JT_IPC_HANDLER_COUNT});
    Configuration.addDeprecation("mapred.jobtracker.restart.recover", 
      new String[] {JTConfig.JT_RESTART_ENABLED});
    Configuration.addDeprecation("mapred.jobtracker.taskScheduler", 
      new String[] {JTConfig.JT_TASK_SCHEDULER});
    Configuration.addDeprecation(
      "mapred.jobtracker.taskScheduler.maxRunningTasksPerJob", 
      new String[] {JTConfig.JT_RUNNINGTASKS_PER_JOB});
    Configuration.addDeprecation("mapred.jobtracker.instrumentation", 
      new String[] {JTConfig.JT_INSTRUMENTATION});
    Configuration.addDeprecation("mapred.jobtracker.maxtasks.per.job", 
      new String[] {JTConfig.JT_TASKS_PER_JOB});
    Configuration.addDeprecation("mapred.heartbeats.in.second", 
      new String[] {JTConfig.JT_HEARTBEATS_IN_SECOND});
    Configuration.addDeprecation("mapred.job.tracker.persist.jobstatus.active", 
      new String[] {JTConfig.JT_PERSIST_JOBSTATUS});
    Configuration.addDeprecation("mapred.job.tracker.persist.jobstatus.hours", 
      new String[] {JTConfig.JT_PERSIST_JOBSTATUS_HOURS});
    Configuration.addDeprecation("mapred.job.tracker.persist.jobstatus.dir", 
      new String[] {JTConfig.JT_PERSIST_JOBSTATUS_DIR});
    Configuration.addDeprecation("mapred.permissions.supergroup", 
      new String[] {MRConfig.MR_SUPERGROUP});
    Configuration.addDeprecation("mapreduce.jobtracker.permissions.supergroup",
        new String[] {MRConfig.MR_SUPERGROUP});
    Configuration.addDeprecation("mapred.task.cache.levels", 
      new String[] {JTConfig.JT_TASKCACHE_LEVELS});
    Configuration.addDeprecation("mapred.jobtracker.taskalloc.capacitypad", 
      new String[] {JTConfig.JT_TASK_ALLOC_PAD_FRACTION});
    Configuration.addDeprecation("mapred.jobinit.threads", 
      new String[] {JTConfig.JT_JOBINIT_THREADS});
    Configuration.addDeprecation("mapred.tasktracker.expiry.interval", 
      new String[] {JTConfig.JT_TRACKER_EXPIRY_INTERVAL});
    Configuration.addDeprecation("mapred.job.tracker.retiredjobs.cache.size", 
      new String[] {JTConfig.JT_RETIREJOB_CACHE_SIZE});
    Configuration.addDeprecation("mapred.job.tracker.retire.jobs", 
      new String[] {JTConfig.JT_RETIREJOBS});
    Configuration.addDeprecation("mapred.healthChecker.interval", 
      new String[] {NodeHealthCheckerService.HEALTH_CHECK_INTERVAL_PROPERTY});
    Configuration
        .addDeprecation(
            "mapred.healthChecker.script.args",
            new String[] {
                NodeHealthCheckerService.HEALTH_CHECK_SCRIPT_ARGUMENTS_PROPERTY
            });
    Configuration
        .addDeprecation(
            "mapred.healthChecker.script.path",
            new String[] {
                NodeHealthCheckerService.HEALTH_CHECK_SCRIPT_PROPERTY });
    Configuration
        .addDeprecation(
            "mapred.healthChecker.script.timeout",
            new String[] {
                NodeHealthCheckerService.HEALTH_CHECK_FAILURE_INTERVAL_PROPERTY
            });
    Configuration.addDeprecation("mapred.local.dir.minspacekill", 
      new String[] {TTConfig.TT_LOCAL_DIR_MINSPACE_KILL});
    Configuration.addDeprecation("mapred.local.dir.minspacestart", 
      new String[] {TTConfig.TT_LOCAL_DIR_MINSPACE_START});
    Configuration.addDeprecation("mapred.task.tracker.http.address", 
      new String[] {TTConfig.TT_HTTP_ADDRESS});
    Configuration.addDeprecation("mapred.task.tracker.report.address", 
      new String[] {TTConfig.TT_REPORT_ADDRESS});
    Configuration.addDeprecation("mapred.task.tracker.task-controller", 
      new String[] {TTConfig.TT_TASK_CONTROLLER});
    Configuration.addDeprecation("mapred.tasktracker.dns.interface", 
      new String[] {TTConfig.TT_DNS_INTERFACE});
    Configuration.addDeprecation("mapred.tasktracker.dns.nameserver", 
      new String[] {TTConfig.TT_DNS_NAMESERVER});
    Configuration.addDeprecation("mapred.tasktracker.events.batchsize", 
      new String[] {TTConfig.TT_MAX_TASK_COMPLETION_EVENTS_TO_POLL});
    Configuration.addDeprecation("mapred.tasktracker.instrumentation", 
      new String[] {TTConfig.TT_INSTRUMENTATION});
    Configuration.addDeprecation("mapred.tasktracker.map.tasks.maximum", 
      new String[] {TTConfig.TT_MAP_SLOTS});
    Configuration.addDeprecation("mapred.tasktracker.reduce.tasks.maximum", 
      new String[] {TTConfig.TT_REDUCE_SLOTS});
    Configuration.addDeprecation(
      "mapred.tasktracker.taskmemorymanager.monitoring-interval", 
      new String[] {TTConfig.TT_MEMORY_MANAGER_MONITORING_INTERVAL});
    Configuration.addDeprecation(
      "mapred.tasktracker.tasks.sleeptime-before-sigkill", 
      new String[] {TTConfig.TT_SLEEP_TIME_BEFORE_SIG_KILL});
    Configuration.addDeprecation("slave.host.name", 
      new String[] {TTConfig.TT_HOST_NAME});
    Configuration.addDeprecation("tasktracker.http.threads", 
      new String[] {TTConfig.TT_HTTP_THREADS});
    Configuration.addDeprecation("local.cache.size", 
      new String[] {TTConfig.TT_LOCAL_CACHE_SIZE});
    Configuration.addDeprecation("tasktracker.contention.tracking", 
      new String[] {TTConfig.TT_CONTENTION_TRACKING});
    Configuration
        .addDeprecation(
            "mapreduce.tasktracker.healthchecker.interval",
            new String[] {
                NodeHealthCheckerService.HEALTH_CHECK_INTERVAL_PROPERTY });
    Configuration
        .addDeprecation(
            "mapreduce.tasktracker.healthchecker.script.args",
            new String[] {
                NodeHealthCheckerService.HEALTH_CHECK_SCRIPT_ARGUMENTS_PROPERTY
            });
    Configuration
        .addDeprecation(
            "mapreduce.tasktracker.healthchecker.script.path",
            new String[] {
                NodeHealthCheckerService.HEALTH_CHECK_SCRIPT_PROPERTY });
    Configuration
        .addDeprecation(
            "mapreduce.tasktracker.healthchecker.script.timeout",
            new String[] {
                NodeHealthCheckerService.HEALTH_CHECK_FAILURE_INTERVAL_PROPERTY
            });
  }
}
