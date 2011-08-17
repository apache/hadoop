/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce.server.jobtracker;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.MRConfig;

/**
 * Place holder for JobTracker server-level configuration.
 * 
 * The keys should have "mapreduce.jobtracker." as the prefix
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface JTConfig extends MRConfig {
  // JobTracker configuration parameters
  public static final String JT_IPC_ADDRESS  = "mapreduce.jobtracker.address";
  public static final String JT_HTTP_ADDRESS = 
    "mapreduce.jobtracker.http.address";
  public static final String JT_IPC_HANDLER_COUNT = 
    "mapreduce.jobtracker.handler.count";
  public static final String JT_RESTART_ENABLED = 
    "mapreduce.jobtracker.restart.recover";
  public static final String JT_TASK_SCHEDULER = 
    "mapreduce.jobtracker.taskscheduler";
  public static final String JT_INSTRUMENTATION = 
    "mapreduce.jobtracker.instrumentation";
  public static final String JT_TASKS_PER_JOB = 
    "mapreduce.jobtracker.maxtasks.perjob";
  public static final String JT_HEARTBEATS_IN_SECOND = 
    "mapreduce.jobtracker.heartbeats.in.second";
  public static final String JT_HEARTBEATS_SCALING_FACTOR = 
    "mapreduce.jobtracker.heartbeats.scaling.factor";
  public static final String JT_HEARTBEAT_INTERVAL_MIN =
    "mapreduce.jobtracker.heartbeat.interval.min";
  public static final int JT_HEARTBEAT_INTERVAL_MIN_DEFAULT = 300;
  public static final String JT_PERSIST_JOBSTATUS = 
    "mapreduce.jobtracker.persist.jobstatus.active";
  public static final String JT_PERSIST_JOBSTATUS_HOURS = 
    "mapreduce.jobtracker.persist.jobstatus.hours";
  public static final String JT_PERSIST_JOBSTATUS_DIR = 
    "mapreduce.jobtracker.persist.jobstatus.dir";

  /**
   * @deprecated Use MR_SUPERGROUP instead
   */
  @Deprecated
  public static final String JT_SUPERGROUP = 
    "mapreduce.jobtracker.permissions.supergroup";
  public static final String JT_RETIREJOBS = 
    "mapreduce.jobtracker.retirejobs";
  public static final String JT_RETIREJOB_CACHE_SIZE = 
    "mapreduce.jobtracker.retiredjobs.cache.size";
  public static final String JT_TASKCACHE_LEVELS = 
    "mapreduce.jobtracker.taskcache.levels";
  public static final String JT_TASK_ALLOC_PAD_FRACTION = 
    "mapreduce.jobtracker.taskscheduler.taskalloc.capacitypad";
  public static final String JT_JOBINIT_THREADS = 
    "mapreduce.jobtracker.jobinit.threads";
  public static final String JT_TRACKER_EXPIRY_INTERVAL = 
    "mapreduce.jobtracker.expire.trackers.interval";
  public static final String JT_RUNNINGTASKS_PER_JOB = 
    "mapreduce.jobtracker.taskscheduler.maxrunningtasks.perjob";
  public static final String JT_HOSTS_FILENAME = 
    "mapreduce.jobtracker.hosts.filename";
  public static final String JT_HOSTS_EXCLUDE_FILENAME = 
    "mapreduce.jobtracker.hosts.exclude.filename";
  public static final String JT_JOBHISTORY_CACHE_SIZE =
    "mapreduce.jobtracker.jobhistory.lru.cache.size";
  public static final String JT_JOBHISTORY_BLOCK_SIZE = 
    "mapreduce.jobtracker.jobhistory.block.size";
  public static final String JT_JOBHISTORY_COMPLETED_LOCATION = 
    "mapreduce.jobtracker.jobhistory.completed.location";
  public static final String JT_JOBHISTORY_LOCATION = 
    "mapreduce.jobtracker.jobhistory.location";
  // number of partial task progress reports we retain in job history
  public static final String JT_JOBHISTORY_TASKPROGRESS_NUMBER_SPLITS =
    "mapreduce.jobtracker.jobhistory.task.numberprogresssplits";
  public static final String JT_AVG_BLACKLIST_THRESHOLD = 
    "mapreduce.jobtracker.blacklist.average.threshold";
  public static final String JT_SYSTEM_DIR = "mapreduce.jobtracker.system.dir";
  public static final String JT_STAGING_AREA_ROOT = 
    "mapreduce.jobtracker.staging.root.dir";
  public static final String JT_MAX_TRACKER_BLACKLISTS = 
    "mapreduce.jobtracker.tasktracker.maxblacklists";
  public static final String JT_JOBHISTORY_MAXAGE = 
    "mapreduce.jobtracker.jobhistory.maxage";
  public static final String JT_MAX_MAPMEMORY_MB = 
    "mapreduce.jobtracker.maxmapmemory.mb";
  public static final String JT_MAX_REDUCEMEMORY_MB = 
    "mapreduce.jobtracker.maxreducememory.mb";
  public static final String JT_MAX_JOB_SPLIT_METAINFO_SIZE = 
  "mapreduce.jobtracker.split.metainfo.maxsize";
  public static final String JT_USER_NAME = "mapreduce.jobtracker.kerberos.principal";
  public static final String JT_KEYTAB_FILE = 
    "mapreduce.jobtracker.keytab.file";
  public static final String PRIVATE_ACTIONS_KEY = 
     "mapreduce.jobtracker.webinterface.trusted";
  public static final String JT_PLUGINS = 
    "mapreduce.jobtracker.plugins";
  public static final String SHUFFLE_EXCEPTION_STACK_REGEX =
    "mapreduce.reduce.shuffle.catch.exception.stack.regex";
  public static final String SHUFFLE_EXCEPTION_MSG_REGEX =
    "mapreduce.reduce.shuffle.catch.exception.message.regex";

}
