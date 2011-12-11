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
package org.apache.hadoop.tools.rumen;

import java.util.regex.Pattern;

import org.apache.hadoop.mapreduce.JobID;

/**
 * 
 *
 */
public class Pre21JobHistoryConstants {
  
  /**
   * Job history files contain key="value" pairs, where keys belong to this enum. 
   * It acts as a global namespace for all keys. 
   */
  static enum Keys {
    JOBTRACKERID,
    START_TIME, FINISH_TIME, JOBID, JOBNAME, USER, JOBCONF, SUBMIT_TIME,
    LAUNCH_TIME, TOTAL_MAPS, TOTAL_REDUCES, FAILED_MAPS, FAILED_REDUCES,
    FINISHED_MAPS, FINISHED_REDUCES, JOB_STATUS, TASKID, HOSTNAME, TASK_TYPE,
    ERROR, TASK_ATTEMPT_ID, TASK_STATUS, COPY_PHASE, SORT_PHASE, REDUCE_PHASE,
    SHUFFLE_FINISHED, SORT_FINISHED, MAP_FINISHED, COUNTERS, SPLITS,
    JOB_PRIORITY, HTTP_PORT, TRACKER_NAME, STATE_STRING, VERSION
  }

  /**
   * This enum contains some of the values commonly used by history log events. 
   * since values in history can only be strings - Values.name() is used in 
   * most places in history file. 
   */
  public static enum Values {
    SUCCESS, FAILED, KILLED, MAP, REDUCE, CLEANUP, RUNNING, PREP, SETUP
  }
  
  /**
   * Pre21 regex for jobhistory filename 
   *   i.e jt-identifier_job-id_user-name_job-name
   */
  static final Pattern JOBHISTORY_FILENAME_REGEX =
    Pattern.compile("[^.].+_(" + JobID.JOBID_REGEX + ")_.+");

  /**
   * Pre21 regex for jobhistory conf filename 
   *   i.e jt-identifier_job-id_conf.xml
   */
  static final Pattern CONF_FILENAME_REGEX =
    Pattern.compile("[^.].+_(" + JobID.JOBID_REGEX 
                    + ")_conf.xml(?:\\.[0-9a-zA-Z]+)?");
 
}
