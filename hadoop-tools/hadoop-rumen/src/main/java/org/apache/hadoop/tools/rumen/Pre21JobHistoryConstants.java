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
import org.apache.hadoop.mapreduce.v2.hs.JobHistory;

/**
 * Job History related constants for Hadoop releases prior to 0.21
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
   * Regex for Pre21 V1(old) jobhistory filename
   *   i.e jt-identifier_job-id_user-name_job-name
   */
  static final Pattern JOBHISTORY_FILENAME_REGEX_V1 =
    Pattern.compile("[^.].+_(" + JobID.JOBID_REGEX + ")_.+");
  /**
   * Regex for Pre21 V2(new) jobhistory filename
   *   i.e job-id_user-name_job-name
   */
  static final Pattern JOBHISTORY_FILENAME_REGEX_V2 =
    Pattern.compile("(" + JobID.JOBID_REGEX + ")_.+");

  static final String OLD_FULL_SUFFIX_REGEX_STRING =
    "(?:\\.[0-9]+" + Pattern.quote(JobHistory.OLD_SUFFIX) + ")";

  /**
   * Regex for Pre21 V1(old) jobhistory conf filename 
   *   i.e jt-identifier_job-id_conf.xml
   */
  static final Pattern CONF_FILENAME_REGEX_V1 =
    Pattern.compile("[^.].+_(" + JobID.JOBID_REGEX + ")_conf.xml"
                    + OLD_FULL_SUFFIX_REGEX_STRING + "?");
  /**
   * Regex for Pre21 V2(new) jobhistory conf filename
   *   i.e job-id_conf.xml
   */
  static final Pattern CONF_FILENAME_REGEX_V2 =
    Pattern.compile("(" + JobID.JOBID_REGEX + ")_conf.xml"
                    + OLD_FULL_SUFFIX_REGEX_STRING + "?");
 
}
