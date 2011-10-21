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

package org.apache.hadoop.mapreduce.v2.jobhistory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Stores Job History configuration keys that can be set by administrators of
 * the Job History server.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class JHAdminConfig {
  /** The prefix to all Job History configuration properties.*/
  public static final String MR_HISTORY_PREFIX = "mapreduce.jobhistory.";
  
  /** host:port address for History Server API.*/
  public static final String MR_HISTORY_ADDRESS = MR_HISTORY_PREFIX + "address";
  public static final int DEFAULT_MR_HISTORY_PORT = 10020;
  public static final String DEFAULT_MR_HISTORY_ADDRESS = "0.0.0.0:" +
      DEFAULT_MR_HISTORY_PORT;
  
  /** If history cleaning should be enabled or not.*/
  public static final String MR_HISTORY_CLEANER_ENABLE = 
    MR_HISTORY_PREFIX + "cleaner.enable";
  
  /** Run the History Cleaner every X ms.*/
  public static final String MR_HISTORY_CLEANER_INTERVAL_MS = 
    MR_HISTORY_PREFIX + "cleaner.interval-ms";
  
  /** The number of threads to handle client API requests.*/
  public static final String MR_HISTORY_CLIENT_THREAD_COUNT = 
    MR_HISTORY_PREFIX + "client.thread-count";
  public static final int DEFAULT_MR_HISTORY_CLIENT_THREAD_COUNT = 10;
  
  /**
   * Size of the date string cache. Effects the number of directories
   * which will be scanned to find a job.
   */
  public static final String MR_HISTORY_DATESTRING_CACHE_SIZE = 
    MR_HISTORY_PREFIX + "datestring.cache.size";
  
  /** Equivalent to 0.20 mapreduce.jobhistory.debug.mode */
  public static final String MR_HISTORY_DEBUG_MODE = 
    MR_HISTORY_PREFIX + "debug-mode";
  
  /** Path where history files should be stored for DONE jobs. **/
  public static final String MR_HISTORY_DONE_DIR =
    MR_HISTORY_PREFIX + "done-dir";

  /**
   *  Path where history files should be stored after a job finished and before
   *  they are pulled into the job history server.
   **/
  public static final String MR_HISTORY_INTERMEDIATE_DONE_DIR =
    MR_HISTORY_PREFIX + "intermediate-done-dir";
  
  /** Size of the job list cache.*/
  public static final String MR_HISTORY_JOBLIST_CACHE_SIZE =
    MR_HISTORY_PREFIX + "joblist.cache.size";
  
  /** The location of the Kerberos keytab file.*/
  public static final String MR_HISTORY_KEYTAB = MR_HISTORY_PREFIX + "keytab";
  
  /** Size of the loaded job cache.*/
  public static final String MR_HISTORY_LOADED_JOB_CACHE_SIZE = 
    MR_HISTORY_PREFIX + "loadedjobs.cache.size";
  
  /**
   * The maximum age of a job history file before it is deleted from the history
   * server.
   */
  public static final String MR_HISTORY_MAX_AGE_MS =
    MR_HISTORY_PREFIX + "max-age-ms";
  
  /**
   * Scan for history files to more from intermediate done dir to done dir
   * every X ms.
   */
  public static final String MR_HISTORY_MOVE_INTERVAL_MS = 
    MR_HISTORY_PREFIX + "move.interval-ms";
  
  /** The number of threads used to move files.*/
  public static final String MR_HISTORY_MOVE_THREAD_COUNT = 
    MR_HISTORY_PREFIX + "move.thread-count";
  
  /** The Kerberos principal for the history server.*/
  public static final String MR_HISTORY_PRINCIPAL = 
    MR_HISTORY_PREFIX + "principal";
  
  /**The address the history server webapp is on.*/
  public static final String MR_HISTORY_WEBAPP_ADDRESS =
    MR_HISTORY_PREFIX + "webapp.address";
  public static final int DEFAULT_MR_HISTORY_WEBAPP_PORT = 19888;
  public static final String DEFAULT_MR_HISTORY_WEBAPP_ADDRESS =
    "0.0.0.0:" + DEFAULT_MR_HISTORY_WEBAPP_PORT;
}
