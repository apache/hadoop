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
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.http.HttpConfig;

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
  public static final String MR_HISTORY_BIND_HOST = MR_HISTORY_PREFIX
      + "bind-host";

  /** The address of the History server admin interface. */
  public static final String JHS_ADMIN_ADDRESS = MR_HISTORY_PREFIX
      + "admin.address";
  public static final int DEFAULT_JHS_ADMIN_PORT = 10033;
  public static final String DEFAULT_JHS_ADMIN_ADDRESS = "0.0.0.0:"
      + DEFAULT_JHS_ADMIN_PORT;

  /** ACL of who can be admin of Job history server. */
  public static final String JHS_ADMIN_ACL = MR_HISTORY_PREFIX + "admin.acl";
  public static final String DEFAULT_JHS_ADMIN_ACL = "*";
  
  /** If history cleaning should be enabled or not.*/
  public static final String MR_HISTORY_CLEANER_ENABLE = 
    MR_HISTORY_PREFIX + "cleaner.enable";
  
  /** Run the History Cleaner every X ms.*/
  public static final String MR_HISTORY_CLEANER_INTERVAL_MS = 
    MR_HISTORY_PREFIX + "cleaner.interval-ms";
  public static final long DEFAULT_MR_HISTORY_CLEANER_INTERVAL_MS = 
    1 * 24 * 60 * 60 * 1000l; //1 day
  
  
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
  public static final int DEFAULT_MR_HISTORY_DATESTRING_CACHE_SIZE = 200000;
  
  /** Path where history files should be stored for DONE jobs. **/
  public static final String MR_HISTORY_DONE_DIR =
    MR_HISTORY_PREFIX + "done-dir";

  /**
   * Maximum time the History server will wait for the FileSystem for History
   * files to become available. Default value is -1, forever.
   */
  public static final String MR_HISTORY_MAX_START_WAIT_TIME =
      MR_HISTORY_PREFIX + "maximum-start-wait-time-millis";
  public static final long DEFAULT_MR_HISTORY_MAX_START_WAIT_TIME = -1;
  /**
   *  Path where history files should be stored after a job finished and before
   *  they are pulled into the job history server.
   **/
  public static final String MR_HISTORY_INTERMEDIATE_DONE_DIR =
    MR_HISTORY_PREFIX + "intermediate-done-dir";
  
  /** Size of the job list cache.*/
  public static final String MR_HISTORY_JOBLIST_CACHE_SIZE =
    MR_HISTORY_PREFIX + "joblist.cache.size";
  public static final int DEFAULT_MR_HISTORY_JOBLIST_CACHE_SIZE = 20000;

  /** The location of the Kerberos keytab file.*/
  public static final String MR_HISTORY_KEYTAB = MR_HISTORY_PREFIX + "keytab";
  
  /** Size of the loaded job cache.*/
  public static final String MR_HISTORY_LOADED_JOB_CACHE_SIZE = 
    MR_HISTORY_PREFIX + "loadedjobs.cache.size";
  public static final int DEFAULT_MR_HISTORY_LOADED_JOB_CACHE_SIZE = 5;

  /** Size of the loaded job cache (in tasks).*/
  public static final String MR_HISTORY_LOADED_TASKS_CACHE_SIZE =
      MR_HISTORY_PREFIX + "loadedtasks.cache.size";

  /**
   * The maximum age of a job history file before it is deleted from the history
   * server.
   */
  public static final String MR_HISTORY_MAX_AGE_MS =
    MR_HISTORY_PREFIX + "max-age-ms";
  public static final long DEFAULT_MR_HISTORY_MAX_AGE = 
    7 * 24 * 60 * 60 * 1000L; //1 week
  
  /**
   * Scan for history files to more from intermediate done dir to done dir
   * every X ms.
   */
  public static final String MR_HISTORY_MOVE_INTERVAL_MS = 
    MR_HISTORY_PREFIX + "move.interval-ms";
  public static final long DEFAULT_MR_HISTORY_MOVE_INTERVAL_MS = 
    3 * 60 * 1000l; //3 minutes
  
  /** The number of threads used to move files.*/
  public static final String MR_HISTORY_MOVE_THREAD_COUNT = 
    MR_HISTORY_PREFIX + "move.thread-count";
  public static final int DEFAULT_MR_HISTORY_MOVE_THREAD_COUNT = 3;
  
  /** The Kerberos principal for the history server.*/
  public static final String MR_HISTORY_PRINCIPAL = 
    MR_HISTORY_PREFIX + "principal";
  
  /** To enable https in MR history server */
  public static final String MR_HS_HTTP_POLICY = MR_HISTORY_PREFIX
      + "http.policy";
  public static String DEFAULT_MR_HS_HTTP_POLICY =
          HttpConfig.Policy.HTTP_ONLY.name();
  
  /**The address the history server webapp is on.*/
  public static final String MR_HISTORY_WEBAPP_ADDRESS =
    MR_HISTORY_PREFIX + "webapp.address";
  public static final int DEFAULT_MR_HISTORY_WEBAPP_PORT = 19888;
  public static final String DEFAULT_MR_HISTORY_WEBAPP_ADDRESS =
    "0.0.0.0:" + DEFAULT_MR_HISTORY_WEBAPP_PORT;
  
  /**The https address the history server webapp is on.*/
  public static final String MR_HISTORY_WEBAPP_HTTPS_ADDRESS =
      MR_HISTORY_PREFIX + "webapp.https.address";
  public static final int DEFAULT_MR_HISTORY_WEBAPP_HTTPS_PORT = 19890;
  public static final String DEFAULT_MR_HISTORY_WEBAPP_HTTPS_ADDRESS =
      "0.0.0.0:" + DEFAULT_MR_HISTORY_WEBAPP_HTTPS_PORT;
  
  /**The kerberos principal to be used for spnego filter for history server*/
  public static final String MR_WEBAPP_SPNEGO_USER_NAME_KEY =
      MR_HISTORY_PREFIX + "webapp.spnego-principal";
  
  /** The kerberos keytab to be used for spnego filter for history server*/
  public static final String MR_WEBAPP_SPNEGO_KEYTAB_FILE_KEY =
      MR_HISTORY_PREFIX + "webapp.spnego-keytab-file";

  /*
   * HS Service Authorization
   */
  public static final String MR_HS_SECURITY_SERVICE_AUTHORIZATION =
      "security.mrhs.client.protocol.acl";
  public static final String MR_HS_SECURITY_SERVICE_AUTHORIZATION_ADMIN_REFRESH =
      "security.mrhs.admin.refresh.protocol.acl";

  /**
   * The HistoryStorage class to use to cache history data.
   */
  public static final String MR_HISTORY_STORAGE =
    MR_HISTORY_PREFIX + "store.class";

  /**
   * Enable the history server to store server state and recover server state
   * upon startup.
   */
  public static final String MR_HS_RECOVERY_ENABLE =
      MR_HISTORY_PREFIX + "recovery.enable";
  public static final boolean DEFAULT_MR_HS_RECOVERY_ENABLE = false;

  /**
   * The HistoryServerStateStoreService class to store and recover server state
   */
  public static final String MR_HS_STATE_STORE =
      MR_HISTORY_PREFIX + "recovery.store.class";

  /**
   * The URI where server state will be stored when
   * HistoryServerFileSystemStateStoreService is configured as the state store
   */
  public static final String MR_HS_FS_STATE_STORE_URI =
      MR_HISTORY_PREFIX + "recovery.store.fs.uri";

  /**
   * The local path where server state will be stored when
   * HistoryServerLeveldbStateStoreService is configured as the state store
   */
  public static final String MR_HS_LEVELDB_STATE_STORE_PATH =
      MR_HISTORY_PREFIX + "recovery.store.leveldb.path";

  /** Whether to use fixed ports with the minicluster. */
  public static final String MR_HISTORY_MINICLUSTER_FIXED_PORTS = MR_HISTORY_PREFIX
       + "minicluster.fixed.ports";
  
  /**
   * Default is false to be able to run tests concurrently without port
   * conflicts.
   */
  public static boolean DEFAULT_MR_HISTORY_MINICLUSTER_FIXED_PORTS = false;

  /**
   * Number of characters allowed for job name in Job History Server web page.
   */
  public static final String MR_HS_JOBNAME_LIMIT = MR_HISTORY_PREFIX
      + "jobname.limit";
  public static final int DEFAULT_MR_HS_JOBNAME_LIMIT = 50;


  /**
   * CSRF settings.
   */
  public static final String MR_HISTORY_CSRF_PREFIX = MR_HISTORY_PREFIX +
                                                      "webapp.rest-csrf.";
  public static final String MR_HISTORY_CSRF_ENABLED = MR_HISTORY_CSRF_PREFIX +
                                                       "enabled";
  public static final String MR_HISTORY_CSRF_CUSTOM_HEADER =
      MR_HISTORY_CSRF_PREFIX + "custom-header";
  public static final String MR_HISTORY_METHODS_TO_IGNORE =
      MR_HISTORY_CSRF_PREFIX + "methods-to-ignore";

  /**
   * XFS settings.
   */
  public static final String MR_HISTORY_XFS_PREFIX = MR_HISTORY_PREFIX +
      "webapp.xfs-filter.";
  public static final String MR_HISTORY_XFS_OPTIONS = MR_HISTORY_XFS_PREFIX +
      "xframe-options";

  /**
   * Settings for .jhist file format.
   */
  public static final String MR_HS_JHIST_FORMAT =
      MR_HISTORY_PREFIX + "jhist.format";
  public static final String DEFAULT_MR_HS_JHIST_FORMAT =
      "binary";
}
