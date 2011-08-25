package org.apache.hadoop.mapreduce.v2.jobhistory;

public class JHConfig {
  public static final String HS_PREFIX = "yarn.server.historyserver.";
  /** host:port address to which to bind to **/
  public static final String HS_BIND_ADDRESS = HS_PREFIX + "address";

  public static final String HS_USER_NAME = HS_PREFIX + "kerberos.principal";
  
  public static final String HS_KEYTAB_FILE = HS_PREFIX + "jeytab.file";
  
  public static final String DEFAULT_HS_BIND_ADDRESS = "0.0.0.0:10020";

  /** Done Dir for for AppMaster **/
  public static final String HISTORY_INTERMEDIATE_DONE_DIR_KEY =
       "yarn.historyfile.intermediateDoneDir";

  /** Done Dir for for AppMaster **/
  public static final String HISTORY_DONE_DIR_KEY =
       "yarn.historyfile.doneDir";

  /**
   * Boolean. Create the base dirs in the JobHistoryEventHandler
   * Set to false for multi-user clusters.
   */
  public static final String CREATE_HISTORY_INTERMEDIATE_BASE_DIR_KEY = 
    "yarn.history.create.intermediate.base.dir";
  
  /** Done Dir for history server. **/
  public static final String HISTORY_SERVER_DONE_DIR_KEY = 
       HS_PREFIX + "historyfile.doneDir";
  
  /**
   * Size of the job list cache.
   */
  public static final String HISTORY_SERVER_JOBLIST_CACHE_SIZE_KEY =
    HS_PREFIX + "joblist.cache.size";
     
  /**
   * Size of the loaded job cache.
   */
  public static final String HISTORY_SERVER_LOADED_JOB_CACHE_SIZE_KEY = 
    HS_PREFIX + "loadedjobs.cache.size";
  
  /**
   * Size of the date string cache. Effects the number of directories
   * which will be scanned to find a job.
   */
  public static final String HISTORY_SERVER_DATESTRING_CACHE_SIZE_KEY = 
    HS_PREFIX + "datestring.cache.size";
  
  /**
   * The time interval in milliseconds for the history server
   * to wake up and scan for files to be moved.
   */
  public static final String HISTORY_SERVER_MOVE_THREAD_INTERVAL = 
    HS_PREFIX + "move.thread.interval";
  
  /**
   * The number of threads used to move files.
   */
  public static final String HISTORY_SERVER_NUM_MOVE_THREADS = 
    HS_PREFIX + "move.threads.count";
  
  // Equivalent to 0.20 mapreduce.jobhistory.debug.mode
  public static final String HISTORY_DEBUG_MODE_KEY = HS_PREFIX + "debug.mode";
  
  public static final String HISTORY_MAXAGE =
    "yarn.historyfile.maxage";
  
  //TODO Move some of the HistoryServer specific out into a separate configuration class.
  public static final String HS_KEYTAB_KEY = HS_PREFIX + "keytab";
  
  public static final String HS_SERVER_PRINCIPAL_KEY = "yarn.historyserver.principal";
  
  public static final String RUN_HISTORY_CLEANER_KEY = 
    HS_PREFIX + "cleaner.run";
  
  /**
   * Run interval for the History Cleaner thread.
   */
  public static final String HISTORY_CLEANER_RUN_INTERVAL = 
    HS_PREFIX + "cleaner.run.interval";
  
  public static final String HS_WEBAPP_BIND_ADDRESS = HS_PREFIX +
      "address.webapp";
  public static final String DEFAULT_HS_WEBAPP_BIND_ADDRESS =
    "0.0.0.0:19888";
  
  public static final String HS_CLIENT_THREADS = 
    HS_PREFIX + "client.threads";
  public static final int DEFAULT_HS_CLIENT_THREADS = 10;
  
//From JTConfig. May need to be moved elsewhere.
  public static final String JOBHISTORY_TASKPROGRESS_NUMBER_SPLITS_KEY = 
    "mapreduce.jobtracker.jobhistory.task.numberprogresssplits";
  
  public static int DEFAULT_NUMBER_PROGRESS_SPLITS = 12;
}
