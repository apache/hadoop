package org.apache.hadoop.mapred;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * Provides methods for writing to and reading from job history. 
 * Job History works in an append mode, JobHistory and its inner classes provide methods 
 * to log job events. 
 * 
 * JobHistory is split into multiple files, format of each file is plain text where each line 
 * is of the format [type (key=value)*], where type identifies the type of the record. 
 * Type maps to UID of one of the inner classes of this class. 
 * 
 * Job history is maintained in a master index which contains star/stop times of all jobs with
 * a few other job level properties. Apart from this each job's history is maintained in a seperate history 
 * file. name of job history files follows the format jobtrackerId_jobid
 *  
 * For parsing the job history it supports a listener based interface where each line is parsed
 * and passed to listener. The listener can create an object model of history or look for specific 
 * events and discard rest of the history.  
 *
 */
public class JobHistory {
  
  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.mapred.JobHistory");
  private static final String DELIMITER = " ";
  private static final String KEY = "(\\w+)";
  private static final String VALUE = "[[^\"]?]+" ; // anything but a " in ""
  
  private static final Pattern pattern = Pattern.compile(KEY + "=" + "\"" + VALUE + "\"");
  
  public static final String JOBTRACKER_START_TIME = String.valueOf(System.currentTimeMillis()); 
  private static final String LOG_DIR = System.getProperty("hadoop.log.dir") + File.separator + "history" ; 
  public static final String MASTER_INDEX_LOG_FILE = "JobHistory.log"; 
  
  private static PrintWriter masterIndex = null;
  private static Map<String, PrintWriter> openJobs = new HashMap<String, PrintWriter>(); 
  private static boolean disableHistory = false; 
  /**
   * Record types are identifiers for each line of log in history files. 
   * A record type appears as the first token in a single line of log. 
   */
  public static enum RecordTypes {Jobtracker, Job, Task, MapAttempt, ReduceAttempt};
  /**
   * Job history files contain key="value" pairs, where keys belong to this enum. 
   * It acts as a global namespace for all keys. 
   */
  public static enum Keys { JOBTRACKERID,
    START_TIME, FINISH_TIME, JOBID, JOBNAME, USER, JOBCONF,SUBMIT_TIME, LAUNCH_TIME, 
    TOTAL_MAPS, TOTAL_REDUCES, FAILED_MAPS, FAILED_REDUCES, FINISHED_MAPS, FINISHED_REDUCES,
    JOB_STATUS, TASKID, HOSTNAME, TASK_TYPE, ERROR, TASK_ATTEMPT_ID, TASK_STATUS, 
    COPY_PHASE, SORT_PHASE, REDUCE_PHASE, SHUFFLE_FINISHED, SORT_FINISHED 
  };
  /**
   * This enum contains some of the values commonly used by history log events. 
   * since values in history can only be strings - Values.name() is used in 
   * most places in history file. 
   */
  public static enum Values {
    SUCCESS, FAILED, KILLED, MAP, REDUCE
  };
  // temp buffer for parsed dataa
  private static Map<Keys,String> parseBuffer = new HashMap<Keys, String>(); 

  // init log files
  static { init() ; } 
  
  /**
   * Initialize JobHistory files. 
   *
   */
  private static void init(){
    if( !disableHistory ){
      try{
        File logDir = new File(LOG_DIR); 
        if( ! logDir.exists() ){
          if( ! logDir.mkdirs() ){
            throw new IOException("Mkdirs failed to create " + logDir.toString());
          }
        }
        masterIndex = 
          new PrintWriter(
              new FileOutputStream(new File( LOG_DIR + File.separator + MASTER_INDEX_LOG_FILE), true )) ;
        // add jobtracker id = tracker start time
        log(masterIndex, RecordTypes.Jobtracker, Keys.START_TIME, JOBTRACKER_START_TIME);  
      }catch(IOException e){
        LOG.error("Failed to initialize JobHistory log file", e); 
        disableHistory = true ; 
      }
    }
  }


  /**
   * Parses history file and invokes Listener.handle() for each line of history. It can 
   * be used for looking through history files for specific items without having to keep 
   * whlole history in memory. 
   * @param path path to history file
   * @param l Listener for history events 
   * @throws IOException
   */
  public static void parseHistory(File path, Listener l) throws IOException{
      BufferedReader reader = new BufferedReader(new FileReader(path));
      String line = null ; 
      StringBuffer buf = new StringBuffer(); 
      while ((line = reader.readLine())!= null){
        buf.append(line); 
        if( ! line.trim().endsWith("\"")){
          continue ; 
        }
        parseLine(buf.toString(), l );
        buf = new StringBuffer(); 
      }
  }
  /**
   * Parse a single line of history. 
   * @param line
   * @param l
   * @throws IOException
   */
  private static void parseLine(String line, Listener l)throws IOException{
    // extract the record type 
    int idx = line.indexOf(' '); 
    String recType = line.substring(0, idx) ;
    String data = line.substring(idx+1, line.length()) ;
    
    Matcher matcher = pattern.matcher(data); 

    while(matcher.find()){
      String tuple = matcher.group(0);
      String []parts = tuple.split("=");
      
      parseBuffer.put(Keys.valueOf(parts[0]), parts[1].substring(1, parts[1].length() -1));
    }

    l.handle(RecordTypes.valueOf(recType), parseBuffer); 
    
    parseBuffer.clear(); 
  }
  
  
  /**
   * Log a raw record type with keys and values. This is method is generally not used directly. 
   * @param recordType type of log event
   * @param key key
   * @param value value
   */
  
  static void log(PrintWriter out, RecordTypes recordType, Enum key, String value){
    out.println(recordType.name() + DELIMITER + key + "=\"" + value + "\""); 
    out.flush();
  }
  
  /**
   * Log a number of keys and values with record. the array length of keys and values
   * should be same. 
   * @param recordType type of log event
   * @param keys type of log event
   * @param values type of log event
   */

  static void log(PrintWriter out, RecordTypes recordType, Enum[] keys, String[] values){
    StringBuffer buf = new StringBuffer(recordType.name()) ; 
    buf.append(DELIMITER) ; 
    for( int i =0 ; i< keys.length ; i++ ){
      buf.append(keys[i]);
      buf.append("=\"");
      buf.append(values[i]);
      buf.append("\"");
      buf.append(DELIMITER); 
    }
    
    out.println(buf.toString());
    out.flush(); 
  }
  
  /**
   * Returns history disable status. by default history is enabled so this
   * method returns false. 
   * @return true if history logging is disabled, false otherwise. 
   */
  public static boolean isDisableHistory() {
    return disableHistory;
  }

  /**
   * Enable/disable history logging. Default value is false, so history 
   * is enabled by default. 
   * @param disableHistory true if history should be disabled, false otherwise. 
   */
  public static void setDisableHistory(boolean disableHistory) {
    JobHistory.disableHistory = disableHistory;
  }
  
  /**
   * Base class contais utility stuff to manage types key value pairs with enums. 
   */
  static class KeyValuePair{
    private Map<Keys, String> values = new HashMap<Keys, String>(); 

    /**
     * Get 'String' value for given key. Most of the places use Strings as 
     * values so the default get' method returns 'String'.  This method never returns 
     * null to ease on GUIs. if no value is found it returns empty string ""
     * @param k 
     * @return if null it returns empty string - "" 
     */
    public String get(Keys k){
      String s = values.get(k); 
      return s == null ? "" : s ; 
    }
    /**
     * Convert value from history to int and return. 
     * if no value is found it returns 0.
     * @param k key 
     */
    public int getInt(Keys k){
      String s = values.get(k); 
      if( null != s ){
        return Integer.parseInt(s);
      }
      return 0; 
    }
    /**
     * Convert value from history to int and return. 
     * if no value is found it returns 0.
     * @param k
     */
    public long getLong(Keys k){
      String s = values.get(k); 
      if( null != s ){
        return Long.parseLong(s);
      }
      return 0; 
    }
    /**
     * Set value for the key. 
     * @param k
     * @param s
     */
    public void set(Keys k, String s){
      values.put(k, s); 
    }
    /**
     * Adds all values in the Map argument to its own values. 
     * @param m
     */
    public void set(Map<Keys, String> m){
      values.putAll(m);
    }
    /**
     * Reads values back from the history, input is same Map as passed to Listener by parseHistory().  
     * @param values
     */
    public void handle(Map<Keys, String> values){
      set(values); 
    }
    /**
     * Returns Map containing all key-values. 
     */
    public Map<Keys, String> getValues(){
      return values; 
    }
  }
  
  /**
   * Helper class for logging or reading back events related to job start, finish or failure. 
   */
  public static class JobInfo extends KeyValuePair{
    
    private Map<String, Task> allTasks = new TreeMap<String, Task>();
    
    /** Create new JobInfo */
    public JobInfo(String jobId){ 
      set(Keys.JOBID, jobId);  
    }

    /**
     * Returns all map and reduce tasks <taskid-Task>. 
     */
    public Map<String, Task> getAllTasks() { return allTasks; }
    
    /**
     * Log job submitted event to history. Creates a new file in history 
     * for the job. if history file creation fails, it disables history 
     * for all other events. 
     * @param jobId job id assigned by job tracker. 
     * @param jobName job name as given by user in job conf
     * @param user user name
     * @param submitTime time when job tracker received the job
     * @param jobConf path to job conf xml file in HDFS. 
     */
    public static void logSubmitted(String jobId, String jobName, String user, 
        long submitTime, String jobConf){
      
      if( ! disableHistory ){
        synchronized(MASTER_INDEX_LOG_FILE){
          JobHistory.log(masterIndex, RecordTypes.Job, 
              new Enum[]{Keys.JOBID, Keys.JOBNAME, Keys.USER, Keys.SUBMIT_TIME, Keys.JOBCONF }, 
              new String[]{jobId, jobName, user, String.valueOf(submitTime),jobConf });
        }
        // setup the history log file for this job
        String logFileName =  JOBTRACKER_START_TIME + "_" + jobId ; 
        File logFile = new File(LOG_DIR + File.separator + logFileName);
        
        try{
          PrintWriter writer = new PrintWriter(logFile);
          openJobs.put(logFileName, writer);
          // add to writer as well 
          JobHistory.log(writer, RecordTypes.Job, 
              new Enum[]{Keys.JOBID, Keys.JOBNAME, Keys.USER, Keys.SUBMIT_TIME, Keys.JOBCONF }, 
              new String[]{jobId, jobName, user, String.valueOf(submitTime) ,jobConf}); 
             
        }catch(IOException e){
          LOG.error("Failed creating job history log file, disabling history", e);
          disableHistory = true ; 
        }
      }
    }
    /**
     * Logs launch time of job. 
     * @param jobId job id, assigned by jobtracker. 
     * @param startTime start time of job. 
     * @param totalMaps total maps assigned by jobtracker. 
     * @param totalReduces total reduces. 
     */
    public static void logStarted(String jobId, long startTime, int totalMaps, int totalReduces){
      if( ! disableHistory ){
        synchronized(MASTER_INDEX_LOG_FILE){
          JobHistory.log(masterIndex, RecordTypes.Job, 
              new Enum[] {Keys.JOBID, Keys.LAUNCH_TIME, Keys.TOTAL_MAPS, Keys.TOTAL_REDUCES },
              new String[] {jobId,  String.valueOf(startTime), 
                String.valueOf(totalMaps), String.valueOf(totalReduces) } ) ; 
        }
        
        String logFileName =  JOBTRACKER_START_TIME + "_" + jobId ; 
        PrintWriter writer = (PrintWriter)openJobs.get(logFileName); 
        
        if( null != writer ){
          JobHistory.log(writer, RecordTypes.Job, 
              new Enum[] {Keys.JOBID, Keys.LAUNCH_TIME,Keys.TOTAL_MAPS, Keys.TOTAL_REDUCES },
              new String[] {jobId,  String.valueOf(startTime), String.valueOf(totalMaps), String.valueOf(totalReduces)} ) ; 
        }
      }
    }
    /**
     * Log job finished. closes the job file in history. 
     * @param jobId job id, assigned by jobtracker. 
     * @param finishTime finish time of job in ms. 
     * @param finishedMaps no of maps successfully finished. 
     * @param finishedReduces no of reduces finished sucessfully. 
     * @param failedMaps no of failed map tasks. 
     * @param failedReduces no of failed reduce tasks. 
     */ 
    public static void logFinished(String jobId, long finishTime, int finishedMaps, int finishedReduces,
        int failedMaps, int failedReduces){
      if( ! disableHistory ){
        synchronized(MASTER_INDEX_LOG_FILE){
          JobHistory.log(masterIndex, RecordTypes.Job,          
              new Enum[] {Keys.JOBID, Keys.FINISH_TIME, Keys.JOB_STATUS, Keys.FINISHED_MAPS, Keys.FINISHED_REDUCES },
              new String[] {jobId,  "" + finishTime, Values.SUCCESS.name(), 
                String.valueOf(finishedMaps), String.valueOf(finishedReduces) } ) ;
        }
        
        // close job file for this job
        String logFileName = JOBTRACKER_START_TIME + "_" + jobId ; 
        PrintWriter writer = openJobs.get(logFileName); 
        if( null != writer){
          JobHistory.log(writer, RecordTypes.Job,          
              new Enum[] {Keys.JOBID, Keys.FINISH_TIME, Keys.JOB_STATUS, Keys.FINISHED_MAPS, Keys.FINISHED_REDUCES,
              Keys.FAILED_MAPS, Keys.FAILED_REDUCES},
              new String[] {jobId,  "" + finishTime, Values.SUCCESS.name(), 
                String.valueOf(finishedMaps), String.valueOf(finishedReduces),
                String.valueOf(failedMaps), String.valueOf(failedReduces)} ) ;
          writer.close();
          openJobs.remove(logFileName); 
        }
        Thread historyCleaner  = new Thread( new HistoryCleaner() );
        historyCleaner.start(); 
      }
    }
    /**
     * Logs job failed event. Closes the job history log file. 
     * @param jobid job id
     * @param timestamp time when job failure was detected in ms.  
     * @param finishedMaps no finished map tasks. 
     * @param finishedReduces no of finished reduce tasks. 
     */
    public static void logFailed(String jobid, long timestamp, int finishedMaps,int finishedReduces){
      if( ! disableHistory ){
        synchronized(MASTER_INDEX_LOG_FILE){
          JobHistory.log(masterIndex, RecordTypes.Job,
              new Enum[] {Keys.JOBID, Keys.FINISH_TIME, Keys.JOB_STATUS, Keys.FINISHED_MAPS, Keys.FINISHED_REDUCES },
              new String[] {jobid,  String.valueOf(timestamp), Values.FAILED.name(), String.valueOf(finishedMaps), 
                String.valueOf(finishedReduces)} ) ; 
        }
          String logFileName =  JOBTRACKER_START_TIME + "_" + jobid ; 
          PrintWriter writer = (PrintWriter)openJobs.get(logFileName); 
          if( null != writer){
            JobHistory.log(writer, RecordTypes.Job,
                new Enum[] {Keys.JOBID, Keys.FINISH_TIME, Keys.JOB_STATUS,Keys.FINISHED_MAPS, Keys.FINISHED_REDUCES },
                new String[] {jobid,  String.valueOf(timestamp), Values.FAILED.name(), String.valueOf(finishedMaps), 
                  String.valueOf(finishedReduces)} ) ; 
            writer.close();
            openJobs.remove(logFileName); 
          }
      }
    }
  }
  /**
   * Helper class for logging or reading back events related to Task's start, finish or failure. 
   * All events logged by this class are logged in a separate file per job in 
   * job tracker history. These events map to TIPs in jobtracker. 
   */
  public static class Task extends KeyValuePair{
    private Map <String, TaskAttempt> taskAttempts = new TreeMap<String, TaskAttempt>(); 

    /**
     * Log start time of task (TIP).
     * @param jobId job id
     * @param taskId task id
     * @param taskType MAP or REDUCE
     * @param startTime startTime of tip. 
     */
    public static void logStarted(String jobId, String taskId, String taskType, 
         long startTime){
      if( ! disableHistory ){
        PrintWriter writer = (PrintWriter)openJobs.get(JOBTRACKER_START_TIME + "_" + jobId); 
        if( null != writer ){
          JobHistory.log(writer, RecordTypes.Task, new Enum[]{Keys.TASKID, Keys.TASK_TYPE , Keys.START_TIME}, 
              new String[]{taskId, taskType, String.valueOf(startTime)}) ;
        }
      }
    }
    /**
     * Log finish time of task. 
     * @param jobId job id
     * @param taskId task id
     * @param taskType MAP or REDUCE
     * @param finishTime finish timeof task in ms
     */
    public static void logFinished(String jobId, String taskId, String taskType, 
        long finishTime){
      if( ! disableHistory ){
        PrintWriter writer = (PrintWriter)openJobs.get(JOBTRACKER_START_TIME + "_" + jobId); 
        if( null != writer ){
          JobHistory.log(writer, RecordTypes.Task, new Enum[]{Keys.TASKID, Keys.TASK_TYPE, 
              Keys.TASK_STATUS, Keys.FINISH_TIME}, 
              new String[]{ taskId,taskType, Values.SUCCESS.name(), String.valueOf(finishTime)}) ;
        }
      }
    }
    /**
     * Log job failed event.
     * @param jobId jobid
     * @param taskId task id
     * @param taskType MAP or REDUCE.
     * @param time timestamp when job failed detected. 
     * @param error error message for failure. 
     */
    public static void logFailed(String jobId, String taskId, String taskType, long time, String error){
      if( ! disableHistory ){
        PrintWriter writer = (PrintWriter)openJobs.get(JOBTRACKER_START_TIME + "_" + jobId); 
        if( null != writer ){
          JobHistory.log(writer, RecordTypes.Task, new Enum[]{Keys.TASKID, Keys.TASK_TYPE, 
              Keys.TASK_STATUS, Keys.FINISH_TIME, Keys.ERROR}, 
              new String[]{ taskId,  taskType, Values.FAILED.name(), String.valueOf(time) , error}) ;
        }
      }
    }
    /**
     * Returns all task attempts for this task. <task attempt id - TaskAttempt>
     */
    public Map<String, TaskAttempt> getTaskAttempts(){
      return this.taskAttempts;
    }
  }

  /**
   * Base class for Map and Reduce TaskAttempts. 
   */
  public static class TaskAttempt extends Task{} 

  /**
   * Helper class for logging or reading back events related to start, finish or failure of 
   * a Map Attempt on a node.
   */
  public static class MapAttempt extends TaskAttempt{
   /**
    * Log start time of this map task attempt. 
    * @param jobId job id
    * @param taskId task id
    * @param taskAttemptId task attempt id
    * @param startTime start time of task attempt as reported by task tracker. 
    * @param hostName host name of the task attempt. 
    */
   public static void logStarted(String jobId, String taskId,String taskAttemptId, long startTime, String hostName){
     if( ! disableHistory ){
       PrintWriter writer = (PrintWriter)openJobs.get(JOBTRACKER_START_TIME + "_" + jobId);
       if( null != writer ){
         JobHistory.log( writer, RecordTypes.MapAttempt, 
             new Enum[]{ Keys.TASK_TYPE, Keys.TASKID, 
               Keys.TASK_ATTEMPT_ID, Keys.START_TIME, Keys.HOSTNAME},
             new String[]{Values.MAP.name(),  taskId, 
                taskAttemptId, String.valueOf(startTime), hostName} ) ; 
       }
      }
    }
   /**
    * Log finish time of map task attempt. 
    * @param jobId job id
    * @param taskId task id
    * @param taskAttemptId task attempt id 
    * @param finishTime finish time
    * @param hostName host name 
    */
    public static void logFinished(String jobId, String taskId, String taskAttemptId, long finishTime, String hostName){
      if( ! disableHistory ){
        PrintWriter writer = (PrintWriter)openJobs.get(JOBTRACKER_START_TIME + "_" + jobId);
        if( null != writer ){
          JobHistory.log(writer, RecordTypes.MapAttempt, 
              new Enum[]{ Keys.TASK_TYPE, Keys.TASKID, Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
              Keys.FINISH_TIME, Keys.HOSTNAME},
              new String[]{Values.MAP.name(), taskId, taskAttemptId, Values.SUCCESS.name(),  
              String.valueOf(finishTime), hostName} ) ; 
        }
      }
    }
    /**
     * Log task attempt failed event.  
     * @param jobId jobid
     * @param taskId taskid
     * @param taskAttemptId task attempt id
     * @param timestamp timestamp
     * @param hostName hostname of this task attempt.
     * @param error error message if any for this task attempt. 
     */
    public static void logFailed(String jobId, String taskId, String taskAttemptId, 
        long timestamp, String hostName, String error){
      if( ! disableHistory ){
        PrintWriter writer = (PrintWriter)openJobs.get(JOBTRACKER_START_TIME + "_" + jobId);
        if( null != writer ){
          JobHistory.log( writer, RecordTypes.MapAttempt, 
              new Enum[]{Keys.TASK_TYPE, Keys.TASKID, Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                Keys.FINISH_TIME, Keys.HOSTNAME, Keys.ERROR},
              new String[]{ Values.MAP.name(), taskId, taskAttemptId, Values.FAILED.name(),
                String.valueOf(timestamp), hostName, error} ) ; 
        }
      }
    } 
  }
  /**
   * Helper class for logging or reading back events related to start, finish or failure of 
   * a Map Attempt on a node.
   */
  public static class ReduceAttempt extends TaskAttempt{
    /**
     * Log start time of  Reduce task attempt. 
     * @param jobId job id
     * @param taskId task id (tip)
     * @param taskAttemptId task attempt id
     * @param startTime start time
     * @param hostName host name 
     */
    public static void logStarted(String jobId, String taskId, String taskAttemptId, 
        long startTime, String hostName){
      if( ! disableHistory ){
        PrintWriter writer = (PrintWriter)openJobs.get(JOBTRACKER_START_TIME + "_" + jobId);
        if( null != writer ){
          JobHistory.log( writer, RecordTypes.ReduceAttempt, 
              new Enum[]{  Keys.TASK_TYPE, Keys.TASKID, 
                Keys.TASK_ATTEMPT_ID, Keys.START_TIME, Keys.HOSTNAME},
              new String[]{Values.REDUCE.name(),  taskId, 
                taskAttemptId, String.valueOf(startTime), hostName} ) ; 
        }
      }
     }
    /**
     * Log finished event of this task. 
     * @param jobId job id
     * @param taskId task id
     * @param taskAttemptId task attempt id
     * @param shuffleFinished shuffle finish time
     * @param sortFinished sort finish time
     * @param finishTime finish time of task
     * @param hostName host name where task attempt executed
     */
     public static void logFinished(String jobId, String taskId, String taskAttemptId, 
        long shuffleFinished, long sortFinished, long finishTime, String hostName){
       if( ! disableHistory ){
         PrintWriter writer = (PrintWriter)openJobs.get(JOBTRACKER_START_TIME + "_" + jobId);
         if( null != writer ){
           JobHistory.log( writer, RecordTypes.ReduceAttempt, 
               new Enum[]{ Keys.TASK_TYPE, Keys.TASKID, Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
               Keys.SHUFFLE_FINISHED, Keys.SORT_FINISHED, Keys.FINISH_TIME, Keys.HOSTNAME},
               new String[]{Values.REDUCE.name(),  taskId, taskAttemptId, Values.SUCCESS.name(), 
               String.valueOf(shuffleFinished), String.valueOf(sortFinished),
               String.valueOf(finishTime), hostName} ) ; 
         }
       }
     }
     /**
      * Log failed reduce task attempt. 
      * @param jobId job id 
      * @param taskId task id
      * @param taskAttemptId task attempt id
      * @param timestamp time stamp when task failed
      * @param hostName host name of the task attempt.  
      * @param error error message of the task. 
      */
     public static void logFailed(String jobId, String taskId,String taskAttemptId, long timestamp, 
          String hostName, String error){
       if( ! disableHistory ){
         PrintWriter writer = (PrintWriter)openJobs.get(JOBTRACKER_START_TIME + "_" + jobId);
         if( null != writer ){
           JobHistory.log( writer, RecordTypes.ReduceAttempt, 
               new Enum[]{  Keys.TASK_TYPE, Keys.TASKID, Keys.TASK_ATTEMPT_ID,Keys.TASK_STATUS, 
                 Keys.FINISH_TIME, Keys.HOSTNAME, Keys.ERROR },
               new String[]{ Values.REDUCE.name(), taskId, taskAttemptId, Values.FAILED.name(), 
               String.valueOf(timestamp), hostName, error } ) ; 
         }
       }
     }
  }
  /**
   * Callback interface for reading back log events from JobHistory. This interface 
   * should be implemented and passed to JobHistory.parseHistory() 
   *
   */
  public static interface Listener{
    /**
     * Callback method for history parser. 
     * @param recType type of record, which is the first entry in the line. 
     * @param values a map of key-value pairs as thry appear in history.
     * @throws IOException
     */
    public void handle(RecordTypes recType, Map<Keys, String> values) throws IOException; 
  }
  
  /**
   * Delete history files older than one month. Update master index and remove all 
   * jobs older than one month. Also if a job tracker has no jobs in last one month
   * remove reference to the job tracker. 
   * @author sanjaydahiya
   *
   */
  public static class HistoryCleaner implements Runnable{
    static final long ONE_DAY_IN_MS = 24 * 60 * 60 * 1000L;
    static final long THIRTY_DAYS_IN_MS = 30 * ONE_DAY_IN_MS;
    private long now ; 
    private static boolean isRunning = false; 
    private static long lastRan ; 

    /**
     * Cleans up history data. 
     */
    public void run(){
      if( isRunning ){
        return ; 
      }
      now = System.currentTimeMillis() ;
      // clean history only once a day at max
      if( lastRan ==0 || (now - lastRan) < ONE_DAY_IN_MS ){
        return ; 
      }
       lastRan = now;  
       isRunning = true ; 
        // update master Index first
        try{
        File logFile = new File(
            LOG_DIR + File.separator + MASTER_INDEX_LOG_FILE); 
        
        synchronized(MASTER_INDEX_LOG_FILE){
          Map<String, Map<String, JobHistory.JobInfo>> jobTrackersToJobs = 
            DefaultJobHistoryParser.parseMasterIndex(logFile);
          
          // find job that started more than one month back and remove them
          // for jobtracker instances which dont have a job in past one month 
          // remove the jobtracker start timestamp as well. 
          for (String jobTrackerId : jobTrackersToJobs.keySet()){
            Map<String, JobHistory.JobInfo> jobs = jobTrackersToJobs.get(jobTrackerId);
            for(Iterator iter = jobs.keySet().iterator(); iter.hasNext() ; iter.next()){
              JobHistory.JobInfo job = jobs.get(iter.next());
              if( now - job.getLong(Keys.SUBMIT_TIME) > THIRTY_DAYS_IN_MS ) {
                iter.remove(); 
              }
              if( jobs.size() == 0 ){
                iter.remove(); 
              }
            }
          }
          masterIndex.close(); 
          masterIndex = new PrintWriter(logFile);
          // delete old history and write back to a new file 
          for (String jobTrackerId : jobTrackersToJobs.keySet()){
            Map<String, JobHistory.JobInfo> jobs = jobTrackersToJobs.get(jobTrackerId);
            
            log(masterIndex, RecordTypes.Jobtracker, Keys.START_TIME, jobTrackerId);

            for(String jobId : jobs.keySet() ){
              JobHistory.JobInfo job = jobs.get(jobId);
              Map<Keys, String> values = job.getValues();
              
              log(masterIndex, RecordTypes.Job, 
                  values.keySet().toArray(new Keys[0]), 
                  values.values().toArray(new String[0])); 

            }
            masterIndex.flush();
          }
        }
      }catch(IOException e){
        LOG.error("Failed loading history log for cleanup", e);
      }
      
      File[] oldFiles = new File(LOG_DIR).listFiles(new FileFilter(){
        public boolean accept(File file){
          // delete if older than 30 days
          if( now - file.lastModified() > THIRTY_DAYS_IN_MS ){
            return true ; 
          }
            return false; 
        }
      });
      for( File f : oldFiles){
        f.delete(); 
        LOG.info("Deleting old history file : " + f.getName());
      }
      isRunning = false ; 
    }
  }
}
