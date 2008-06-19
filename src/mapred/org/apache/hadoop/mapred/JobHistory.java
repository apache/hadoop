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

package org.apache.hadoop.mapred;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

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
 */
public class JobHistory {
  
  private static final Log LOG = LogFactory.getLog("org.apache.hadoop.mapred.JobHistory");
  private static final String DELIMITER = " ";
  private static final String KEY = "(\\w+)";
  private static final String VALUE = "[[^\"]?]+"; // anything but a " in ""
  
  private static final Pattern pattern = Pattern.compile(KEY + "=" + "\"" + VALUE + "\"");
  
  public static final String JOBTRACKER_START_TIME =
                               String.valueOf(System.currentTimeMillis());
  private static String JOBTRACKER_UNIQUE_STRING = null;
  private static String LOG_DIR = null;
  private static Map<String, ArrayList<PrintWriter>> openJobs = 
                     new HashMap<String, ArrayList<PrintWriter>>();
  private static boolean disableHistory = false; 
  /**
   * Record types are identifiers for each line of log in history files. 
   * A record type appears as the first token in a single line of log. 
   */
  public static enum RecordTypes {
    Jobtracker, Job, Task, MapAttempt, ReduceAttempt
  }

  /**
   * Job history files contain key="value" pairs, where keys belong to this enum. 
   * It acts as a global namespace for all keys. 
   */
  public static enum Keys { 
    JOBTRACKERID,
    START_TIME, FINISH_TIME, JOBID, JOBNAME, USER, JOBCONF, SUBMIT_TIME, 
    LAUNCH_TIME, TOTAL_MAPS, TOTAL_REDUCES, FAILED_MAPS, FAILED_REDUCES, 
    FINISHED_MAPS, FINISHED_REDUCES, JOB_STATUS, TASKID, HOSTNAME, TASK_TYPE, 
    ERROR, TASK_ATTEMPT_ID, TASK_STATUS, COPY_PHASE, SORT_PHASE, REDUCE_PHASE, 
    SHUFFLE_FINISHED, SORT_FINISHED, COUNTERS, SPLITS
  }

  /**
   * This enum contains some of the values commonly used by history log events. 
   * since values in history can only be strings - Values.name() is used in 
   * most places in history file. 
   */
  public static enum Values {
    SUCCESS, FAILED, KILLED, MAP, REDUCE
  }

  // temp buffer for parsed dataa
  private static Map<Keys,String> parseBuffer = new HashMap<Keys, String>(); 

  /**
   * Initialize JobHistory files. 
   * @param conf Jobconf of the job tracker.
   * @param hostname jobtracker's hostname
   * @return true if intialized properly
   *         false otherwise
   */
  public static boolean init(JobConf conf, String hostname){
    try {
      LOG_DIR = conf.get("hadoop.job.history.location" ,
        "file:///" + new File(
        System.getProperty("hadoop.log.dir")).getAbsolutePath()
        + File.separator + "history");
      JOBTRACKER_UNIQUE_STRING = hostname + "_" + 
                                   JOBTRACKER_START_TIME + "_";
      Path logDir = new Path(LOG_DIR);
      FileSystem fs = logDir.getFileSystem(conf);
      if (!fs.exists(logDir)){
        if (!fs.mkdirs(logDir)){
          throw new IOException("Mkdirs failed to create " + logDir.toString());
        }
      }
      conf.set("hadoop.job.history.location", LOG_DIR);
      disableHistory = false;
    } catch(IOException e) {
        LOG.error("Failed to initialize JobHistory log file", e); 
        disableHistory = true;
    }
    return !(disableHistory);
  }

  /**
   * Parses history file and invokes Listener.handle() for 
   * each line of history. It can be used for looking through history
   * files for specific items without having to keep whole history in memory. 
   * @param path path to history file
   * @param l Listener for history events 
   * @param fs FileSystem where history file is present
   * @throws IOException
   */
  public static void parseHistoryFromFS(String path, Listener l, FileSystem fs)
  throws IOException{
    FSDataInputStream in = fs.open(new Path(path));
    BufferedReader reader = new BufferedReader(new InputStreamReader (in));
    try {
      String line = null; 
      StringBuffer buf = new StringBuffer(); 
      while ((line = reader.readLine())!= null){
        buf.append(line); 
        if (!line.trim().endsWith("\"")){
          continue; 
        }
        parseLine(buf.toString(), l);
        buf = new StringBuffer(); 
      }
    } finally {
      try { reader.close(); } catch (IOException ex) {}
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
    String recType = line.substring(0, idx);
    String data = line.substring(idx+1, line.length());
    
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
  
  static void log(PrintWriter out, RecordTypes recordType, Keys key, 
                  String value){
    out.println(recordType.name() + DELIMITER + key + "=\"" + value + "\""); 
  }
  
  /**
   * Log a number of keys and values with record. the array length of keys and values
   * should be same. 
   * @param recordType type of log event
   * @param keys type of log event
   * @param values type of log event
   */

  static void log(ArrayList<PrintWriter> writers, RecordTypes recordType, 
                  Keys[] keys, String[] values) {
    StringBuffer buf = new StringBuffer(recordType.name()); 
    buf.append(DELIMITER); 
    for(int i =0; i< keys.length; i++){
      buf.append(keys[i]);
      buf.append("=\"");
      buf.append(values[i]);
      buf.append("\"");
      buf.append(DELIMITER); 
    }
    
    for (PrintWriter out : writers) {
      out.println(buf.toString());
    }
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
      return s == null ? "" : s; 
    }
    /**
     * Convert value from history to int and return. 
     * if no value is found it returns 0.
     * @param k key 
     */
    public int getInt(Keys k){
      String s = values.get(k); 
      if (null != s){
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
      if (null != s){
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
    public synchronized void handle(Map<Keys, String> values){
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

    @Deprecated
    public static String getLocalJobFilePath(String jobid) {
      return getLocalJobFilePath(JobID.forName(jobid));
    }

    /**
     * Get the path of the locally stored job file
     * @param jobId id of the job
     * @return the path of the job file on the local file system 
     */
    public static String getLocalJobFilePath(JobID jobId){
      return System.getProperty("hadoop.log.dir") + File.separator +
               jobId + "_conf.xml";
    }
    
    /**
     * Helper function to encode the URL of the path of the job-history
     * log file. 
     * 
     * @param logFile path of the job-history file
     * @return URL encoded path
     * @throws IOException
     */
    public static String encodeJobHistoryFilePath(String logFile)
    throws IOException {
      Path rawPath = new Path(logFile);
      String encodedFileName = null;
      try {
        encodedFileName = URLEncoder.encode(rawPath.getName(), "UTF-8");
      } catch (UnsupportedEncodingException uee) {
        IOException ioe = new IOException();
        ioe.initCause(uee);
        ioe.setStackTrace(uee.getStackTrace());
        throw ioe;
      }
      
      Path encodedPath = new Path(rawPath.getParent(), encodedFileName);
      return encodedPath.toString();
    }
    
    /**
     * Helper function to encode the URL of the filename of the job-history 
     * log file.
     * 
     * @param logFileName file name of the job-history file
     * @return URL encoded filename
     * @throws IOException
     */
    public static String encodeJobHistoryFileName(String logFileName)
    throws IOException {
      String encodedFileName = null;
      try {
        encodedFileName = URLEncoder.encode(logFileName, "UTF-8");
      } catch (UnsupportedEncodingException uee) {
        IOException ioe = new IOException();
        ioe.initCause(uee);
        ioe.setStackTrace(uee.getStackTrace());
        throw ioe;
      }
      return encodedFileName;
    }
    
    /**
     * Helper function to decode the URL of the filename of the job-history 
     * log file.
     * 
     * @param logFileName file name of the job-history file
     * @return URL decoded filename
     * @throws IOException
     */
    public static String decodeJobHistoryFileName(String logFileName)
    throws IOException {
      String decodedFileName = null;
      try {
        decodedFileName = URLDecoder.decode(logFileName, "UTF-8");
      } catch (UnsupportedEncodingException uee) {
        IOException ioe = new IOException();
        ioe.initCause(uee);
        ioe.setStackTrace(uee.getStackTrace());
        throw ioe;
      }
      return decodedFileName;
    }

    @Deprecated
    public static void logSubmitted(String jobid, JobConf jobConf,
                                    String jobConfPath, long submitTime
                                   ) throws IOException {
      logSubmitted(JobID.forName(jobid), jobConf, jobConfPath, submitTime);
    }

    /**
     * Log job submitted event to history. Creates a new file in history 
     * for the job. if history file creation fails, it disables history 
     * for all other events. 
     * @param jobId job id assigned by job tracker.
     * @param jobConf job conf of the job
     * @param jobConfPath path to job conf xml file in HDFS.
     * @param submitTime time when job tracker received the job
     * @throws IOException
     */
    public static void logSubmitted(JobID jobId, JobConf jobConf, 
                                    String jobConfPath, long submitTime) 
    throws IOException {
      FileSystem fs = null;
      String userLogDir = null;
      String jobUniqueString = JOBTRACKER_UNIQUE_STRING + jobId;

      if (!disableHistory){
        // Get the username and job name to be used in the actual log filename;
        // sanity check them too
        String jobName = jobConf.getJobName();
        if (jobName == null || jobName.length() == 0) {
          jobName = "NA";
        }

        String user = jobConf.getUser();
        if (user == null || user.length() == 0) {
          user = "NA";
        }
        
        // setup the history log file for this job
        String logFileName = 
            encodeJobHistoryFileName(jobUniqueString +  "_" + user + "_" + 
                                     jobName);

        // find user log directory 
        Path outputPath = FileOutputFormat.getOutputPath(jobConf);
        userLogDir = jobConf.get("hadoop.job.history.user.location",
        		outputPath == null ? null : outputPath.toString());
        if ("none".equals(userLogDir)) {
          userLogDir = null;
        }
        if (userLogDir != null) {
          userLogDir = userLogDir + Path.SEPARATOR + "_logs" + 
                       Path.SEPARATOR + "history";
        }

        Path logFile = null;
        Path userLogFile = null;
        if (LOG_DIR != null ) {
          logFile = new Path(LOG_DIR, logFileName);
        }
        if (userLogDir != null ) {
          userLogFile = new Path(userLogDir, logFileName);
        }

        try{
          ArrayList<PrintWriter> writers = new ArrayList<PrintWriter>();
          FSDataOutputStream out = null;
          PrintWriter writer = null;

          if (LOG_DIR != null) {
            // create output stream for logging in hadoop.job.history.location
            fs = new Path(LOG_DIR).getFileSystem(jobConf);
            out = fs.create(logFile, true, 4096);
            writer = new PrintWriter(out);
            writers.add(writer);
          }
          if (userLogDir != null) {
            // create output stream for logging 
            // in hadoop.job.history.user.location
            fs = new Path(userLogDir).getFileSystem(jobConf);
            out = fs.create(userLogFile, true, 4096);
            writer = new PrintWriter(out);
            writers.add(writer);
          }

          openJobs.put(jobUniqueString, writers);

          //add to writer as well 
          JobHistory.log(writers, RecordTypes.Job, 
                         new Keys[]{Keys.JOBID, Keys.JOBNAME, Keys.USER, Keys.SUBMIT_TIME, Keys.JOBCONF }, 
                         new String[]{jobId.toString(), jobName, user, 
                                      String.valueOf(submitTime) , jobConfPath}
                        ); 
             
        }catch(IOException e){
          LOG.error("Failed creating job history log file, disabling history", e);
          disableHistory = true; 
        }
      }
      // Always store job conf on local file system 
      String localJobFilePath =  JobInfo.getLocalJobFilePath(jobId); 
      File localJobFile = new File(localJobFilePath);
      FileOutputStream jobOut = null;
      try {
        jobOut = new FileOutputStream(localJobFile);
        jobConf.write(jobOut);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Job conf for " + jobId + " stored at " 
                    + localJobFile.getAbsolutePath());
        }
      } catch (IOException ioe) {
        LOG.error("Failed to store job conf on the local filesystem ", ioe);
      } finally {
        if (jobOut != null) {
          try {
            jobOut.close();
          } catch (IOException ie) {
            LOG.info("Failed to close the job configuration file " 
                       + StringUtils.stringifyException(ie));
          }
        }
      }

      /* Storing the job conf on the log dir */
      Path jobFilePath = null;
      if (LOG_DIR != null) {
        jobFilePath = new Path(LOG_DIR + File.separator + 
                               jobUniqueString + "_conf.xml");
      }
      Path userJobFilePath = null;
      if (userLogDir != null) {
        userJobFilePath = new Path(userLogDir + File.separator +
                                   jobUniqueString + "_conf.xml");
      }
      FSDataOutputStream jobFileOut = null;
      try {
        if (LOG_DIR != null) {
          fs = new Path(LOG_DIR).getFileSystem(jobConf);
          if (!fs.exists(jobFilePath)) {
            jobFileOut = fs.create(jobFilePath);
            jobConf.write(jobFileOut);
            jobFileOut.close();
          }
        } 
        if (userLogDir != null) {
          fs = new Path(userLogDir).getFileSystem(jobConf);
          jobFileOut = fs.create(userJobFilePath);
          jobConf.write(jobFileOut);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Job conf for " + jobId + " stored at " 
                    + jobFilePath + "and" + userJobFilePath );
        }
      } catch (IOException ioe) {
        LOG.error("Failed to store job conf on the local filesystem ", ioe);
      } finally {
        if (jobFileOut != null) {
          try {
            jobFileOut.close();
          } catch (IOException ie) {
            LOG.info("Failed to close the job configuration file " 
                     + StringUtils.stringifyException(ie));
          }
        }
      } 
    }

    @Deprecated
    public static void logStarted(String jobid, long startTime, int totalMaps,
                                  int totalReduces) {
      logStarted(JobID.forName(jobid), startTime, totalMaps, totalReduces);
    }

    /**
     * Logs launch time of job. 
     * @param jobId job id, assigned by jobtracker. 
     * @param startTime start time of job. 
     * @param totalMaps total maps assigned by jobtracker. 
     * @param totalReduces total reduces. 
     */
    public static void logStarted(JobID jobId, long startTime, int totalMaps, int totalReduces){
      if (!disableHistory){
        String logFileKey =  JOBTRACKER_UNIQUE_STRING + jobId; 
        ArrayList<PrintWriter> writer = openJobs.get(logFileKey); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.Job, 
                         new Keys[] {Keys.JOBID, Keys.LAUNCH_TIME, Keys.TOTAL_MAPS, Keys.TOTAL_REDUCES },
                         new String[] {jobId.toString(),  String.valueOf(startTime), String.valueOf(totalMaps), String.valueOf(totalReduces)}); 
        }
      }
    }
    
    @Deprecated
    public static void logFinished(String jobId, long finishTime,
                                   int finishedMaps, int finishedReduces,
                                   int failedMaps, int failedReduces,
                                   Counters counters) {
      logFinished(JobID.forName(jobId), finishTime, finishedMaps,
                  finishedReduces, failedMaps, failedReduces, counters);
    }

    /**
     * Log job finished. closes the job file in history. 
     * @param jobId job id, assigned by jobtracker. 
     * @param finishTime finish time of job in ms. 
     * @param finishedMaps no of maps successfully finished. 
     * @param finishedReduces no of reduces finished sucessfully. 
     * @param failedMaps no of failed map tasks. 
     * @param failedReduces no of failed reduce tasks. 
     * @param counters the counters from the job
     */ 
    public static void logFinished(JobID jobId, long finishTime, 
                                   int finishedMaps, int finishedReduces,
                                   int failedMaps, int failedReduces,
                                   Counters counters){
      if (!disableHistory){
        // close job file for this job
        String logFileKey =  JOBTRACKER_UNIQUE_STRING + jobId; 
        ArrayList<PrintWriter> writer = openJobs.get(logFileKey); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.Job,          
                         new Keys[] {Keys.JOBID, Keys.FINISH_TIME, 
                                     Keys.JOB_STATUS, Keys.FINISHED_MAPS, 
                                     Keys.FINISHED_REDUCES,
                                     Keys.FAILED_MAPS, Keys.FAILED_REDUCES,
                                     Keys.COUNTERS},
                         new String[] {jobId.toString(),  Long.toString(finishTime), 
                                       Values.SUCCESS.name(), 
                                       String.valueOf(finishedMaps), 
                                       String.valueOf(finishedReduces),
                                       String.valueOf(failedMaps), 
                                       String.valueOf(failedReduces),
                                       counters.makeCompactString()});
          for (PrintWriter out : writer) {
            out.close();
          }
          openJobs.remove(logFileKey); 
        }
        Thread historyCleaner  = new Thread(new HistoryCleaner());
        historyCleaner.start(); 
      }
    }
    
    @Deprecated
    public static void logFailed(String jobid, long timestamp, 
                                 int finishedMaps, int finishedReduces) {
      logFailed(JobID.forName(jobid), timestamp, finishedMaps, finishedReduces);
    }

    /**
     * Logs job failed event. Closes the job history log file. 
     * @param jobid job id
     * @param timestamp time when job failure was detected in ms.  
     * @param finishedMaps no finished map tasks. 
     * @param finishedReduces no of finished reduce tasks. 
     */
    public static void logFailed(JobID jobid, long timestamp, int finishedMaps, int finishedReduces){
      if (!disableHistory){
        String logFileKey =  JOBTRACKER_UNIQUE_STRING + jobid; 
        ArrayList<PrintWriter> writer = openJobs.get(logFileKey); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.Job,
                         new Keys[] {Keys.JOBID, Keys.FINISH_TIME, Keys.JOB_STATUS, Keys.FINISHED_MAPS, Keys.FINISHED_REDUCES },
                         new String[] {jobid.toString(),  String.valueOf(timestamp), Values.FAILED.name(), String.valueOf(finishedMaps), 
                                       String.valueOf(finishedReduces)}); 
          for (PrintWriter out : writer) {
            out.close();
          }
          openJobs.remove(logFileKey); 
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

    @Deprecated
    public static void logStarted(String jobId, String taskId, String taskType,
                                  long startTime) {
      logStarted(TaskID.forName(taskId), taskType, startTime, "n/a");
    }

    /**
     * Log start time of task (TIP).
     * @param taskId task id
     * @param taskType MAP or REDUCE
     * @param startTime startTime of tip. 
     */
    public static void logStarted(TaskID taskId, String taskType, 
                                  long startTime, String splitLocations) {
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                     + taskId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.Task, 
                         new Keys[]{Keys.TASKID, Keys.TASK_TYPE ,
                                    Keys.START_TIME, Keys.SPLITS}, 
                         new String[]{taskId.toString(), taskType,
                                      String.valueOf(startTime),
                                      splitLocations});
        }
      }
    }
    
    @Deprecated
    public static void logFinished(String jobid, String taskid, String taskType,
                                   long finishTime, Counters counters) {
      logFinished(TaskID.forName(taskid), taskType, finishTime, counters);
    }

    /**
     * Log finish time of task. 
     * @param taskId task id
     * @param taskType MAP or REDUCE
     * @param finishTime finish timeof task in ms
     */
    public static void logFinished(TaskID taskId, String taskType, 
                                   long finishTime, Counters counters){
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                     + taskId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.Task, 
                         new Keys[]{Keys.TASKID, Keys.TASK_TYPE, 
                                    Keys.TASK_STATUS, Keys.FINISH_TIME,
                                    Keys.COUNTERS}, 
                         new String[]{ taskId.toString(), taskType, Values.SUCCESS.name(), 
                                       String.valueOf(finishTime),
                                       counters.makeCompactString()});
        }
      }
    }
    
    @Deprecated
    public static void logFailed(String jobid, String taskid, String taskType,
                                 long time, String error) {
      logFailed(TaskID.forName(taskid), taskType, time, error);
    }

    /**
     * Log job failed event.
     * @param taskId task id
     * @param taskType MAP or REDUCE.
     * @param time timestamp when job failed detected. 
     * @param error error message for failure. 
     */
    public static void logFailed(TaskID taskId, String taskType, long time, String error){
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                     + taskId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.Task, 
                         new Keys[]{Keys.TASKID, Keys.TASK_TYPE, 
                                    Keys.TASK_STATUS, Keys.FINISH_TIME, Keys.ERROR}, 
                         new String[]{ taskId.toString(),  taskType, Values.FAILED.name(), String.valueOf(time) , error});
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
    @Deprecated
    public static void logStarted(String jobid, String taskid, String attemptid,
                                  long startTime, String hostName) {
      logStarted(TaskAttemptID.forName(attemptid), startTime, hostName);
    }

    /**
     * Log start time of this map task attempt. 
     * @param taskAttemptId task attempt id
     * @param startTime start time of task attempt as reported by task tracker. 
     * @param hostName host name of the task attempt. 
     */
    public static void logStarted(TaskAttemptID taskAttemptId, long startTime, String hostName){
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                     + taskAttemptId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.MapAttempt, 
                         new Keys[]{ Keys.TASK_TYPE, Keys.TASKID, 
                                     Keys.TASK_ATTEMPT_ID, Keys.START_TIME, 
                                     Keys.HOSTNAME},
                         new String[]{Values.MAP.name(),  taskAttemptId.getTaskID().toString(), 
                                      taskAttemptId.toString(), String.valueOf(startTime), hostName}); 
        }
      }
    }
    
    @Deprecated
    public static void logFinished(String jobid, String taskid, 
                                   String attemptid, long time, String host) {
      logFinished(TaskAttemptID.forName(attemptid), time, host);
    }

    /**
     * Log finish time of map task attempt. 
     * @param taskAttemptId task attempt id 
     * @param finishTime finish time
     * @param hostName host name 
     */
    public static void logFinished(TaskAttemptID taskAttemptId, long finishTime, 
                                   String hostName){
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                     + taskAttemptId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.MapAttempt, 
                         new Keys[]{ Keys.TASK_TYPE, Keys.TASKID, 
                                     Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                     Keys.FINISH_TIME, Keys.HOSTNAME},
                         new String[]{Values.MAP.name(), taskAttemptId.getTaskID().toString(),
                                      taskAttemptId.toString(), Values.SUCCESS.name(),  
                                      String.valueOf(finishTime), hostName}); 
        }
      }
    }

    @Deprecated
    public static void logFailed(String jobid, String taskid,
                                 String attemptid, long timestamp, String host, 
                                 String err) {
      logFailed(TaskAttemptID.forName(attemptid), timestamp, host, err);
    }

    /**
     * Log task attempt failed event.  
     * @param taskAttemptId task attempt id
     * @param timestamp timestamp
     * @param hostName hostname of this task attempt.
     * @param error error message if any for this task attempt. 
     */
    public static void logFailed(TaskAttemptID taskAttemptId, 
                                 long timestamp, String hostName, String error){
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                     + taskAttemptId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.MapAttempt, 
                         new Keys[]{Keys.TASK_TYPE, Keys.TASKID, Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                    Keys.FINISH_TIME, Keys.HOSTNAME, Keys.ERROR},
                         new String[]{ Values.MAP.name(), taskAttemptId.getTaskID().toString(),
                                       taskAttemptId.toString(), Values.FAILED.name(),
                                       String.valueOf(timestamp), hostName, error}); 
        }
      }
    }

    @Deprecated
    public static void logKilled(String jobid, String taskid, String attemptid,
                                 long timestamp, String hostname, String error){
      logKilled(TaskAttemptID.forName(attemptid), timestamp, hostname, error);
    }

    /**
     * Log task attempt killed event.  
     * @param taskAttemptId task attempt id
     * @param timestamp timestamp
     * @param hostName hostname of this task attempt.
     * @param error error message if any for this task attempt. 
     */
    public static void logKilled(TaskAttemptID taskAttemptId, 
                                 long timestamp, String hostName, String error){
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                     + taskAttemptId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.MapAttempt, 
                         new Keys[]{Keys.TASK_TYPE, Keys.TASKID, Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                    Keys.FINISH_TIME, Keys.HOSTNAME, Keys.ERROR},
                         new String[]{ Values.MAP.name(), taskAttemptId.getTaskID().toString(), 
                                       taskAttemptId.toString(), Values.KILLED.name(),
                                       String.valueOf(timestamp), hostName, error}); 
        }
      }
    } 
  }
  /**
   * Helper class for logging or reading back events related to start, finish or failure of 
   * a Map Attempt on a node.
   */
  public static class ReduceAttempt extends TaskAttempt{
    
    @Deprecated
    public static void logStarted(String jobid, String taskid, String attemptid,
                                  long startTime, String hostName) {
      logStarted(TaskAttemptID.forName(attemptid), startTime, hostName);
    }

    /**
     * Log start time of  Reduce task attempt. 
     * @param taskAttemptId task attempt id
     * @param startTime start time
     * @param hostName host name 
     */
    public static void logStarted(TaskAttemptID taskAttemptId, 
                                  long startTime, String hostName){
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                     + taskAttemptId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.ReduceAttempt, 
                         new Keys[]{  Keys.TASK_TYPE, Keys.TASKID, 
                                      Keys.TASK_ATTEMPT_ID, Keys.START_TIME, Keys.HOSTNAME},
                         new String[]{Values.REDUCE.name(),  taskAttemptId.getTaskID().toString(), 
                                      taskAttemptId.toString(), String.valueOf(startTime), hostName}); 
        }
      }
    }
    
    @Deprecated
    public static void logFinished(String jobid, String taskid, String attemptid,
                                   long shuffleFinished, long sortFinished,
                                   long finishTime, String hostname) {
      logFinished(TaskAttemptID.forName(attemptid), shuffleFinished, 
                  sortFinished, finishTime, hostname);
    }

    /**
     * Log finished event of this task. 
     * @param taskAttemptId task attempt id
     * @param shuffleFinished shuffle finish time
     * @param sortFinished sort finish time
     * @param finishTime finish time of task
     * @param hostName host name where task attempt executed
     */
    public static void logFinished(TaskAttemptID taskAttemptId, long shuffleFinished, 
                                   long sortFinished, long finishTime, 
                                   String hostName){
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                     + taskAttemptId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.ReduceAttempt, 
                         new Keys[]{ Keys.TASK_TYPE, Keys.TASKID, 
                                     Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                     Keys.SHUFFLE_FINISHED, Keys.SORT_FINISHED,
                                     Keys.FINISH_TIME, Keys.HOSTNAME},
                         new String[]{Values.REDUCE.name(),  taskAttemptId.getTaskID().toString(), 
                                      taskAttemptId.toString(), Values.SUCCESS.name(), 
                                      String.valueOf(shuffleFinished), String.valueOf(sortFinished),
                                      String.valueOf(finishTime), hostName}); 
        }
      }
    }

    @Deprecated
    public static void logFailed(String jobid, String taskid, String attemptid,
                                 long timestamp, String hostname, String error){
      logFailed(TaskAttemptID.forName(attemptid), timestamp, hostname, error);
    }

    /**
     * Log failed reduce task attempt. 
     * @param taskAttemptId task attempt id
     * @param timestamp time stamp when task failed
     * @param hostName host name of the task attempt.  
     * @param error error message of the task. 
     */
    public static void logFailed(TaskAttemptID taskAttemptId, long timestamp, 
                                 String hostName, String error){
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                     + taskAttemptId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.ReduceAttempt, 
                         new Keys[]{  Keys.TASK_TYPE, Keys.TASKID, Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                      Keys.FINISH_TIME, Keys.HOSTNAME, Keys.ERROR },
                         new String[]{ Values.REDUCE.name(), taskAttemptId.getTaskID().toString(), 
                                       taskAttemptId.toString(), Values.FAILED.name(), 
                                       String.valueOf(timestamp), hostName, error }); 
        }
      }
    }
    
    @Deprecated
    public static void logKilled(String jobid, String taskid, String attemptid,
                                 long timestamp, String hostname, String error){
      logKilled(TaskAttemptID.forName(attemptid), timestamp, hostname, error);
    }

    /**
     * Log killed reduce task attempt. 
     * @param taskAttemptId task attempt id
     * @param timestamp time stamp when task failed
     * @param hostName host name of the task attempt.  
     * @param error error message of the task. 
     */
    public static void logKilled(TaskAttemptID taskAttemptId, long timestamp, 
                                 String hostName, String error){
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                     + taskAttemptId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.ReduceAttempt, 
                         new Keys[]{  Keys.TASK_TYPE, Keys.TASKID, 
                                      Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                      Keys.FINISH_TIME, Keys.HOSTNAME, 
                                      Keys.ERROR },
                         new String[]{ Values.REDUCE.name(), taskAttemptId.getTaskID().toString(), 
                                       taskAttemptId.toString(), Values.KILLED.name(), 
                                       String.valueOf(timestamp), hostName, error }); 
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
   *
   */
  public static class HistoryCleaner implements Runnable{
    static final long ONE_DAY_IN_MS = 24 * 60 * 60 * 1000L;
    static final long THIRTY_DAYS_IN_MS = 30 * ONE_DAY_IN_MS;
    private long now; 
    private static boolean isRunning = false; 
    private static long lastRan; 

    /**
     * Cleans up history data. 
     */
    public void run(){
      if (isRunning){
        return; 
      }
      now = System.currentTimeMillis();
      // clean history only once a day at max
      if (lastRan ==0 || (now - lastRan) < ONE_DAY_IN_MS){
        return; 
      }
      lastRan = now;  
      isRunning = true; 
      File[] oldFiles = new File(LOG_DIR).listFiles(new FileFilter(){
          public boolean accept(File file){
            // delete if older than 30 days
            if (now - file.lastModified() > THIRTY_DAYS_IN_MS){
              return true; 
            }
            return false; 
          }
        });
      for(File f : oldFiles){
        f.delete(); 
        LOG.info("Deleting old history file : " + f.getName());
      }
      isRunning = false; 
    }
  }
}
