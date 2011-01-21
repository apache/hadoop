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
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
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
 * 
 * CHANGE LOG :
 * Version 0 : The history has the following format : 
 *             TAG KEY1="VALUE1" KEY2="VALUE2" and so on. 
               TAG can be Job, Task, MapAttempt or ReduceAttempt. 
               Note that a '"' is the line delimiter.
 * Version 1 : Changes the line delimiter to '.'
               Values are now escaped for unambiguous parsing. 
               Added the Meta tag to store version info.
 */
public class JobHistory {
  
  static final long VERSION = 1L;
  public static final Log LOG = LogFactory.getLog(JobHistory.class);
  private static final String DELIMITER = " ";
  static final char LINE_DELIMITER_CHAR = '.';
  static final char[] charsToEscape = new char[] {'"', '=', 
                                                LINE_DELIMITER_CHAR};
  static final String DIGITS = "[0-9]+";

  static final String KEY = "(\\w+)";
  // value is any character other than quote, but escaped quotes can be there
  static final String VALUE = "[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*"; 
  
  static final Pattern pattern = Pattern.compile(KEY + "=" + "\"" + VALUE + "\"");
  
  public static final int JOB_NAME_TRIM_LENGTH = 50;
  private static String JOBTRACKER_UNIQUE_STRING = null;
  private static String LOG_DIR = null;
  private static Map<String, ArrayList<PrintWriter>> openJobs = 
                     new ConcurrentHashMap<String, ArrayList<PrintWriter>>();
  private static boolean disableHistory = false; 
  private static final String SECONDARY_FILE_SUFFIX = ".recover";
  private static long jobHistoryBlockSize = 0;
  private static String jobtrackerHostname;
  final static FsPermission HISTORY_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0750); // rwxr-x---
  final static FsPermission HISTORY_FILE_PERMISSION =
    FsPermission.createImmutable((short) 0740); // rwxr-----
  private static JobConf jtConf;
  /**
   * Record types are identifiers for each line of log in history files. 
   * A record type appears as the first token in a single line of log. 
   */
  public static enum RecordTypes {
    Jobtracker, Job, Task, MapAttempt, ReduceAttempt, Meta
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
    SHUFFLE_FINISHED, SORT_FINISHED, COUNTERS, SPLITS, JOB_PRIORITY, HTTP_PORT, 
    TRACKER_NAME, STATE_STRING, VERSION
  }

  /**
   * This enum contains some of the values commonly used by history log events. 
   * since values in history can only be strings - Values.name() is used in 
   * most places in history file. 
   */
  public static enum Values {
    SUCCESS, FAILED, KILLED, MAP, REDUCE, CLEANUP, RUNNING, PREP, SETUP
  }

  // temp buffer for parsed dataa
  private static Map<Keys,String> parseBuffer = new HashMap<Keys, String>(); 

  /**
   * Initialize JobHistory files. 
   * @param conf Jobconf of the job tracker.
   * @param hostname jobtracker's hostname
   * @param jobTrackerStartTime jobtracker's start time
   * @return true if intialized properly
   *         false otherwise
   */
  public static boolean init(JobConf conf, String hostname, 
                              long jobTrackerStartTime){
    try {
      LOG_DIR = conf.get("hadoop.job.history.location" ,
        "file:///" + new File(
        System.getProperty("hadoop.log.dir")).getAbsolutePath()
        + File.separator + "history");
      JOBTRACKER_UNIQUE_STRING = hostname + "_" + 
                                    String.valueOf(jobTrackerStartTime) + "_";
      jobtrackerHostname = hostname;
      Path logDir = new Path(LOG_DIR);
      FileSystem fs = logDir.getFileSystem(conf);
      if (!fs.exists(logDir)){
        if (!fs.mkdirs(logDir, new FsPermission(HISTORY_DIR_PERMISSION))) {
          throw new IOException("Mkdirs failed to create " + logDir.toString());
        }
      }
      conf.set("hadoop.job.history.location", LOG_DIR);
      disableHistory = false;
      // set the job history block size (default is 3MB)
      jobHistoryBlockSize = 
        conf.getLong("mapred.jobtracker.job.history.block.size", 
                     3 * 1024 * 1024);
      jtConf = conf;
    } catch(IOException e) {
        LOG.error("Failed to initialize JobHistory log file", e); 
        disableHistory = true;
    }
    return !(disableHistory);
  }

  /**
   * Manages job-history's meta information such as version etc.
   * Helps in logging version information to the job-history and recover
   * version information from the history. 
   */
  static class MetaInfoManager implements Listener {
    private long version = 0L;
    private KeyValuePair pairs = new KeyValuePair();
    
    // Extract the version of the history that was used to write the history
    public MetaInfoManager(String line) throws IOException {
      if (null != line) {
        // Parse the line
        parseLine(line, this, false);
      }
    }
    
    // Get the line delimiter
    char getLineDelim() {
      if (version == 0) {
        return '"';
      } else {
        return LINE_DELIMITER_CHAR;
      }
    }
    
    // Checks if the values are escaped or not
    boolean isValueEscaped() {
      // Note that the values are not escaped in version 0
      return version != 0;
    }
    
    public void handle(RecordTypes recType, Map<Keys, String> values) 
    throws IOException {
      // Check if the record is of type META
      if (RecordTypes.Meta == recType) {
        pairs.handle(values);
        version = pairs.getLong(Keys.VERSION); // defaults to 0
      }
    }
    
    /**
     * Logs history meta-info to the history file. This needs to be called once
     * per history file. 
     * @param jobId job id, assigned by jobtracker. 
     */
    static void logMetaInfo(ArrayList<PrintWriter> writers){
      if (!disableHistory){
        if (null != writers){
          JobHistory.log(writers, RecordTypes.Meta, 
              new Keys[] {Keys.VERSION},
              new String[] {String.valueOf(VERSION)}); 
        }
      }
    }
  }
  
  /** Escapes the string especially for {@link JobHistory}
   */
  static String escapeString(String data) {
    return StringUtils.escapeString(data, StringUtils.ESCAPE_CHAR, 
                                    charsToEscape);
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
      
      // Read the meta-info line. Note that this might a jobinfo line for files
      // written with older format
      line = reader.readLine();
      
      // Check if the file is empty
      if (line == null) {
        return;
      }
      
      // Get the information required for further processing
      MetaInfoManager mgr = new MetaInfoManager(line);
      boolean isEscaped = mgr.isValueEscaped();
      String lineDelim = String.valueOf(mgr.getLineDelim());  
      String escapedLineDelim = 
        StringUtils.escapeString(lineDelim, StringUtils.ESCAPE_CHAR, 
                                 mgr.getLineDelim());
      
      do {
        buf.append(line); 
        if (!line.trim().endsWith(lineDelim) 
            || line.trim().endsWith(escapedLineDelim)) {
          buf.append("\n");
          continue; 
        }
        parseLine(buf.toString(), l, isEscaped);
        buf = new StringBuffer(); 
      } while ((line = reader.readLine())!= null);
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
  private static void parseLine(String line, Listener l, boolean isEscaped) 
  throws IOException{
    // extract the record type 
    int idx = line.indexOf(' '); 
    String recType = line.substring(0, idx);
    String data = line.substring(idx+1, line.length());
    
    Matcher matcher = pattern.matcher(data); 

    while(matcher.find()){
      String tuple = matcher.group(0);
      String []parts = StringUtils.split(tuple, StringUtils.ESCAPE_CHAR, '=');
      String value = parts[1].substring(1, parts[1].length() -1);
      if (isEscaped) {
        value = StringUtils.unEscapeString(value, StringUtils.ESCAPE_CHAR,
                                           charsToEscape);
      }
      parseBuffer.put(Keys.valueOf(parts[0]), value);
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
    value = escapeString(value);
    out.println(recordType.name() + DELIMITER + key + "=\"" + value + "\""
                + DELIMITER + LINE_DELIMITER_CHAR); 
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
      values[i] = escapeString(values[i]);
      buf.append(values[i]);
      buf.append("\"");
      buf.append(DELIMITER); 
    }
    buf.append(LINE_DELIMITER_CHAR);
    
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
   * Get the history location
   */
  static Path getJobHistoryLocation() {
    return new Path(LOG_DIR);
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
    
    /**
     * Get the job name from the job conf
     */
    static String getJobName(JobConf jobConf) {
      String jobName = jobConf.getJobName();
      if (jobName == null || jobName.length() == 0) {
        jobName = "NA";
      }
      return jobName;
    }
    
    /**
     * Get the user name from the job conf
     */
    public static String getUserName(JobConf jobConf) {
      String user = jobConf.getUser();
      if (user == null || user.length() == 0) {
        user = "NA";
      }
      return user;
    }
    
    /**
     * Get the job history file path given the history filename
     */
    public static Path getJobHistoryLogLocation(String logFileName)
    {
      return LOG_DIR == null ? null : new Path(LOG_DIR, logFileName);
    }

    /**
     * Get the user job history file path
     */
    public static Path getJobHistoryLogLocationForUser(String logFileName, 
                                                       JobConf jobConf) {
      // find user log directory 
      Path userLogFile = null;
      Path outputPath = FileOutputFormat.getOutputPath(jobConf);
      String userLogDir = jobConf.get("hadoop.job.history.user.location",
                                      outputPath == null 
                                      ? null 
                                      : outputPath.toString());
      if ("none".equals(userLogDir)) {
        userLogDir = null;
      }
      if (userLogDir != null) {
        userLogDir = userLogDir + Path.SEPARATOR + "_logs" + Path.SEPARATOR 
                     + "history";
        userLogFile = new Path(userLogDir, logFileName);
      }
      return userLogFile;
    }

    /**
     * Generates the job history filename for a new job
     */
    private static String getNewJobHistoryFileName(JobConf jobConf, JobID id) {
      return JOBTRACKER_UNIQUE_STRING
             + id.toString() + "_" + getUserName(jobConf) + "_" 
             + trimJobName(getJobName(jobConf));
    }
    
    /**
     * Trims the job-name if required
     */
    private static String trimJobName(String jobName) {
      if (jobName.length() > JOB_NAME_TRIM_LENGTH) {
        jobName = jobName.substring(0, JOB_NAME_TRIM_LENGTH);
      }
      return jobName;
    }
    
    private static String escapeRegexChars( String string ) {
      return "\\Q"+string.replaceAll("\\\\E", "\\\\E\\\\\\\\E\\\\Q")+"\\E";
    }

    /**
     * Recover the job history filename from the history folder. 
     * Uses the following pattern
     *    $jt-hostname_[0-9]*_$job-id_$user-$job-name*
     * @param jobConf the job conf
     * @param id job id
     */
    public static synchronized String getJobHistoryFileName(JobConf jobConf, 
                                                            JobID id) 
    throws IOException {
      String user = getUserName(jobConf);
      String jobName = trimJobName(getJobName(jobConf));
      
      FileSystem fs = new Path(LOG_DIR).getFileSystem(jobConf);
      if (LOG_DIR == null) {
        return null;
      }

      // Make the pattern matching the job's history file
      final Pattern historyFilePattern = 
        Pattern.compile(jobtrackerHostname + "_" + DIGITS + "_" 
                        + id.toString() + "_" + user + "_" 
                        + escapeRegexChars(jobName) + "+");
      // a path filter that matches 4 parts of the filenames namely
      //  - jt-hostname
      //  - job-id
      //  - username
      //  - jobname
      PathFilter filter = new PathFilter() {
        public boolean accept(Path path) {
          String fileName = path.getName();
          try {
            fileName = decodeJobHistoryFileName(fileName);
          } catch (IOException ioe) {
            LOG.info("Error while decoding history file " + fileName + "."
                     + " Ignoring file.", ioe);
            return false;
          }
          return historyFilePattern.matcher(fileName).find();
        }
      };
      
      FileStatus[] statuses = fs.listStatus(new Path(LOG_DIR), filter);
      String filename = null;
      if (statuses.length == 0) {
        LOG.info("Nothing to recover for job " + id);
      } else {
        // return filename considering that fact the name can be a 
        // secondary filename like filename.recover
        filename = decodeJobHistoryFileName(statuses[0].getPath().getName());
        // Remove the '.recover' suffix if it exists
        if (filename.endsWith(jobName + SECONDARY_FILE_SUFFIX)) {
          int newLength = filename.length() - SECONDARY_FILE_SUFFIX.length();
          filename = filename.substring(0, newLength);
        }
        filename = encodeJobHistoryFileName(filename);
        LOG.info("Recovered job history filename for job " + id + " is " 
                 + filename);
      }
      return filename;
    }
    
    /** Since there was a restart, there should be a master file and 
     * a recovery file. Once the recovery is complete, the master should be 
     * deleted as an indication that the recovery file should be treated as the 
     * master upon completion or next restart.
     * @param fileName the history filename that needs checkpointing
     * @param conf Job conf
     * @throws IOException
     */
    static synchronized void checkpointRecovery(String fileName, JobConf conf) 
    throws IOException {
      Path logPath = JobHistory.JobInfo.getJobHistoryLogLocation(fileName);
      if (logPath != null) {
        FileSystem fs = logPath.getFileSystem(conf);
        LOG.info("Deleting job history file " + logPath.getName());
        fs.delete(logPath, false);
      }
      // do the same for the user file too
      logPath = JobHistory.JobInfo.getJobHistoryLogLocationForUser(fileName, 
                                                                   conf);
      if (logPath != null) {
        FileSystem fs = logPath.getFileSystem(conf);
        fs.delete(logPath, false);
      }
    }
    
    static String getSecondaryJobHistoryFile(String filename) 
    throws IOException {
      return encodeJobHistoryFileName(
          decodeJobHistoryFileName(filename) + SECONDARY_FILE_SUFFIX);
    }
    
    /** Selects one of the two files generated as a part of recovery. 
     * The thumb rule is that always select the oldest file. 
     * This call makes sure that only one file is left in the end. 
     * @param conf job conf
     * @param logFilePath Path of the log file
     * @throws IOException 
     */
    public synchronized static Path recoverJobHistoryFile(JobConf conf, 
                                                          Path logFilePath) 
    throws IOException {
      Path ret;
      FileSystem fs = logFilePath.getFileSystem(conf);
      String logFileName = logFilePath.getName();
      String tmpFilename = getSecondaryJobHistoryFile(logFileName);
      Path logDir = logFilePath.getParent();
      Path tmpFilePath = new Path(logDir, tmpFilename);
      if (fs.exists(logFilePath)) {
        LOG.info(logFileName + " exists!");
        if (fs.exists(tmpFilePath)) {
          LOG.info("Deleting " + tmpFilename 
                   + "  and using " + logFileName + " for recovery.");
          fs.delete(tmpFilePath, false);
        }
        ret = tmpFilePath;
      } else {
        LOG.info(logFileName + " doesnt exist! Using " 
                 + tmpFilename + " for recovery.");
        if (fs.exists(tmpFilePath)) {
          LOG.info("Renaming " + tmpFilename + " to " + logFileName);
          fs.rename(tmpFilePath, logFilePath);
          ret = tmpFilePath;
        } else {
          ret = logFilePath;
        }
      }

      // do the same for the user files too
      logFilePath = getJobHistoryLogLocationForUser(logFileName, conf);
      if (logFilePath != null) {
        fs = logFilePath.getFileSystem(conf);
        logDir = logFilePath.getParent();
        tmpFilePath = new Path(logDir, tmpFilename);
        if (fs.exists(logFilePath)) {
          LOG.info(logFileName + " exists!");
          if (fs.exists(tmpFilePath)) {
            LOG.info("Deleting " + tmpFilename + "  and making " + logFileName 
                     + " as the master history file for user.");
            fs.delete(tmpFilePath, false);
          }
        } else {
          LOG.info(logFileName + " doesnt exist! Using " 
                   + tmpFilename + " as the master history file for user.");
          if (fs.exists(tmpFilePath)) {
            LOG.info("Renaming " + tmpFilename + " to " + logFileName 
                     + " in user directory");
            fs.rename(tmpFilePath, logFilePath);
          }
        }
      }
      
      return ret;
    }

    /** Finalize the recovery and make one file in the end. 
     * This invloves renaming the recover file to the master file.
     * @param id Job id  
     * @param conf the job conf
     * @throws IOException
     */
    static synchronized void finalizeRecovery(JobID id, JobConf conf) 
    throws IOException {
      String masterLogFileName = 
        JobHistory.JobInfo.getJobHistoryFileName(conf, id);
      if (masterLogFileName == null) {
        return;
      }
      Path masterLogPath = 
        JobHistory.JobInfo.getJobHistoryLogLocation(masterLogFileName);
      String tmpLogFileName = getSecondaryJobHistoryFile(masterLogFileName);
      Path tmpLogPath = 
        JobHistory.JobInfo.getJobHistoryLogLocation(tmpLogFileName);
      if (masterLogPath != null) {
        FileSystem fs = masterLogPath.getFileSystem(conf);

        // rename the tmp file to the master file. Note that this should be 
        // done only when the file is closed and handles are released.
        if(fs.exists(tmpLogPath)) {
          LOG.info("Renaming " + tmpLogFileName + " to " + masterLogFileName);
          fs.rename(tmpLogPath, masterLogPath);
        }
      }
      
      // do the same for the user file too
      masterLogPath = 
        JobHistory.JobInfo.getJobHistoryLogLocationForUser(masterLogFileName,
                                                           conf);
      tmpLogPath = 
        JobHistory.JobInfo.getJobHistoryLogLocationForUser(tmpLogFileName, 
                                                           conf);
      if (masterLogPath != null) {
        FileSystem fs = masterLogPath.getFileSystem(conf);
        if (fs.exists(tmpLogPath)) {
          LOG.info("Renaming " + tmpLogFileName + " to " + masterLogFileName
                   + " in user directory");
          fs.rename(tmpLogPath, masterLogPath);
        }
      }
    }

    /**
     * Deletes job data from the local disk.
     * For now just deletes the localized copy of job conf
     */
    static void cleanupJob(JobID id) {
      String localJobFilePath =  JobInfo.getLocalJobFilePath(id);
      File f = new File (localJobFilePath);
      LOG.info("Deleting localized job conf at " + f);
      if (!f.delete()) {
        LOG.debug("Failed to delete file " + f);
      }
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
     * @deprecated Use 
     *     {@link #logSubmitted(JobID, JobConf, String, long, boolean)} instead.
     */
    public static void logSubmitted(JobID jobId, JobConf jobConf, 
                                    String jobConfPath, long submitTime) 
    throws IOException {
      logSubmitted(jobId, jobConf, jobConfPath, submitTime, true);
    }
    
    public static void logSubmitted(JobID jobId, JobConf jobConf, 
                                    String jobConfPath, long submitTime, 
                                    boolean restarted) 
    throws IOException {
      FileSystem fs = null;
      String userLogDir = null;
      String jobUniqueString = JOBTRACKER_UNIQUE_STRING + jobId;

      if (!disableHistory){
        // Get the username and job name to be used in the actual log filename;
        // sanity check them too        
        String jobName = getJobName(jobConf);

        String user = getUserName(jobConf);
        
        // get the history filename
        String logFileName = null;
        if (restarted) {
          logFileName = getJobHistoryFileName(jobConf, jobId);
          if (logFileName == null) {
            logFileName =
              encodeJobHistoryFileName(getNewJobHistoryFileName(jobConf, jobId));
            
          }
        } else {
          logFileName = 
            encodeJobHistoryFileName(getNewJobHistoryFileName(jobConf, jobId));
        }

        // setup the history log file for this job
        Path logFile = getJobHistoryLogLocation(logFileName);
        
        // find user log directory
        Path userLogFile = 
          getJobHistoryLogLocationForUser(logFileName, jobConf);

        try{
          ArrayList<PrintWriter> writers = new ArrayList<PrintWriter>();
          FSDataOutputStream out = null;
          PrintWriter writer = null;

          if (LOG_DIR != null) {
            // create output stream for logging in hadoop.job.history.location
            fs = new Path(LOG_DIR).getFileSystem(jobConf);
            
            if (restarted) {
              logFile = recoverJobHistoryFile(jobConf, logFile);
              logFileName = logFile.getName();
            }
            
            int defaultBufferSize = 
              fs.getConf().getInt("io.file.buffer.size", 4096);
            out = fs.create(logFile, 
                            new FsPermission(HISTORY_FILE_PERMISSION),
                            true, 
                            defaultBufferSize, 
                            fs.getDefaultReplication(), 
                            jobHistoryBlockSize, null);
            writer = new PrintWriter(out);
            writers.add(writer);
          }
          if (userLogFile != null) {
            // Get the actual filename as recoverJobHistoryFile() might return
            // a different filename
            userLogDir = userLogFile.getParent().toString();
            userLogFile = new Path(userLogDir, logFileName);
            
            // create output stream for logging 
            // in hadoop.job.history.user.location
            fs = userLogFile.getFileSystem(jobConf);
 
            out = fs.create(userLogFile, true, 4096);
            writer = new PrintWriter(out);
            writers.add(writer);
          }

          openJobs.put(jobUniqueString, writers);
          
          // Log the history meta info
          JobHistory.MetaInfoManager.logMetaInfo(writers);

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
        jobConf.writeXml(jobOut);
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
          int defaultBufferSize = 
              fs.getConf().getInt("io.file.buffer.size", 4096);
          if (!fs.exists(jobFilePath)) {
            jobFileOut = fs.create(jobFilePath, 
                                   new FsPermission(HISTORY_FILE_PERMISSION),
                                   true, 
                                   defaultBufferSize, 
                                   fs.getDefaultReplication(), 
                                   fs.getDefaultBlockSize(), null);
            jobConf.writeXml(jobFileOut);
            jobFileOut.close();
          }
        } 
        if (userLogDir != null) {
          fs = new Path(userLogDir).getFileSystem(jobConf);
          jobFileOut = fs.create(userJobFilePath);
          jobConf.writeXml(jobFileOut);
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
    /**
     * Logs launch time of job. 
     * 
     * @param jobId job id, assigned by jobtracker. 
     * @param startTime start time of job. 
     * @param totalMaps total maps assigned by jobtracker. 
     * @param totalReduces total reduces. 
     */
    public static void logInited(JobID jobId, long startTime, 
                                 int totalMaps, int totalReduces) {
      if (!disableHistory){
        String logFileKey =  JOBTRACKER_UNIQUE_STRING + jobId; 
        ArrayList<PrintWriter> writer = openJobs.get(logFileKey); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.Job, 
              new Keys[] {Keys.JOBID, Keys.LAUNCH_TIME, Keys.TOTAL_MAPS, 
                          Keys.TOTAL_REDUCES, Keys.JOB_STATUS},
              new String[] {jobId.toString(), String.valueOf(startTime), 
                            String.valueOf(totalMaps), 
                            String.valueOf(totalReduces), 
                            Values.PREP.name()}); 
        }
      }
    }
    
   /**
     * Logs the job as RUNNING. 
     *
     * @param jobId job id, assigned by jobtracker. 
     * @param startTime start time of job. 
     * @param totalMaps total maps assigned by jobtracker. 
     * @param totalReduces total reduces. 
     * @deprecated Use {@link #logInited(JobID, long, int, int)} and 
     * {@link #logStarted(JobID)}
     */
    @Deprecated
    public static void logStarted(JobID jobId, long startTime, 
                                  int totalMaps, int totalReduces) {
      logStarted(jobId);
    }
    
    /**
     * Logs job as running 
     * @param jobId job id, assigned by jobtracker. 
     */
    public static void logStarted(JobID jobId){
      if (!disableHistory){
        String logFileKey =  JOBTRACKER_UNIQUE_STRING + jobId; 
        ArrayList<PrintWriter> writer = openJobs.get(logFileKey); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.Job, 
              new Keys[] {Keys.JOBID, Keys.JOB_STATUS},
              new String[] {jobId.toString(),  
                            Values.RUNNING.name()}); 
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
                                       counters.makeEscapedCompactString()});
          for (PrintWriter out : writer) {
            out.close();
          }
          openJobs.remove(logFileKey); 
        }
        Thread historyCleaner  = new Thread(new HistoryCleaner());
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
    /**
     * Logs job killed event. Closes the job history log file.
     * 
     * @param jobid
     *          job id
     * @param timestamp
     *          time when job killed was issued in ms.
     * @param finishedMaps
     *          no finished map tasks.
     * @param finishedReduces
     *          no of finished reduce tasks.
     */
    public static void logKilled(JobID jobid, long timestamp, int finishedMaps,
        int finishedReduces) {
      if (!disableHistory) {
        String logFileKey = JOBTRACKER_UNIQUE_STRING + jobid;
        ArrayList<PrintWriter> writer = openJobs.get(logFileKey);

        if (null != writer) {
          JobHistory.log(writer, RecordTypes.Job, new Keys[] { Keys.JOBID,
              Keys.FINISH_TIME, Keys.JOB_STATUS, Keys.FINISHED_MAPS,
              Keys.FINISHED_REDUCES }, new String[] { jobid.toString(),
              String.valueOf(timestamp), Values.KILLED.name(),
              String.valueOf(finishedMaps), String.valueOf(finishedReduces) });
          for (PrintWriter out : writer) {
            out.close();
          }
          openJobs.remove(logFileKey);
        }
      }
    }
    /**
     * Log job's priority. 
     * @param jobid job id
     * @param priority Jobs priority 
     */
    public static void logJobPriority(JobID jobid, JobPriority priority){
      if (!disableHistory){
        String logFileKey =  JOBTRACKER_UNIQUE_STRING + jobid; 
        ArrayList<PrintWriter> writer = openJobs.get(logFileKey); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.Job,
                         new Keys[] {Keys.JOBID, Keys.JOB_PRIORITY},
                         new String[] {jobid.toString(), priority.toString()});
        }
      }
    }
    /**
     * Log job's submit-time/launch-time 
     * @param jobid job id
     * @param submitTime job's submit time
     * @param launchTime job's launch time
     * @param restartCount number of times the job got restarted
     * @deprecated Use {@link #logJobInfo(JobID, long, long)} instead.
     */
    public static void logJobInfo(JobID jobid, long submitTime, long launchTime,
                                  int restartCount){
      logJobInfo(jobid, submitTime, launchTime);
    }

    public static void logJobInfo(JobID jobid, long submitTime, long launchTime)
    {
      if (!disableHistory){
        String logFileKey =  JOBTRACKER_UNIQUE_STRING + jobid; 
        ArrayList<PrintWriter> writer = openJobs.get(logFileKey); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.Job,
                         new Keys[] {Keys.JOBID, Keys.SUBMIT_TIME, 
                                     Keys.LAUNCH_TIME},
                         new String[] {jobid.toString(), 
                                       String.valueOf(submitTime), 
                                       String.valueOf(launchTime)});
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
                                       counters.makeEscapedCompactString()});
        }
      }
    }

    /**
     * Update the finish time of task. 
     * @param taskId task id
     * @param finishTime finish time of task in ms
     */
    public static void logUpdates(TaskID taskId, long finishTime){
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                     + taskId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.Task, 
                         new Keys[]{Keys.TASKID, Keys.FINISH_TIME}, 
                         new String[]{ taskId.toString(), 
                                       String.valueOf(finishTime)});
        }
      }
    }

    /**
     * Log job failed event.
     * @param taskId task id
     * @param taskType MAP or REDUCE.
     * @param time timestamp when job failed detected. 
     * @param error error message for failure. 
     */
    public static void logFailed(TaskID taskId, String taskType, long time, String error){
      logFailed(taskId, taskType, time, error, null);
    }
    
    /**
     * @param failedDueToAttempt The attempt that caused the failure, if any
     */
    public static void logFailed(TaskID taskId, String taskType, long time,
                                 String error, 
                                 TaskAttemptID failedDueToAttempt){
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                     + taskId.getJobID()); 

        if (null != writer){
          String failedAttempt = failedDueToAttempt == null
                                 ? ""
                                 : failedDueToAttempt.toString();
          JobHistory.log(writer, RecordTypes.Task, 
                         new Keys[]{Keys.TASKID, Keys.TASK_TYPE, 
                                    Keys.TASK_STATUS, Keys.FINISH_TIME, 
                                    Keys.ERROR, Keys.TASK_ATTEMPT_ID}, 
                         new String[]{ taskId.toString(),  taskType, 
                                      Values.FAILED.name(), 
                                      String.valueOf(time) , error, 
                                      failedAttempt});
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
     * @param taskAttemptId task attempt id
     * @param startTime start time of task attempt as reported by task tracker. 
     * @param hostName host name of the task attempt. 
     * @deprecated Use 
     *             {@link #logStarted(TaskAttemptID, long, String, int, String)}
     */
    @Deprecated
    public static void logStarted(TaskAttemptID taskAttemptId, long startTime, String hostName){
      logStarted(taskAttemptId, startTime, hostName, -1, Values.MAP.name());
    }
    
    /**
     * Log start time of this map task attempt.
     *  
     * @param taskAttemptId task attempt id
     * @param startTime start time of task attempt as reported by task tracker. 
     * @param trackerName name of the tracker executing the task attempt.
     * @param httpPort http port of the task tracker executing the task attempt
     * @param taskType Whether the attempt is cleanup or setup or map 
     */
    public static void logStarted(TaskAttemptID taskAttemptId, long startTime,
                                  String trackerName, int httpPort, 
                                  String taskType) {
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                   + taskAttemptId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.MapAttempt, 
                         new Keys[]{ Keys.TASK_TYPE, Keys.TASKID, 
                                     Keys.TASK_ATTEMPT_ID, Keys.START_TIME, 
                                     Keys.TRACKER_NAME, Keys.HTTP_PORT},
                         new String[]{taskType,
                                      taskAttemptId.getTaskID().toString(), 
                                      taskAttemptId.toString(), 
                                      String.valueOf(startTime), trackerName,
                                      httpPort == -1 ? "" : 
                                        String.valueOf(httpPort)}); 
        }
      }
    }
    
    /**
     * Log finish time of map task attempt. 
     * @param taskAttemptId task attempt id 
     * @param finishTime finish time
     * @param hostName host name 
     * @deprecated Use 
     * {@link #logFinished(TaskAttemptID, long, String, String, String, Counters)}
     */
    @Deprecated
    public static void logFinished(TaskAttemptID taskAttemptId, long finishTime, 
                                   String hostName){
      logFinished(taskAttemptId, finishTime, hostName, Values.MAP.name(), "", 
                  new Counters());
    }

    /**
     * Log finish time of map task attempt. 
     * 
     * @param taskAttemptId task attempt id 
     * @param finishTime finish time
     * @param hostName host name 
     * @param taskType Whether the attempt is cleanup or setup or map 
     * @param stateString state string of the task attempt
     * @param counter counters of the task attempt
     */
    public static void logFinished(TaskAttemptID taskAttemptId, 
                                   long finishTime, 
                                   String hostName,
                                   String taskType,
                                   String stateString, 
                                   Counters counter) {
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                   + taskAttemptId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.MapAttempt, 
                         new Keys[]{ Keys.TASK_TYPE, Keys.TASKID, 
                                     Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                     Keys.FINISH_TIME, Keys.HOSTNAME, 
                                     Keys.STATE_STRING, Keys.COUNTERS},
                         new String[]{taskType, 
                                      taskAttemptId.getTaskID().toString(),
                                      taskAttemptId.toString(), 
                                      Values.SUCCESS.name(),  
                                      String.valueOf(finishTime), hostName, 
                                      stateString, 
                                      counter.makeEscapedCompactString()}); 
        }
      }
    }

    /**
     * Log task attempt failed event.  
     * @param taskAttemptId task attempt id
     * @param timestamp timestamp
     * @param hostName hostname of this task attempt.
     * @param error error message if any for this task attempt.
     * @deprecated Use
     * {@link #logFailed(TaskAttemptID, long, String, String, String)} 
     */
    @Deprecated
    public static void logFailed(TaskAttemptID taskAttemptId, 
                                 long timestamp, String hostName, 
                                 String error) {
      logFailed(taskAttemptId, timestamp, hostName, error, Values.MAP.name());
    }

    /**
     * Log task attempt failed event. 
     *  
     * @param taskAttemptId task attempt id
     * @param timestamp timestamp
     * @param hostName hostname of this task attempt.
     * @param error error message if any for this task attempt. 
     * @param taskType Whether the attempt is cleanup or setup or map 
     */
    public static void logFailed(TaskAttemptID taskAttemptId, 
                                 long timestamp, String hostName, 
                                 String error, String taskType) {
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                   + taskAttemptId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.MapAttempt, 
                         new Keys[]{Keys.TASK_TYPE, Keys.TASKID, 
                                    Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                    Keys.FINISH_TIME, Keys.HOSTNAME, Keys.ERROR},
                         new String[]{ taskType, 
                                       taskAttemptId.getTaskID().toString(),
                                       taskAttemptId.toString(), 
                                       Values.FAILED.name(),
                                       String.valueOf(timestamp), 
                                       hostName, error}); 
        }
      }
    }
    
    /**
     * Log task attempt killed event.  
     * @param taskAttemptId task attempt id
     * @param timestamp timestamp
     * @param hostName hostname of this task attempt.
     * @param error error message if any for this task attempt. 
     * @deprecated Use 
     * {@link #logKilled(TaskAttemptID, long, String, String, String)}
     */
    @Deprecated
    public static void logKilled(TaskAttemptID taskAttemptId, 
                                 long timestamp, String hostName, String error){
      logKilled(taskAttemptId, timestamp, hostName, error, Values.MAP.name());
    } 
    
    /**
     * Log task attempt killed event.  
     * 
     * @param taskAttemptId task attempt id
     * @param timestamp timestamp
     * @param hostName hostname of this task attempt.
     * @param error error message if any for this task attempt. 
     * @param taskType Whether the attempt is cleanup or setup or map 
     */
    public static void logKilled(TaskAttemptID taskAttemptId, 
                                 long timestamp, String hostName,
                                 String error, String taskType) {
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                   + taskAttemptId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.MapAttempt, 
                         new Keys[]{Keys.TASK_TYPE, Keys.TASKID,
                                    Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                    Keys.FINISH_TIME, Keys.HOSTNAME,
                                    Keys.ERROR},
                         new String[]{ taskType, 
                                       taskAttemptId.getTaskID().toString(), 
                                       taskAttemptId.toString(),
                                       Values.KILLED.name(),
                                       String.valueOf(timestamp), 
                                       hostName, error}); 
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
     * @param taskAttemptId task attempt id
     * @param startTime start time
     * @param hostName host name 
     * @deprecated Use 
     * {@link #logStarted(TaskAttemptID, long, String, int, String)}
     */
    @Deprecated
    public static void logStarted(TaskAttemptID taskAttemptId, 
                                  long startTime, String hostName){
      logStarted(taskAttemptId, startTime, hostName, -1, Values.REDUCE.name());
    }
    
    /**
     * Log start time of  Reduce task attempt. 
     * 
     * @param taskAttemptId task attempt id
     * @param startTime start time
     * @param trackerName tracker name 
     * @param httpPort the http port of the tracker executing the task attempt
     * @param taskType Whether the attempt is cleanup or setup or reduce 
     */
    public static void logStarted(TaskAttemptID taskAttemptId, 
                                  long startTime, String trackerName, 
                                  int httpPort, 
                                  String taskType) {
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                   + taskAttemptId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.ReduceAttempt, 
                         new Keys[]{  Keys.TASK_TYPE, Keys.TASKID, 
                                      Keys.TASK_ATTEMPT_ID, Keys.START_TIME,
                                      Keys.TRACKER_NAME, Keys.HTTP_PORT},
                         new String[]{taskType,
                                      taskAttemptId.getTaskID().toString(), 
                                      taskAttemptId.toString(), 
                                      String.valueOf(startTime), trackerName,
                                      httpPort == -1 ? "" : 
                                        String.valueOf(httpPort)}); 
        }
      }
    }
    
    /**
     * Log finished event of this task. 
     * @param taskAttemptId task attempt id
     * @param shuffleFinished shuffle finish time
     * @param sortFinished sort finish time
     * @param finishTime finish time of task
     * @param hostName host name where task attempt executed
     * @deprecated Use 
     * {@link #logFinished(TaskAttemptID, long, long, long, String, String, String, Counters)}
     */
    @Deprecated
    public static void logFinished(TaskAttemptID taskAttemptId, long shuffleFinished, 
                                   long sortFinished, long finishTime, 
                                   String hostName){
      logFinished(taskAttemptId, shuffleFinished, sortFinished, 
                  finishTime, hostName, Values.REDUCE.name(),
                  "", new Counters());
    }
    
    /**
     * Log finished event of this task. 
     * 
     * @param taskAttemptId task attempt id
     * @param shuffleFinished shuffle finish time
     * @param sortFinished sort finish time
     * @param finishTime finish time of task
     * @param hostName host name where task attempt executed
     * @param taskType Whether the attempt is cleanup or setup or reduce 
     * @param stateString the state string of the attempt
     * @param counter counters of the attempt
     */
    public static void logFinished(TaskAttemptID taskAttemptId, 
                                   long shuffleFinished, 
                                   long sortFinished, long finishTime, 
                                   String hostName, String taskType,
                                   String stateString, Counters counter) {
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                   + taskAttemptId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.ReduceAttempt, 
                         new Keys[]{ Keys.TASK_TYPE, Keys.TASKID, 
                                     Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                     Keys.SHUFFLE_FINISHED, Keys.SORT_FINISHED,
                                     Keys.FINISH_TIME, Keys.HOSTNAME, 
                                     Keys.STATE_STRING, Keys.COUNTERS},
                         new String[]{taskType,
                                      taskAttemptId.getTaskID().toString(), 
                                      taskAttemptId.toString(), 
                                      Values.SUCCESS.name(), 
                                      String.valueOf(shuffleFinished), 
                                      String.valueOf(sortFinished),
                                      String.valueOf(finishTime), hostName,
                                      stateString, 
                                      counter.makeEscapedCompactString()}); 
        }
      }
    }
    
    /**
     * Log failed reduce task attempt. 
     * @param taskAttemptId task attempt id
     * @param timestamp time stamp when task failed
     * @param hostName host name of the task attempt.  
     * @param error error message of the task.
     * @deprecated Use 
     * {@link #logFailed(TaskAttemptID, long, String, String, String)} 
     */
    @Deprecated
    public static void logFailed(TaskAttemptID taskAttemptId, long timestamp, 
                                 String hostName, String error){
      logFailed(taskAttemptId, timestamp, hostName, error, Values.REDUCE.name());
    }
    
    /**
     * Log failed reduce task attempt.
     *  
     * @param taskAttemptId task attempt id
     * @param timestamp time stamp when task failed
     * @param hostName host name of the task attempt.  
     * @param error error message of the task. 
     * @param taskType Whether the attempt is cleanup or setup or reduce 
     */
    public static void logFailed(TaskAttemptID taskAttemptId, long timestamp, 
                                 String hostName, String error, 
                                 String taskType) {
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                   + taskAttemptId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.ReduceAttempt, 
                         new Keys[]{  Keys.TASK_TYPE, Keys.TASKID, 
                                      Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                      Keys.FINISH_TIME, Keys.HOSTNAME,
                                      Keys.ERROR },
                         new String[]{ taskType, 
                                       taskAttemptId.getTaskID().toString(), 
                                       taskAttemptId.toString(), 
                                       Values.FAILED.name(), 
                                       String.valueOf(timestamp), hostName, error }); 
        }
      }
    }
    
    /**
     * Log killed reduce task attempt. 
     * @param taskAttemptId task attempt id
     * @param timestamp time stamp when task failed
     * @param hostName host name of the task attempt.  
     * @param error error message of the task.
     * @deprecated Use 
     * {@link #logKilled(TaskAttemptID, long, String, String, String)} 
     */
    @Deprecated
    public static void logKilled(TaskAttemptID taskAttemptId, long timestamp, 
                                 String hostName, String error) {
      logKilled(taskAttemptId, timestamp, hostName, error, Values.REDUCE.name());
    }
    
    /**
     * Log killed reduce task attempt. 
     * 
     * @param taskAttemptId task attempt id
     * @param timestamp time stamp when task failed
     * @param hostName host name of the task attempt.  
     * @param error error message of the task. 
     * @param taskType Whether the attempt is cleanup or setup or reduce 
    */
    public static void logKilled(TaskAttemptID taskAttemptId, long timestamp, 
                                 String hostName, String error, 
                                 String taskType) {
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                   + taskAttemptId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.ReduceAttempt, 
                         new Keys[]{  Keys.TASK_TYPE, Keys.TASKID, 
                                      Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                      Keys.FINISH_TIME, Keys.HOSTNAME, 
                                      Keys.ERROR },
                         new String[]{ taskType,
                                       taskAttemptId.getTaskID().toString(), 
                                       taskAttemptId.toString(), 
                                       Values.KILLED.name(), 
                                       String.valueOf(timestamp), 
                                       hostName, error }); 
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
    private static long lastRan = 0; 

    /**
     * Cleans up history data. 
     */
    public void run(){
      if (isRunning){
        return; 
      }
      now = System.currentTimeMillis();
      // clean history only once a day at max
      if (lastRan != 0 && (now - lastRan) < ONE_DAY_IN_MS) {
        return; 
      }
      lastRan = now;  
      isRunning = true; 
      try {
        Path logDir = new Path(LOG_DIR);
        FileSystem fs = logDir.getFileSystem(jtConf);
        FileStatus[] historyFiles = fs.listStatus(logDir);
        // delete if older than 30 days
        if (historyFiles != null) {
          for (FileStatus f : historyFiles) {
            if (now - f.getModificationTime() > THIRTY_DAYS_IN_MS) {
              fs.delete(f.getPath(), true); 
              LOG.info("Deleting old history file : " + f.getPath());
            }
          }
        }
      } catch (IOException ie) {
        LOG.info("Error cleaning up history directory" + 
                 StringUtils.stringifyException(ie));
      }
      isRunning = false; 
    }
    
    static long getLastRan() {
      return lastRan;
    }
  }

  /**
   * Return the TaskLogsUrl of a particular TaskAttempt
   * 
   * @param attempt
   * @return the taskLogsUrl. null if http-port or tracker-name or
   *         task-attempt-id are unavailable.
   */
  public static String getTaskLogsUrl(JobHistory.TaskAttempt attempt) {
    if (attempt.get(Keys.HTTP_PORT).equals("")
        || attempt.get(Keys.TRACKER_NAME).equals("")
        || attempt.get(Keys.TASK_ATTEMPT_ID).equals("")) {
      return null;
    }

    String taskTrackerName =
      JobInProgress.convertTrackerNameToHostName(
        attempt.get(Keys.TRACKER_NAME));
    return TaskLogServlet.getTaskLogUrl(taskTrackerName, attempt
        .get(Keys.HTTP_PORT), attempt.get(Keys.TASK_ATTEMPT_ID));
  }
}
