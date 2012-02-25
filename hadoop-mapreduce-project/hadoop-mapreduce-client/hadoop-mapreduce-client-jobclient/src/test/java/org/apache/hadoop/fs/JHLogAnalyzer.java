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

package org.apache.hadoop.fs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * Job History Log Analyzer.
 * 
 * <h3>Description.</h3>
 * This a tool for parsing and analyzing history logs of map-reduce jobs.
 * History logs contain information about execution of jobs, tasks, and 
 * attempts. This tool focuses on submission, launch, start, and finish times,
 * as well as the success or failure of jobs, tasks, and attempts.
 * <p>
 * The analyzer calculates <em>per hour slot utilization</em> for the cluster 
 * as follows.
 * For each task attempt it divides the time segment from the start of the 
 * attempt t<sub>S</sub> to the finish t<sub>F</sub> into whole hours 
 * [t<sub>0</sub>, ..., t<sub>n</sub>], where t<sub>0</sub> <= t<sub>S</sub> 
 * is the maximal whole hour preceding t<sub>S</sub>, and
 * t<sub>n</sub> >= t<sub>F</sub> is the minimal whole hour after t<sub>F</sub>. 
 * Thus, [t<sub>0</sub>, ..., t<sub>n</sub>] covers the segment 
 * [t<sub>S</sub>, t<sub>F</sub>], during which the attempt was executed.
 * Each interval [t<sub>i</sub>, t<sub>i+1</sub>] fully contained in 
 * [t<sub>S</sub>, t<sub>F</sub>] corresponds to exactly one slot on
 * a map-reduce cluster (usually MAP-slot or REDUCE-slot).
 * If interval [t<sub>i</sub>, t<sub>i+1</sub>] only intersects with 
 * [t<sub>S</sub>, t<sub>F</sub>] then we say that the task 
 * attempt used just a fraction of the slot during this hour.
 * The fraction equals the size of the intersection.
 * Let slotTime(A, h) denote the number of slots calculated that way for a 
 * specific attempt A during hour h.
 * The tool then sums all slots for all attempts for every hour.
 * The result is the slot hour utilization of the cluster:
 * <tt>slotTime(h) = SUM<sub>A</sub> slotTime(A,h)</tt>.
 * <p>
 * Log analyzer calculates slot hours for <em>MAP</em> and <em>REDUCE</em> 
 * attempts separately.
 * <p>
 * Log analyzer distinguishes between <em>successful</em> and <em>failed</em>
 * attempts. Task attempt is considered successful if its own status is SUCCESS
 * and the statuses of the task and the job it is a part of are also SUCCESS.
 * Otherwise the task attempt is considered failed.
 * <p>
 * Map-reduce clusters are usually configured to have a fixed number of MAP 
 * and REDUCE slots per node. Thus the maximal possible number of slots on
 * the cluster is <tt>total_slots = total_nodes * slots_per_node</tt>.
 * Effective slot hour cannot exceed <tt>total_slots</tt> for successful
 * attempts.
 * <p>
 * <em>Pending time</em> characterizes the wait time of attempts.
 * It is calculated similarly to the slot hour except that the wait interval
 * starts when the job is submitted and ends when an attempt starts execution.
 * In addition to that pending time also includes intervals between attempts
 * of the same task if it was re-executed.
 * <p>
 * History log analyzer calculates two pending time variations. First is based
 * on job submission time as described above, second, starts the wait interval
 * when the job is launched rather than submitted.
 * 
 * <h3>Input.</h3>
 * The following input parameters can be specified in the argument string
 * to the job log analyzer:
 * <ul>
 * <li><tt>-historyDir inputDir</tt> specifies the location of the directory
 * where analyzer will be looking for job history log files.</li>
 * <li><tt>-resFile resultFile</tt> the name of the result file.</li>
 * <li><tt>-usersIncluded | -usersExcluded userList</tt> slot utilization and 
 * pending time can be calculated for all or for all but the specified users.
 * <br>
 * <tt>userList</tt> is a comma or semicolon separated list of users.</li>
 * <li><tt>-gzip</tt> is used if history log files are compressed.
 * Only {@link GzipCodec} is currently supported.</li>
 * <li><tt>-jobDelimiter pattern</tt> one can concatenate original log files into 
 * larger file(s) with the specified delimiter to recognize the end of the log
 * for one job from the next one.<br>
 * <tt>pattern</tt> is a java regular expression
 * {@link java.util.regex.Pattern}, which should match only the log delimiters.
 * <br>
 * E.g. pattern <tt>".!!FILE=.*!!"</tt> matches delimiters, which contain
 * the original history log file names in the following form:<br>
 * <tt>"$!!FILE=my.job.tracker.com_myJobId_user_wordcount.log!!"</tt></li>
 * <li><tt>-clean</tt> cleans up default directories used by the analyzer.</li>
 * <li><tt>-test</tt> test one file locally and exit;
 * does not require map-reduce.</li>
 * <li><tt>-help</tt> print usage.</li>
 * </ul>
 * 
 * <h3>Output.</h3>
 * The output file is formatted as a tab separated table consisting of four
 * columns: <tt>SERIES, PERIOD, TYPE, SLOT_HOUR</tt>.
 * <ul>
 * <li><tt>SERIES</tt> one of the four statistical series;</li>
 * <li><tt>PERIOD</tt> the start of the time interval in the following format:
 * <tt>"yyyy-mm-dd hh:mm:ss"</tt>;</li>
 * <li><tt>TYPE</tt> the slot type, e.g. MAP or REDUCE;</li>
 * <li><tt>SLOT_HOUR</tt> the value of the slot usage during this 
 * time interval.</li>
 * </ul>
 */
@SuppressWarnings("deprecation")
public class JHLogAnalyzer {
  private static final Log LOG = LogFactory.getLog(JHLogAnalyzer.class);
  // Constants
  private static final String JHLA_ROOT_DIR = 
                            System.getProperty("test.build.data", "stats/JHLA");
  private static final Path INPUT_DIR = new Path(JHLA_ROOT_DIR, "jhla_input");
  private static final String BASE_INPUT_FILE_NAME = "jhla_in_";
  private static final Path OUTPUT_DIR = new Path(JHLA_ROOT_DIR, "jhla_output");
  private static final Path RESULT_FILE = 
                            new Path(JHLA_ROOT_DIR, "jhla_result.txt");
  private static final Path DEFAULT_HISTORY_DIR = new Path("history");

  private static final int DEFAULT_TIME_INTERVAL_MSEC = 1000*60*60; // 1 hour

  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }

  static enum StatSeries {
    STAT_ALL_SLOT_TIME
          (AccumulatingReducer.VALUE_TYPE_LONG + "allSlotTime"),
    STAT_FAILED_SLOT_TIME
          (AccumulatingReducer.VALUE_TYPE_LONG + "failedSlotTime"),
    STAT_SUBMIT_PENDING_SLOT_TIME
          (AccumulatingReducer.VALUE_TYPE_LONG + "submitPendingSlotTime"),
    STAT_LAUNCHED_PENDING_SLOT_TIME
          (AccumulatingReducer.VALUE_TYPE_LONG + "launchedPendingSlotTime");    

    private String statName = null;
    private StatSeries(String name) {this.statName = name;}
    public String toString() {return statName;}
  }

  private static class FileCreateDaemon extends Thread {
    private static final int NUM_CREATE_THREADS = 10;
    private static volatile int numFinishedThreads;
    private static volatile int numRunningThreads;
    private static FileStatus[] jhLogFiles;

    FileSystem fs;
    int start;
    int end;

    FileCreateDaemon(FileSystem fs, int start, int end) {
      this.fs = fs;
      this.start = start;
      this.end = end;
    }

    public void run() {
      try {
        for(int i=start; i < end; i++) {
          String name = getFileName(i);
          Path controlFile = new Path(INPUT_DIR, "in_file_" + name);
          SequenceFile.Writer writer = null;
          try {
            writer = SequenceFile.createWriter(fs, fs.getConf(), controlFile,
                                               Text.class, LongWritable.class,
                                               CompressionType.NONE);
            String logFile = jhLogFiles[i].getPath().toString();
            writer.append(new Text(logFile), new LongWritable(0));
          } catch(Exception e) {
            throw new IOException(e);
          } finally {
            if (writer != null)
              writer.close();
            writer = null;
          }
        }
      } catch(IOException ex) {
        LOG.error("FileCreateDaemon failed.", ex);
      }
      numFinishedThreads++;
    }

    private static void createControlFile(FileSystem fs, Path jhLogDir
    ) throws IOException {
      fs.delete(INPUT_DIR, true);
      jhLogFiles = fs.listStatus(jhLogDir);

      numFinishedThreads = 0;
      try {
        int start = 0;
        int step = jhLogFiles.length / NUM_CREATE_THREADS
        + ((jhLogFiles.length % NUM_CREATE_THREADS) > 0 ? 1 : 0);
        FileCreateDaemon[] daemons = new FileCreateDaemon[NUM_CREATE_THREADS];
        numRunningThreads = 0;
        for(int tIdx=0; tIdx < NUM_CREATE_THREADS && start < jhLogFiles.length; tIdx++) {
          int end = Math.min(start + step, jhLogFiles.length);
          daemons[tIdx] = new FileCreateDaemon(fs, start, end);
          start += step;
          numRunningThreads++;
        }
        for(int tIdx=0; tIdx < numRunningThreads; tIdx++) {
          daemons[tIdx].start();
        }
      } finally {
        int prevValue = 0;
        while(numFinishedThreads < numRunningThreads) {
          if(prevValue < numFinishedThreads) {
            LOG.info("Finished " + numFinishedThreads + " threads out of " + numRunningThreads);
            prevValue = numFinishedThreads;
          }
          try {Thread.sleep(500);} catch (InterruptedException e) {}
        }
      }
    }
  }

  private static void createControlFile(FileSystem fs, Path jhLogDir
  ) throws IOException {
    LOG.info("creating control file: JH log dir = " + jhLogDir);
    FileCreateDaemon.createControlFile(fs, jhLogDir);
    LOG.info("created control file: JH log dir = " + jhLogDir);
  }

  private static String getFileName(int fIdx) {
    return BASE_INPUT_FILE_NAME + Integer.toString(fIdx);
  }

  /**
   * If keyVal is of the form KEY="VALUE", then this will return [KEY, VALUE]
   */
  private static String [] getKeyValue(String t) throws IOException {
    String[] keyVal = t.split("=\"*|\"");
    return keyVal;
  }

  /**
   * JobHistory log record.
   */
  private static class JobHistoryLog {
    String JOBID;
    String JOB_STATUS;
    long SUBMIT_TIME;
    long LAUNCH_TIME;
    long FINISH_TIME;
    long TOTAL_MAPS;
    long TOTAL_REDUCES;
    long FINISHED_MAPS;
    long FINISHED_REDUCES;
    String USER;
    Map<String, TaskHistoryLog> tasks;

    boolean isSuccessful() {
     return (JOB_STATUS != null) && JOB_STATUS.equals("SUCCESS");
    }

    void parseLine(String line) throws IOException {
      StringTokenizer tokens = new StringTokenizer(line);
      if(!tokens.hasMoreTokens())
        return;
      String what = tokens.nextToken();
      // Line should start with one of the following:
      // Job, Task, MapAttempt, ReduceAttempt
      if(what.equals("Job"))
        updateJob(tokens);
      else if(what.equals("Task"))
        updateTask(tokens);
      else if(what.indexOf("Attempt") >= 0)
        updateTaskAttempt(tokens);
    }

    private void updateJob(StringTokenizer tokens) throws IOException {
      while(tokens.hasMoreTokens()) {
        String t = tokens.nextToken();
        String[] keyVal = getKeyValue(t);
        if(keyVal.length < 2) continue;

        if(keyVal[0].equals("JOBID")) {
          if(JOBID == null)
            JOBID = new String(keyVal[1]);
          else if(!JOBID.equals(keyVal[1])) {
            LOG.error("Incorrect JOBID: "
                + keyVal[1].substring(0, Math.min(keyVal[1].length(), 100)) 
                + " expect " + JOBID);
            return;
          }
        }
        else if(keyVal[0].equals("JOB_STATUS"))
          JOB_STATUS = new String(keyVal[1]);
        else if(keyVal[0].equals("SUBMIT_TIME"))
          SUBMIT_TIME = Long.parseLong(keyVal[1]);
        else if(keyVal[0].equals("LAUNCH_TIME"))
          LAUNCH_TIME = Long.parseLong(keyVal[1]);
        else if(keyVal[0].equals("FINISH_TIME"))
          FINISH_TIME = Long.parseLong(keyVal[1]);
        else if(keyVal[0].equals("TOTAL_MAPS"))
          TOTAL_MAPS = Long.parseLong(keyVal[1]);
        else if(keyVal[0].equals("TOTAL_REDUCES"))
          TOTAL_REDUCES = Long.parseLong(keyVal[1]);
        else if(keyVal[0].equals("FINISHED_MAPS"))
          FINISHED_MAPS = Long.parseLong(keyVal[1]);
        else if(keyVal[0].equals("FINISHED_REDUCES"))
          FINISHED_REDUCES = Long.parseLong(keyVal[1]);
        else if(keyVal[0].equals("USER"))
          USER = new String(keyVal[1]);
      }
    }

    private void updateTask(StringTokenizer tokens) throws IOException {
      // unpack
      TaskHistoryLog task = new TaskHistoryLog().parse(tokens);
      if(task.TASKID == null) {
        LOG.error("TASKID = NULL for job " + JOBID);
        return;
      }
      // update or insert
      if(tasks == null)
        tasks = new HashMap<String, TaskHistoryLog>((int)(TOTAL_MAPS + TOTAL_REDUCES));
      TaskHistoryLog existing = tasks.get(task.TASKID);
      if(existing == null)
        tasks.put(task.TASKID, task);
      else
        existing.updateWith(task);
    }

    private void updateTaskAttempt(StringTokenizer tokens) throws IOException {
      // unpack
      TaskAttemptHistoryLog attempt = new TaskAttemptHistoryLog();
      String taskID = attempt.parse(tokens);
      if(taskID == null) return;
      if(tasks == null)
        tasks = new HashMap<String, TaskHistoryLog>((int)(TOTAL_MAPS + TOTAL_REDUCES));
      TaskHistoryLog existing = tasks.get(taskID);
      if(existing == null) {
        existing = new TaskHistoryLog(taskID);
        tasks.put(taskID, existing);
      }
      existing.updateWith(attempt);
    }
  }

  /**
   * TaskHistory log record.
   */
  private static class TaskHistoryLog {
    String TASKID;
    String TASK_TYPE;   // MAP, REDUCE, SETUP, CLEANUP
    String TASK_STATUS;
    long START_TIME;
    long FINISH_TIME;
    Map<String, TaskAttemptHistoryLog> attempts;

    TaskHistoryLog() {}

    TaskHistoryLog(String taskID) {
      TASKID = taskID;
    }

    boolean isSuccessful() {
      return (TASK_STATUS != null) && TASK_STATUS.equals("SUCCESS");
    }

    TaskHistoryLog parse(StringTokenizer tokens) throws IOException {
      while(tokens.hasMoreTokens()) {
        String t = tokens.nextToken();
        String[] keyVal = getKeyValue(t);
        if(keyVal.length < 2) continue;

        if(keyVal[0].equals("TASKID")) {
          if(TASKID == null)
            TASKID = new String(keyVal[1]);
          else if(!TASKID.equals(keyVal[1])) {
            LOG.error("Incorrect TASKID: "
                + keyVal[1].substring(0, Math.min(keyVal[1].length(), 100)) 
                + " expect " + TASKID);
            continue;
          }
        }
        else if(keyVal[0].equals("TASK_TYPE"))
          TASK_TYPE = new String(keyVal[1]);
        else if(keyVal[0].equals("TASK_STATUS"))
          TASK_STATUS = new String(keyVal[1]);
        else if(keyVal[0].equals("START_TIME"))
          START_TIME = Long.parseLong(keyVal[1]);
        else if(keyVal[0].equals("FINISH_TIME"))
          FINISH_TIME = Long.parseLong(keyVal[1]);
      }
      return this;
    }

    /**
     * Update with non-null fields of the same task log record.
     */
    void updateWith(TaskHistoryLog from) throws IOException {
      if(TASKID == null)
        TASKID = from.TASKID;
      else if(!TASKID.equals(from.TASKID)) {
        throw new IOException("Incorrect TASKID: " + from.TASKID
                            + " expect " + TASKID);
      }
      if(TASK_TYPE == null)
        TASK_TYPE = from.TASK_TYPE;
      else if(! TASK_TYPE.equals(from.TASK_TYPE)) {
        LOG.error(
            "Incorrect TASK_TYPE: " + from.TASK_TYPE + " expect " + TASK_TYPE
            + " for task " + TASKID);
        return;
      }
      if(from.TASK_STATUS != null)
        TASK_STATUS = from.TASK_STATUS;
      if(from.START_TIME > 0)
        START_TIME = from.START_TIME;
      if(from.FINISH_TIME > 0)
        FINISH_TIME = from.FINISH_TIME;
    }

    /**
     * Update with non-null fields of the task attempt log record.
     */
    void updateWith(TaskAttemptHistoryLog attempt) throws IOException {
      if(attempt.TASK_ATTEMPT_ID == null) {
        LOG.error("Unexpected TASK_ATTEMPT_ID = null for task " + TASKID);
        return;
      }
      if(attempts == null)
        attempts = new HashMap<String, TaskAttemptHistoryLog>();
      TaskAttemptHistoryLog existing = attempts.get(attempt.TASK_ATTEMPT_ID);
      if(existing == null)
        attempts.put(attempt.TASK_ATTEMPT_ID, attempt);
      else
        existing.updateWith(attempt);
      // update task start time
      if(attempt.START_TIME > 0 && 
          (this.START_TIME == 0 || this.START_TIME > attempt.START_TIME))
        START_TIME = attempt.START_TIME;
    }
  }

  /**
   * TaskAttemptHistory log record.
   */
  private static class TaskAttemptHistoryLog {
    String TASK_ATTEMPT_ID;
    String TASK_STATUS; // this task attempt status
    long START_TIME;
    long FINISH_TIME;
    long HDFS_BYTES_READ;
    long HDFS_BYTES_WRITTEN;
    long FILE_BYTES_READ;
    long FILE_BYTES_WRITTEN;

    /**
     * Task attempt is considered successful iff all three statuses
     * of the attempt, the task, and the job equal "SUCCESS".
     */
    boolean isSuccessful() {
      return (TASK_STATUS != null) && TASK_STATUS.equals("SUCCESS");
    }

    String parse(StringTokenizer tokens) throws IOException {
      String taskID = null;
      while(tokens.hasMoreTokens()) {
        String t = tokens.nextToken();
        String[] keyVal = getKeyValue(t);
        if(keyVal.length < 2) continue;

        if(keyVal[0].equals("TASKID")) {
          if(taskID == null)
            taskID = new String(keyVal[1]);
          else if(!taskID.equals(keyVal[1])) {
            LOG.error("Incorrect TASKID: " + keyVal[1] + " expect " + taskID);
            continue;
          }
        }
        else if(keyVal[0].equals("TASK_ATTEMPT_ID")) {
          if(TASK_ATTEMPT_ID == null)
            TASK_ATTEMPT_ID = new String(keyVal[1]);
          else if(!TASK_ATTEMPT_ID.equals(keyVal[1])) {
            LOG.error("Incorrect TASKID: " + keyVal[1] + " expect " + taskID);
            continue;
          }
        }
        else if(keyVal[0].equals("TASK_STATUS"))
          TASK_STATUS = new String(keyVal[1]);
        else if(keyVal[0].equals("START_TIME"))
          START_TIME = Long.parseLong(keyVal[1]);
        else if(keyVal[0].equals("FINISH_TIME"))
          FINISH_TIME = Long.parseLong(keyVal[1]);
      }
      return taskID;
    }

    /**
     * Update with non-null fields of the same task attempt log record.
     */
    void updateWith(TaskAttemptHistoryLog from) throws IOException {
      if(TASK_ATTEMPT_ID == null)
        TASK_ATTEMPT_ID = from.TASK_ATTEMPT_ID;
      else if(! TASK_ATTEMPT_ID.equals(from.TASK_ATTEMPT_ID)) {
        throw new IOException(
            "Incorrect TASK_ATTEMPT_ID: " + from.TASK_ATTEMPT_ID 
            + " expect " + TASK_ATTEMPT_ID);
      }
      if(from.TASK_STATUS != null)
        TASK_STATUS = from.TASK_STATUS;
      if(from.START_TIME > 0)
        START_TIME = from.START_TIME;
      if(from.FINISH_TIME > 0)
        FINISH_TIME = from.FINISH_TIME;
      if(from.HDFS_BYTES_READ > 0)
        HDFS_BYTES_READ = from.HDFS_BYTES_READ;
      if(from.HDFS_BYTES_WRITTEN > 0)
        HDFS_BYTES_WRITTEN = from.HDFS_BYTES_WRITTEN;
      if(from.FILE_BYTES_READ > 0)
        FILE_BYTES_READ = from.FILE_BYTES_READ;
      if(from.FILE_BYTES_WRITTEN > 0)
        FILE_BYTES_WRITTEN = from.FILE_BYTES_WRITTEN;
    }
  }

  /**
   * Key = statName*date-time*taskType
   * Value = number of msec for the our
   */
  private static class IntervalKey {
    static final String KEY_FIELD_DELIMITER = "*";
    String statName;
    String dateTime;
    String taskType;

    IntervalKey(String stat, long timeMSec, String taskType) {
      statName = stat;
      SimpleDateFormat dateF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      dateTime = dateF.format(new Date(timeMSec));
      this.taskType = taskType;
    }

    IntervalKey(String key) {
      StringTokenizer keyTokens = new StringTokenizer(key, KEY_FIELD_DELIMITER);
      if(!keyTokens.hasMoreTokens()) return;
      statName = keyTokens.nextToken();
      if(!keyTokens.hasMoreTokens()) return;
      dateTime = keyTokens.nextToken();
      if(!keyTokens.hasMoreTokens()) return;
      taskType = keyTokens.nextToken();
    }

    void setStatName(String stat) {
      statName = stat;
    }

    String getStringKey() {
      return statName + KEY_FIELD_DELIMITER +
             dateTime + KEY_FIELD_DELIMITER +
             taskType;
    }

    Text getTextKey() {
      return new Text(getStringKey());
    }

    public String toString() {
      return getStringKey();
    }
  }

  /**
   * Mapper class.
   */
  private static class JHLAMapper extends IOMapperBase<Object> {
    /**
     * A line pattern, which delimits history logs of different jobs,
     * if multiple job logs are written in the same file.
     * Null value means only one job log per file is expected.
     * The pattern should be a regular expression as in
     * {@link String#matches(String)}.
     */
    String jobDelimiterPattern;
    int maxJobDelimiterLineLength;
    /** Count only these users jobs */
    Collection<String> usersIncluded;
    /** Exclude jobs of the following users */
    Collection<String> usersExcluded;
    /** Type of compression for compressed files: gzip */
    Class<? extends CompressionCodec> compressionClass;

    JHLAMapper() throws IOException {
    }

    JHLAMapper(Configuration conf) throws IOException {
      configure(new JobConf(conf));
    }

    public void configure(JobConf conf) {
      super.configure(conf );
      usersIncluded = getUserList(conf.get("jhla.users.included", null));
      usersExcluded = getUserList(conf.get("jhla.users.excluded", null));
      String zipClassName = conf.get("jhla.compression.class", null);
      try {
        compressionClass = (zipClassName == null) ? null : 
          Class.forName(zipClassName).asSubclass(CompressionCodec.class);
      } catch(Exception e) {
        throw new RuntimeException("Compression codec not found: ", e);
      }
      jobDelimiterPattern = conf.get("jhla.job.delimiter.pattern", null);
      maxJobDelimiterLineLength = conf.getInt("jhla.job.delimiter.length", 512);
    }

    @Override
    public void map(Text key, 
                    LongWritable value,
                    OutputCollector<Text, Text> output, 
                    Reporter reporter) throws IOException {
      String name = key.toString();
      long longValue = value.get();
      
      reporter.setStatus("starting " + name + " ::host = " + hostName);
      
      long tStart = System.currentTimeMillis();
      parseLogFile(fs, new Path(name), longValue, output, reporter);
      long tEnd = System.currentTimeMillis();
      long execTime = tEnd - tStart;
      
      reporter.setStatus("finished " + name + " ::host = " + hostName +
          " in " + execTime/1000 + " sec.");
    }

    public Object doIO(Reporter reporter, 
                       String path, // full path of history log file 
                       long offset  // starting offset within the file
                       ) throws IOException {
      return null;
    }

    void collectStats(OutputCollector<Text, Text> output, 
        String name,
        long execTime,
        Object jobObjects) throws IOException {
    }

    private boolean isEndOfJobLog(String line) {
      if(jobDelimiterPattern == null)
        return false;
      return line.matches(jobDelimiterPattern);
    }

    /**
     * Collect information about one job.
     * 
     * @param fs - file system
     * @param filePath - full path of a history log file
     * @param offset - starting offset in the history log file
     * @throws IOException
     */
    public void parseLogFile(FileSystem fs,
                                    Path filePath,
                                    long offset,
                                    OutputCollector<Text, Text> output,
                                    Reporter reporter
                                  ) throws IOException {
      InputStream in = null;
      try {
        // open file & seek
        FSDataInputStream stm = fs.open(filePath);
        stm.seek(offset);
        in = stm;
        LOG.info("Opened " + filePath);
        reporter.setStatus("Opened " + filePath);
        // get a compression filter if specified
        if(compressionClass != null) {
          CompressionCodec codec = (CompressionCodec)
            ReflectionUtils.newInstance(compressionClass, new Configuration());
          in = codec.createInputStream(stm);
          LOG.info("Codec created " + filePath);
          reporter.setStatus("Codec created " + filePath);
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        LOG.info("Reader created " + filePath);
        // skip to the next job log start
        long processed = 0L;
        if(jobDelimiterPattern != null) {
          for(String line = reader.readLine();
                line != null; line = reader.readLine()) {
            if((stm.getPos() - processed) > 100000) {
              processed = stm.getPos();
              reporter.setStatus("Processing " + filePath + " at " + processed);
            }
            if(isEndOfJobLog(line))
              break;
          }
        }
        // parse lines and update job history
        JobHistoryLog jh = new JobHistoryLog();
        int jobLineCount = 0;
        for(String line = readLine(reader);
              line != null; line = readLine(reader)) {
          jobLineCount++;
          if((stm.getPos() - processed) > 20000) {
            processed = stm.getPos();
            long numTasks = (jh.tasks == null ? 0 : jh.tasks.size());
            String txt = "Processing " + filePath + " at " + processed
                    + " # tasks = " + numTasks;
            reporter.setStatus(txt);
            LOG.info(txt);
          }
          if(isEndOfJobLog(line)) {
            if(jh.JOBID != null) {
              LOG.info("Finished parsing job: " + jh.JOBID
                     + " line count = " + jobLineCount);
              collectJobStats(jh, output, reporter);
              LOG.info("Collected stats for job: " + jh.JOBID);
            }
            jh = new JobHistoryLog();
            jobLineCount = 0;
          } else
            jh.parseLine(line);
        }
        if(jh.JOBID == null) {
          LOG.error("JOBID = NULL in " + filePath + " at " + processed);
          return;
        }
        collectJobStats(jh, output, reporter);
      } catch(Exception ie) {
        // parsing errors can happen if the file has been truncated
        LOG.error("JHLAMapper.parseLogFile", ie);
        reporter.setStatus("JHLAMapper.parseLogFile failed "
                          + StringUtils.stringifyException(ie));
        throw new IOException("Job failed.", ie);
      } finally {
        if(in != null) in.close();
      }
    }

    /**
     * Read lines until one ends with a " ." or "\" "
     */
    private StringBuffer resBuffer = new StringBuffer();
    private String readLine(BufferedReader reader) throws IOException {
      resBuffer.setLength(0);
      reader.mark(maxJobDelimiterLineLength);
      for(String line = reader.readLine();
                line != null; line = reader.readLine()) {
        if(isEndOfJobLog(line)) {
          if(resBuffer.length() == 0)
            resBuffer.append(line);
          else
            reader.reset();
          break;
        }
        if(resBuffer.length() == 0)
          resBuffer.append(line);
        else if(resBuffer.length() < 32000)
          resBuffer.append(line);
        if(line.endsWith(" .") || line.endsWith("\" ")) {
          break;
        }
        reader.mark(maxJobDelimiterLineLength);
      }
      String result = resBuffer.length() == 0 ? null : resBuffer.toString();
      resBuffer.setLength(0);
      return result;
    }

    private void collectPerIntervalStats(OutputCollector<Text, Text> output,
        long start, long finish, String taskType,
        StatSeries ... stats) throws IOException {
      long curInterval = (start / DEFAULT_TIME_INTERVAL_MSEC)
                                * DEFAULT_TIME_INTERVAL_MSEC;
      long curTime = start;
      long accumTime = 0;
      while(curTime < finish) {
        // how much of the task time belonged to current interval
        long nextInterval = curInterval + DEFAULT_TIME_INTERVAL_MSEC;
        long intervalTime = ((finish < nextInterval) ? 
            finish : nextInterval) - curTime;
        IntervalKey key = new IntervalKey("", curInterval, taskType);
        Text val = new Text(String.valueOf(intervalTime));
        for(StatSeries statName : stats) {
          key.setStatName(statName.toString());
          output.collect(key.getTextKey(), val);
        }

        curTime = curInterval = nextInterval;
        accumTime += intervalTime;
      }
      // For the pending stat speculative attempts may intersect.
      // Only one of them is considered pending.
      assert accumTime == finish - start || finish < start;
    }

    private void collectJobStats(JobHistoryLog jh,
                                        OutputCollector<Text, Text> output,
                                        Reporter reporter
                                        ) throws IOException {
      if(jh == null)
        return;
      if(jh.tasks == null)
        return;
      if(jh.SUBMIT_TIME <= 0)
        throw new IOException("Job " + jh.JOBID 
                            + " SUBMIT_TIME = " + jh.SUBMIT_TIME);
      if(usersIncluded != null && !usersIncluded.contains(jh.USER))
          return;
      if(usersExcluded != null && usersExcluded.contains(jh.USER))
          return;

      int numAttempts = 0;
      long totalTime = 0;
      boolean jobSuccess = jh.isSuccessful();
      long jobWaitTime = jh.LAUNCH_TIME - jh.SUBMIT_TIME;
      // attemptSubmitTime is the job's SUBMIT_TIME,
      // or the previous attempt FINISH_TIME for all subsequent attempts
      for(TaskHistoryLog th : jh.tasks.values()) {
        if(th.attempts == null)
          continue;
        // Task is successful iff both the task and the job are a "SUCCESS"
        long attemptSubmitTime = jh.LAUNCH_TIME;
        boolean taskSuccess = jobSuccess && th.isSuccessful();
        for(TaskAttemptHistoryLog tah : th.attempts.values()) {
          // Task attempt is considered successful iff all three statuses
          // of the attempt, the task, and the job equal "SUCCESS"
          boolean success = taskSuccess && tah.isSuccessful();
          if(tah.START_TIME == 0) {
            LOG.error("Start time 0 for task attempt " + tah.TASK_ATTEMPT_ID);
            continue;
          }
          if(tah.FINISH_TIME < tah.START_TIME) {
            LOG.error("Finish time " + tah.FINISH_TIME + " is less than " +
            		"Start time " + tah.START_TIME + " for task attempt " +
            		tah.TASK_ATTEMPT_ID);
            tah.FINISH_TIME = tah.START_TIME;
          }

          if(!"MAP".equals(th.TASK_TYPE) && !"REDUCE".equals(th.TASK_TYPE) &&
             !"CLEANUP".equals(th.TASK_TYPE) && !"SETUP".equals(th.TASK_TYPE)) {
            LOG.error("Unexpected TASK_TYPE = " + th.TASK_TYPE
            + " for attempt " + tah.TASK_ATTEMPT_ID);
          }

          collectPerIntervalStats(output,
                  attemptSubmitTime, tah.START_TIME, th.TASK_TYPE,
                  StatSeries.STAT_LAUNCHED_PENDING_SLOT_TIME);
          collectPerIntervalStats(output,
                  attemptSubmitTime - jobWaitTime, tah.START_TIME, th.TASK_TYPE,
                  StatSeries.STAT_SUBMIT_PENDING_SLOT_TIME);
          if(success)
            collectPerIntervalStats(output,
                  tah.START_TIME, tah.FINISH_TIME, th.TASK_TYPE,
                  StatSeries.STAT_ALL_SLOT_TIME);
          else
            collectPerIntervalStats(output,
                  tah.START_TIME, tah.FINISH_TIME, th.TASK_TYPE,
                  StatSeries.STAT_ALL_SLOT_TIME,
                  StatSeries.STAT_FAILED_SLOT_TIME);
          totalTime += (tah.FINISH_TIME - tah.START_TIME);
          numAttempts++;
          if(numAttempts % 500 == 0) {
            reporter.setStatus("Processing " + jh.JOBID + " at " + numAttempts);
          }
          attemptSubmitTime = tah.FINISH_TIME;
        }
      }
      LOG.info("Total    Maps = " + jh.TOTAL_MAPS
          + "  Reduces = " + jh.TOTAL_REDUCES);
      LOG.info("Finished Maps = " + jh.FINISHED_MAPS
          + "  Reduces = " + jh.FINISHED_REDUCES);
      LOG.info("numAttempts = " + numAttempts);
      LOG.info("totalTime   = " + totalTime);
      LOG.info("averageAttemptTime = " 
          + (numAttempts==0 ? 0 : totalTime/numAttempts));
      LOG.info("jobTotalTime = " + (jh.FINISH_TIME <= jh.SUBMIT_TIME? 0 :
                                    jh.FINISH_TIME - jh.SUBMIT_TIME));
    }
  }

  public static class JHLAPartitioner implements Partitioner<Text, Text> {
    static final int NUM_REDUCERS = 9;

    public void configure(JobConf conf) {}

    public int getPartition(Text key, Text value, int numPartitions) {
      IntervalKey intKey = new IntervalKey(key.toString());
      if(intKey.statName.equals(StatSeries.STAT_ALL_SLOT_TIME.toString())) {
        if(intKey.taskType.equals("MAP"))
          return 0;
        else if(intKey.taskType.equals("REDUCE"))
          return 1;
      } else if(intKey.statName.equals(
          StatSeries.STAT_SUBMIT_PENDING_SLOT_TIME.toString())) {
        if(intKey.taskType.equals("MAP"))
          return 2;
        else if(intKey.taskType.equals("REDUCE"))
          return 3;
      } else if(intKey.statName.equals(
          StatSeries.STAT_LAUNCHED_PENDING_SLOT_TIME.toString())) {
        if(intKey.taskType.equals("MAP"))
          return 4;
        else if(intKey.taskType.equals("REDUCE"))
          return 5;
      } else if(intKey.statName.equals(
          StatSeries.STAT_FAILED_SLOT_TIME.toString())) {
        if(intKey.taskType.equals("MAP"))
          return 6;
        else if(intKey.taskType.equals("REDUCE"))
          return 7;
      }
      return 8;
    }
  }

  private static void runJHLA(
          Class<? extends Mapper<Text, LongWritable, Text, Text>> mapperClass, 
          Path outputDir,
          Configuration fsConfig) throws IOException {
    JobConf job = new JobConf(fsConfig, JHLogAnalyzer.class);

    job.setPartitionerClass(JHLAPartitioner.class);

    FileInputFormat.setInputPaths(job, INPUT_DIR);
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(mapperClass);
    job.setReducerClass(AccumulatingReducer.class);

    FileOutputFormat.setOutputPath(job, outputDir);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(JHLAPartitioner.NUM_REDUCERS);
    JobClient.runJob(job);
  }

  private static class LoggingCollector implements OutputCollector<Text, Text> {
    public void collect(Text key, Text value) throws IOException {
      LOG.info(key + " == " + value);
    }
  }

  /**
   * Run job history log analyser.
   */
  public static void main(String[] args) {
    Path resFileName = RESULT_FILE;
    Configuration conf = new Configuration();

    try {
      conf.setInt("test.io.file.buffer.size", 0);
      Path historyDir = DEFAULT_HISTORY_DIR;
      String testFile = null;
      boolean cleanup = false;

      boolean initControlFiles = true;
      for (int i = 0; i < args.length; i++) {       // parse command line
        if (args[i].equalsIgnoreCase("-historyDir")) {
          historyDir = new Path(args[++i]);
        } else if (args[i].equalsIgnoreCase("-resFile")) {
          resFileName = new Path(args[++i]);
        } else if (args[i].equalsIgnoreCase("-usersIncluded")) {
          conf.set("jhla.users.included", args[++i]);
        } else if (args[i].equalsIgnoreCase("-usersExcluded")) {
          conf.set("jhla.users.excluded", args[++i]);
        } else if (args[i].equalsIgnoreCase("-gzip")) {
          conf.set("jhla.compression.class", GzipCodec.class.getCanonicalName());
        } else if (args[i].equalsIgnoreCase("-jobDelimiter")) {
          conf.set("jhla.job.delimiter.pattern", args[++i]);
        } else if (args[i].equalsIgnoreCase("-jobDelimiterLength")) {
          conf.setInt("jhla.job.delimiter.length", Integer.parseInt(args[++i]));
        } else if(args[i].equalsIgnoreCase("-noInit")) {
          initControlFiles = false;
        } else if(args[i].equalsIgnoreCase("-test")) {
          testFile = args[++i];
        } else if(args[i].equalsIgnoreCase("-clean")) {
          cleanup = true;
        } else if(args[i].equalsIgnoreCase("-jobQueue")) {
          conf.set("mapred.job.queue.name", args[++i]);
        } else if(args[i].startsWith("-Xmx")) {
          conf.set("mapred.child.java.opts", args[i]);
        } else {
          printUsage();
        }
      }

      if(cleanup) {
        cleanup(conf);
        return;
      }
      if(testFile != null) {
        LOG.info("Start JHLA test ============ ");
        LocalFileSystem lfs = FileSystem.getLocal(conf);
        conf.set("fs.defaultFS", "file:///");
        JHLAMapper map = new JHLAMapper(conf);
        map.parseLogFile(lfs, new Path(testFile), 0L,
                         new LoggingCollector(), Reporter.NULL);
        return;
      }

      FileSystem fs = FileSystem.get(conf);
      if(initControlFiles)
        createControlFile(fs, historyDir);
      long tStart = System.currentTimeMillis();
      runJHLA(JHLAMapper.class, OUTPUT_DIR, conf);
      long execTime = System.currentTimeMillis() - tStart;

      analyzeResult(fs, 0, execTime, resFileName);
    } catch(IOException e) {
      System.err.print(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }


  private static void printUsage() {
    String className = JHLogAnalyzer.class.getSimpleName();
    System.err.println("Usage: " + className
      + "\n\t[-historyDir inputDir] | [-resFile resultFile] |"
      + "\n\t[-usersIncluded | -usersExcluded userList] |"
      + "\n\t[-gzip] | [-jobDelimiter pattern] |"
      + "\n\t[-help | -clean | -test testFile]");
    System.exit(-1);
  }

  private static Collection<String> getUserList(String users) {
    if(users == null)
      return null;
    StringTokenizer tokens = new StringTokenizer(users, ",;");
    Collection<String> userList = new ArrayList<String>(tokens.countTokens());
    while(tokens.hasMoreTokens())
      userList.add(tokens.nextToken());
    return userList;
  }

  /**
   * Result is combined from all reduce output files and is written to
   * RESULT_FILE in the format
   * column 1: 
   */
  private static void analyzeResult( FileSystem fs, 
                                     int testType,
                                     long execTime,
                                     Path resFileName
                                     ) throws IOException {
    LOG.info("Analizing results ...");
    DataOutputStream out = null;
    BufferedWriter writer = null;
    try {
      out = new DataOutputStream(fs.create(resFileName));
      writer = new BufferedWriter(new OutputStreamWriter(out));
      writer.write("SERIES\tPERIOD\tTYPE\tSLOT_HOUR\n");
      FileStatus[] reduceFiles = fs.listStatus(OUTPUT_DIR);
      assert reduceFiles.length == JHLAPartitioner.NUM_REDUCERS;
      for(int i = 0; i < JHLAPartitioner.NUM_REDUCERS; i++) {
        DataInputStream in = null;
        BufferedReader lines = null;
        try {
          in = fs.open(reduceFiles[i].getPath());
          lines = new BufferedReader(new InputStreamReader(in));
    
          String line;
          while((line = lines.readLine()) != null) {
            StringTokenizer tokens = new StringTokenizer(line, "\t*");
            String attr = tokens.nextToken();
            String dateTime = tokens.nextToken();
            String taskType = tokens.nextToken();
            double val = Long.parseLong(tokens.nextToken()) /
                                    (double)DEFAULT_TIME_INTERVAL_MSEC;
            writer.write(attr.substring(2));  // skip the stat type "l:"
            writer.write("\t");
            writer.write(dateTime);
            writer.write("\t");
            writer.write(taskType);
            writer.write("\t");
            writer.write(String.valueOf((float)val));
            writer.newLine();
          }
        } finally {
          if(lines != null) lines.close();
          if(in != null) in.close();
        }
      }
    } finally {
      if(writer != null) writer.close();
      if(out != null) out.close();
    }
    LOG.info("Analizing results ... done.");
  }

  private static void cleanup(Configuration conf) throws IOException {
    LOG.info("Cleaning up test files");
    FileSystem fs = FileSystem.get(conf);
    fs.delete(new Path(JHLA_ROOT_DIR), true);
  }
}
