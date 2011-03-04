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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TaskLog.LogFileDetail;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapreduce.server.tasktracker.JVMInfo;
import org.apache.hadoop.mapreduce.server.tasktracker.userlogs.JvmFinishedEvent;
import org.apache.hadoop.mapreduce.server.tasktracker.userlogs.UserLogManager;
import org.apache.hadoop.mapreduce.split.JobSplit;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Verify the logs' truncation functionality.
 */
public class TestTaskLogsTruncater {

  static final Log LOG = LogFactory.getLog(TestTaskLogsTruncater.class);

  /**
   * clean-up any stale directories after enabling writable permissions for all
   * attempt-dirs.
   * 
   * @throws IOException
   */
  @After
  public void tearDown() throws IOException {
    File logDir = TaskLog.getUserLogDir();
    for (File attemptDir : logDir.listFiles()) {
      attemptDir.setWritable(true);
      FileUtil.fullyDelete(attemptDir);
    }
  }

  void writeRealBytes(TaskAttemptID firstAttemptID,
      TaskAttemptID attemptID, LogName logName, long numBytes, char data)
      throws IOException {

    File logFile = TaskLog.getTaskLogFile(firstAttemptID, false, logName);
    File logLocation = logFile.getParentFile();

    LOG.info("Going to write " + numBytes + " real bytes to the log file "
        + logFile);

    if (!logLocation.exists()
        && !logLocation.mkdirs()) {
      throw new IOException("Couldn't create all ancestor dirs for "
          + logFile);
    }

    File attemptDir = TaskLog.getAttemptDir(attemptID, false);
    if (!attemptDir.exists() && !attemptDir.mkdirs()) {
      throw new IOException("Couldn't create all ancestor dirs for "
          + logFile);
    }

    // Need to call up front to set currenttaskid.
    TaskLog.syncLogs(logLocation.toString(), attemptID, false);

    FileWriter writer = new FileWriter(logFile, true);
    for (long i = 0; i < numBytes; i++) {
      writer.write(data);
    }
    writer.close();
    TaskLog.syncLogs(logLocation.toString(), attemptID, false);
    LOG.info("Written " + numBytes + " real bytes to the log file "
        + logFile);
  }

  private static Map<LogName, Long> getAllLogsFileLengths(
      TaskAttemptID tid, boolean isCleanup) throws IOException {
    Map<LogName, Long> allLogsFileLengths = new HashMap<LogName, Long>();

    // If the index file doesn't exist, we cannot get log-file lengths. So set
    // them to zero.
    if (!TaskLog.getIndexFile(tid, isCleanup).exists()) {
      for (LogName log : LogName.values()) {
        allLogsFileLengths.put(log, Long.valueOf(0));
      }
      return allLogsFileLengths;
    }

    Map<LogName, LogFileDetail> logFilesDetails =
        TaskLog.getAllLogsFileDetails(tid, isCleanup);
    for (LogName log : logFilesDetails.keySet()) {
      allLogsFileLengths.put(log,
          Long.valueOf(logFilesDetails.get(log).length));
    }
    return allLogsFileLengths;
  }

  private Configuration setRetainSizes(long mapRetainSize,
      long reduceRetainSize) {
    Configuration conf = new Configuration();
    conf.setLong(TaskLogsTruncater.MAP_USERLOG_RETAIN_SIZE, mapRetainSize);
    conf.setLong(TaskLogsTruncater.REDUCE_USERLOG_RETAIN_SIZE, reduceRetainSize);
    return conf;
  }

  /**
   * Test cases which don't need any truncation of log-files. Without JVM-reuse.
   * 
   * @throws IOException
   */
  @Test
  public void testNoTruncationNeeded() throws IOException {
    Configuration conf = setRetainSizes(1000L, 1000L);
    UserLogManager logManager = new UtilsForTests.InLineUserLogManager(conf);

    TaskID baseId = new TaskID();
    int taskcount = 0;

    TaskAttemptID attemptID = new TaskAttemptID(baseId, taskcount++);
    Task task = new MapTask(null, attemptID, 0, new JobSplit.TaskSplitIndex(),
                            0);

    // Let the tasks write logs within retain-size
    for (LogName log : LogName.values()) {
      writeRealBytes(attemptID, attemptID, log, 500, 'H');
    }
    File logIndex = TaskLog.getIndexFile(attemptID, false);
    long indexModificationTimeStamp = logIndex.lastModified();

    File attemptDir = TaskLog.getAttemptDir(attemptID, false);
    assertTrue(attemptDir + " doesn't exist!", attemptDir.exists());
    assertEquals("index file got modified", indexModificationTimeStamp,
        logIndex.lastModified());

    // Finish the task and the JVM too.
    JVMInfo jvmInfo = new JVMInfo(attemptDir, Arrays.asList(task));
    logManager.addLogEvent(new JvmFinishedEvent(jvmInfo));

    // There should be no truncation of the log-file.
    assertTrue(attemptDir.exists());
    assertEquals("index file got modified", indexModificationTimeStamp,
        logIndex.lastModified());

    Map<LogName, Long> logLengths = getAllLogsFileLengths(attemptID, false);
    for (LogName log : LogName.values()) {
      File logFile = TaskLog.getTaskLogFile(attemptID, false, log);
      assertEquals(500, logFile.length());
      // The index file should also be proper.
      assertEquals(500, logLengths.get(log).longValue());
    }

    // truncate it once again
    logManager.addLogEvent(new JvmFinishedEvent(jvmInfo));
    assertEquals("index file got modified", indexModificationTimeStamp,
        logIndex.lastModified());
    
    logLengths = getAllLogsFileLengths(attemptID, false);
    for (LogName log : LogName.values()) {
      File logFile = TaskLog.getTaskLogFile(attemptID, false, log);
      assertEquals(500, logFile.length());
      // The index file should also be proper.
      assertEquals(500, logLengths.get(log).longValue());
    }
  }

  /**
   * Test the disabling of truncation of log-file.
   * 
   * @throws IOException
   */
  @Test
  public void testDisabledLogTruncation() throws IOException {
    // Anything less than 0 disables the truncation.
    Configuration conf = setRetainSizes(-1L, -1L);
    UserLogManager logManager = new UtilsForTests.InLineUserLogManager(conf);

    TaskID baseId = new TaskID();
    int taskcount = 0;

    TaskAttemptID attemptID = new TaskAttemptID(baseId, taskcount++);
    Task task = new MapTask(null, attemptID, 0, new JobSplit.TaskSplitIndex(),
                            0);

    // Let the tasks write some logs
    for (LogName log : LogName.values()) {
      writeRealBytes(attemptID, attemptID, log, 1500, 'H');
    }

    File attemptDir = TaskLog.getAttemptDir(attemptID, false);
    assertTrue(attemptDir + " doesn't exist!", attemptDir.exists());

    // Finish the task and the JVM too.
    JVMInfo jvmInfo = new JVMInfo(attemptDir, Arrays.asList(task));
    logManager.addLogEvent(new JvmFinishedEvent(jvmInfo));

    // The log-file should not be truncated.
    assertTrue(attemptDir.exists());
    Map<LogName, Long> logLengths = getAllLogsFileLengths(attemptID, false);
    for (LogName log : LogName.values()) {
      File logFile = TaskLog.getTaskLogFile(attemptID, false, log);
      assertEquals(1500, logFile.length());
      // The index file should also be proper.
      assertEquals(1500, logLengths.get(log).longValue());
    }
  }

  /**
   * Test the truncation of log-file when JVMs are not reused.
   * 
   * @throws IOException
   */
  @Test
  public void testLogTruncationOnFinishing() throws IOException {
    Configuration conf = setRetainSizes(1000L, 1000L);
    UserLogManager logManager = new UtilsForTests.InLineUserLogManager(conf);

    TaskID baseId = new TaskID();
    int taskcount = 0;

    TaskAttemptID attemptID = new TaskAttemptID(baseId, taskcount++);
    Task task = new MapTask(null, attemptID, 0, new JobSplit.TaskSplitIndex(), 
                            0);

    // Let the tasks write logs more than retain-size
    for (LogName log : LogName.values()) {
      writeRealBytes(attemptID, attemptID, log, 1500, 'H');
    }

    File attemptDir = TaskLog.getAttemptDir(attemptID, false);
    assertTrue(attemptDir + " doesn't exist!", attemptDir.exists());

    // Finish the task and the JVM too.
    JVMInfo jvmInfo = new JVMInfo(attemptDir, Arrays.asList(task));
    logManager.addLogEvent(new JvmFinishedEvent(jvmInfo));

    // The log-file should now be truncated.
    assertTrue(attemptDir.exists());

    Map<LogName, Long> logLengths = getAllLogsFileLengths(attemptID, false);
    for (LogName log : LogName.values()) {
      File logFile = TaskLog.getTaskLogFile(attemptID, false, log);
      assertEquals(1000, logFile.length());
      // The index file should also be proper.
      assertEquals(1000, logLengths.get(log).longValue());
    }

    // truncate once again
    logLengths = getAllLogsFileLengths(attemptID, false);
    for (LogName log : LogName.values()) {
      File logFile = TaskLog.getTaskLogFile(attemptID, false, log);
      assertEquals(1000, logFile.length());
      // The index file should also be proper.
      assertEquals(1000, logLengths.get(log).longValue());
    }
  }

  /**
   * Test the truncation of log-file.
   * 
   * It writes two log files and truncates one, does not truncate other. 
   * 
   * @throws IOException
   */
  @Test
  public void testLogTruncation() throws IOException {
    Configuration conf = setRetainSizes(1000L, 1000L);
    UserLogManager logManager = new UtilsForTests.InLineUserLogManager(conf);

    TaskID baseId = new TaskID();
    int taskcount = 0;

    TaskAttemptID attemptID = new TaskAttemptID(baseId, taskcount++);
    Task task = new MapTask(null, attemptID, 0, new JobSplit.TaskSplitIndex(), 
                            0);

    // Let the tasks write logs more than retain-size
    writeRealBytes(attemptID, attemptID, LogName.SYSLOG, 1500, 'H');
    writeRealBytes(attemptID, attemptID, LogName.STDERR, 500, 'H');

    File attemptDir = TaskLog.getAttemptDir(attemptID, false);
    assertTrue(attemptDir + " doesn't exist!", attemptDir.exists());

    // Finish the task and the JVM too.
    JVMInfo jvmInfo = new JVMInfo(attemptDir, Arrays.asList(task));
    logManager.addLogEvent(new JvmFinishedEvent(jvmInfo));

    // The log-file should now be truncated.
    assertTrue(attemptDir.exists());

    Map<LogName, Long> logLengths = getAllLogsFileLengths(attemptID, false);
    File logFile = TaskLog.getTaskLogFile(attemptID, false, LogName.SYSLOG);
    assertEquals(1000, logFile.length());
    // The index file should also be proper.
    assertEquals(1000, logLengths.get(LogName.SYSLOG).longValue());
    logFile = TaskLog.getTaskLogFile(attemptID, false, LogName.STDERR);
    assertEquals(500, logFile.length());
    // The index file should also be proper.
    assertEquals(500, logLengths.get(LogName.STDERR).longValue());

    // truncate once again
    logManager.addLogEvent(new JvmFinishedEvent(jvmInfo));
    logLengths = getAllLogsFileLengths(attemptID, false);
    logFile = TaskLog.getTaskLogFile(attemptID, false, LogName.SYSLOG);
    assertEquals(1000, logFile.length());
    // The index file should also be proper.
    assertEquals(1000, logLengths.get(LogName.SYSLOG).longValue());
    logFile = TaskLog.getTaskLogFile(attemptID, false, LogName.STDERR);
    assertEquals(500, logFile.length());
    // The index file should also be proper.
    assertEquals(500, logLengths.get(LogName.STDERR).longValue());
  }

  /**
   * Test the truncation of log-file when JVM-reuse is enabled.
   * 
   * @throws IOException
   */
  @Test
  public void testLogTruncationOnFinishingWithJVMReuse() throws IOException {
    Configuration conf = setRetainSizes(150L, 150L);
    UserLogManager logManager = new UtilsForTests.InLineUserLogManager(conf);

    TaskID baseTaskID = new TaskID();
    int attemptsCount = 0;

    // Assuming the job's retain size is 150
    TaskAttemptID attempt1 = new TaskAttemptID(baseTaskID, attemptsCount++);
    Task task1 = new MapTask(null, attempt1, 0, new JobSplit.TaskSplitIndex(),
                             0);

    // Let the tasks write logs more than retain-size
    writeRealBytes(attempt1, attempt1, LogName.SYSLOG, 200, 'A');

    File attemptDir = TaskLog.getAttemptDir(attempt1, false);
    assertTrue(attemptDir + " doesn't exist!", attemptDir.exists());

    // Start another attempt in the same JVM
    TaskAttemptID attempt2 = new TaskAttemptID(baseTaskID, attemptsCount++);
    Task task2 = new MapTask(null, attempt2, 0, new JobSplit.TaskSplitIndex(),
                             0);
    // Let attempt2 also write some logs
    writeRealBytes(attempt1, attempt2, LogName.SYSLOG, 100, 'B');
    // Start yet another attempt in the same JVM
    TaskAttemptID attempt3 = new TaskAttemptID(baseTaskID, attemptsCount++);
    Task task3 = new MapTask(null, attempt3, 0, new JobSplit.TaskSplitIndex(),
                             0);
    // Let attempt3 also write some logs
    writeRealBytes(attempt1, attempt3, LogName.SYSLOG, 225, 'C');
    // Finish the JVM.
    JVMInfo jvmInfo = new JVMInfo(attemptDir, 
        Arrays.asList((new Task[] { task1, task2, task3 })));
    logManager.addLogEvent(new JvmFinishedEvent(jvmInfo));

    // The log-file should now be truncated.
    assertTrue(attemptDir.exists());
    File logFile = TaskLog.getTaskLogFile(attempt1, false, LogName.SYSLOG);
    assertEquals(400, logFile.length());
    // The index files should also be proper.
    assertEquals(150, getAllLogsFileLengths(attempt1, false).get(
        LogName.SYSLOG).longValue());
    assertEquals(100, getAllLogsFileLengths(attempt2, false).get(
        LogName.SYSLOG).longValue());
    assertEquals(150, getAllLogsFileLengths(attempt3, false).get(
        LogName.SYSLOG).longValue());

    // assert the data.
    FileReader reader =
        new FileReader(TaskLog.getTaskLogFile(attempt1, false, LogName.SYSLOG));
    int ch, bytesRead = 0;
    boolean dataValid = true;
    while ((ch = reader.read()) != -1) {
      bytesRead++;
      if (bytesRead <= 150) {
        if ((char) ch != 'A') {
          LOG.warn("Truncation didn't happen properly. At "
              + (bytesRead + 1) + "th byte, expected 'A' but found "
              + (char) ch);
          dataValid = false;
        }
      } else if (bytesRead <= 250) {
        if ((char) ch != 'B') {
          LOG.warn("Truncation didn't happen properly. At "
              + (bytesRead + 1) + "th byte, expected 'B' but found "
              + (char) ch);
          dataValid = false;
        }
      } else if ((char) ch != 'C') {
        LOG.warn("Truncation didn't happen properly. At " + (bytesRead + 1)
            + "th byte, expected 'C' but found " + (char) ch);
        dataValid = false;
      }
    }
    assertTrue("Log-truncation didn't happen properly!", dataValid);

    logManager.addLogEvent(new JvmFinishedEvent(jvmInfo));
    assertEquals(400, logFile.length());
  }

  private static String TEST_ROOT_DIR =
      new File(System.getProperty("test.build.data", "/tmp")).toURI().toString().replace(
          ' ', '+');

  private static String STDERR_LOG = "stderr log";
  public static class LoggingMapper<K, V> extends IdentityMapper<K, V> {

    public void map(K key, V val, OutputCollector<K, V> output,
        Reporter reporter) throws IOException {
      // Write lots of logs
      for (int i = 0; i < 1000; i++) {
        System.out.println("Lots of logs! Lots of logs! "
            + "Waiting to be truncated! Lots of logs!");
      }
      // write some log into stderr
      System.err.println(STDERR_LOG);
      super.map(key, val, output, reporter);
    }
  }

  /**
   * Test logs monitoring with {@link MiniMRCluster}
   * 
   * @throws IOException
   */
  @Test
  public void testLogsMonitoringWithMiniMR() throws IOException {

    MiniMRCluster mr = null;
    try {
      JobConf clusterConf = new JobConf();
      clusterConf.setLong(TaskLogsTruncater.MAP_USERLOG_RETAIN_SIZE, 10000L);
      clusterConf.setLong(TaskLogsTruncater.REDUCE_USERLOG_RETAIN_SIZE, 10000L);
      mr = new MiniMRCluster(1, "file:///", 3, null, null, clusterConf);

      JobConf conf = mr.createJobConf();

      Path inDir = new Path(TEST_ROOT_DIR + "/input");
      Path outDir = new Path(TEST_ROOT_DIR + "/output");
      FileSystem fs = FileSystem.get(conf);
      if (fs.exists(outDir)) {
        fs.delete(outDir, true);
      }
      if (!fs.exists(inDir)) {
        fs.mkdirs(inDir);
      }
      String input = "The quick brown fox jumped over the lazy dog";
      DataOutputStream file = fs.create(new Path(inDir, "part-0"));
      file.writeBytes(input);
      file.close();

      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputKeyClass(LongWritable.class);
      conf.setOutputValueClass(Text.class);

      FileInputFormat.setInputPaths(conf, inDir);
      FileOutputFormat.setOutputPath(conf, outDir);
      conf.setNumMapTasks(1);
      conf.setNumReduceTasks(0);
      conf.setMapperClass(LoggingMapper.class);

      RunningJob job = JobClient.runJob(conf);
      assertTrue(job.getJobState() == JobStatus.SUCCEEDED);
      for (TaskCompletionEvent tce : job.getTaskCompletionEvents(0)) {
        long length =
            TaskLog.getTaskLogFile(tce.getTaskAttemptId(), false,
                TaskLog.LogName.STDOUT).length();
        assertTrue("STDOUT log file length for " + tce.getTaskAttemptId()
            + " is " + length + " and not <=10000", length <= 10000);
        if (tce.isMap) {
          String stderr = TestMiniMRMapRedDebugScript.readTaskLog(
              LogName.STDERR, tce.getTaskAttemptId(), false);
          System.out.println("STDERR log:" + stderr);
          assertTrue(stderr.length() > 0);
          assertTrue(stderr.length() < 10000);
          assertTrue(stderr.equals(STDERR_LOG));
        }
      }
    } finally {
      if (mr != null) {
        mr.shutdown();
      }
    }
  }

  /**
   * Test the truncation of DEBUGOUT file by {@link TaskLogsTruncater}
   * @throws IOException 
   */
  @Test
  public void testDebugLogsTruncationWithMiniMR() throws IOException {

    MiniMRCluster mr = null;
    try {
      JobConf clusterConf = new JobConf();
      clusterConf.setLong(TaskLogsTruncater.MAP_USERLOG_RETAIN_SIZE, 10000L);
      clusterConf.setLong(TaskLogsTruncater.REDUCE_USERLOG_RETAIN_SIZE, 10000L);
      mr = new MiniMRCluster(1, "file:///", 3, null, null, clusterConf);

      JobConf conf = mr.createJobConf();

      Path inDir = new Path(TEST_ROOT_DIR + "/input");
      Path outDir = new Path(TEST_ROOT_DIR + "/output");
      FileSystem fs = FileSystem.get(conf);
      if (fs.exists(outDir)) {
        fs.delete(outDir, true);
      }
      if (!fs.exists(inDir)) {
        fs.mkdirs(inDir);
      }
      String input = "The quick brown fox jumped over the lazy dog";
      DataOutputStream file = fs.create(new Path(inDir, "part-0"));
      file.writeBytes(input);
      file.close();

      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputKeyClass(LongWritable.class);
      conf.setOutputValueClass(Text.class);

      FileInputFormat.setInputPaths(conf, inDir);
      FileOutputFormat.setOutputPath(conf, outDir);
      conf.setNumMapTasks(1);
      conf.setMaxMapAttempts(1);
      conf.setNumReduceTasks(0);
      conf.setMapperClass(TestMiniMRMapRedDebugScript.MapClass.class);

      // copy debug script to cache from local file system.
      Path scriptPath = new Path(TEST_ROOT_DIR, "debug-script.txt");
      String debugScriptContent =
          "for ((i=0;i<1000;i++)); " + "do "
              + "echo \"Lots of logs! Lots of logs! "
              + "Waiting to be truncated! Lots of logs!\";" + "done";
      DataOutputStream scriptFile = fs.create(scriptPath);
      scriptFile.writeBytes(debugScriptContent);
      scriptFile.close();
      new File(scriptPath.toUri().getPath()).setExecutable(true);

      URI uri = scriptPath.toUri();
      DistributedCache.createSymlink(conf);
      DistributedCache.addCacheFile(uri, conf);
      conf.setMapDebugScript(scriptPath.toUri().getPath());

      RunningJob job = null;
      try {
        JobClient jc = new JobClient(conf);
        job = jc.submitJob(conf);
        try {
          jc.monitorAndPrintJob(conf, job);
        } catch (InterruptedException e) {
          //
        }
      } catch (IOException ioe) {
      } finally{
        for (TaskCompletionEvent tce : job.getTaskCompletionEvents(0)) {
          File debugOutFile =
              TaskLog.getTaskLogFile(tce.getTaskAttemptId(), false,
                  TaskLog.LogName.DEBUGOUT);
          if (debugOutFile.exists()) {
            long length = debugOutFile.length();
            assertTrue("DEBUGOUT log file length for "
                + tce.getTaskAttemptId() + " is " + length
                + " and not =10000", length == 10000);
          }
        }
      }
    } finally {
      if (mr != null) {
        mr.shutdown();
      }
    }
  }
}
