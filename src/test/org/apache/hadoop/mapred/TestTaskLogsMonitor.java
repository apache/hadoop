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

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TaskLog.LogFileDetail;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.mapred.lib.IdentityMapper;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Verify the logs' monitoring functionality.
 */
public class TestTaskLogsMonitor {

  static final Log LOG = LogFactory.getLog(TestTaskLogsMonitor.class);

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

    File logFile = TaskLog.getTaskLogFile(firstAttemptID, logName);

    LOG.info("Going to write " + numBytes + " real bytes to the log file "
        + logFile);

    if (!logFile.getParentFile().exists()
        && !logFile.getParentFile().mkdirs()) {
      throw new IOException("Couldn't create all ancestor dirs for "
          + logFile);
    }

    File attemptDir = TaskLog.getBaseDir(attemptID.toString());
    if (!attemptDir.exists() && !attemptDir.mkdirs()) {
      throw new IOException("Couldn't create all ancestor dirs for "
          + logFile);
    }

    // Need to call up front to set currenttaskid.
    TaskLog.syncLogs(firstAttemptID, attemptID);

    FileWriter writer = new FileWriter(logFile, true);
    for (long i = 0; i < numBytes; i++) {
      writer.write(data);
    }
    writer.close();
    TaskLog.syncLogs(firstAttemptID, attemptID);
    LOG.info("Written " + numBytes + " real bytes to the log file "
        + logFile);
  }

  private static Map<LogName, Long> getAllLogsFileLengths(
      TaskAttemptID tid, boolean isCleanup) throws IOException {
    Map<LogName, Long> allLogsFileLengths = new HashMap<LogName, Long>();

    // If the index file doesn't exist, we cannot get log-file lengths. So set
    // them to zero.
    if (!TaskLog.getIndexFile(tid.toString(), isCleanup).exists()) {
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

  /**
   * Test cases which don't need any truncation of log-files. Without JVM-reuse.
   * 
   * @throws IOException
   */
  @Test
  public void testNoTruncationNeeded() throws IOException {
    TaskTracker taskTracker = new TaskTracker();
    TaskLogsMonitor logsMonitor = new TaskLogsMonitor(1000L, 1000L);
    taskTracker.setTaskLogsMonitor(logsMonitor);

    TaskID baseId = new TaskID();
    int taskcount = 0;

    TaskAttemptID attemptID = new TaskAttemptID(baseId, taskcount++);
    Task task = new MapTask(null, attemptID, 0, null, null, 0, null);

    // Let the tasks write logs within retain-size
    writeRealBytes(attemptID, attemptID, LogName.SYSLOG, 500, 'H');

    logsMonitor.monitorTaskLogs();
    File attemptDir = TaskLog.getBaseDir(attemptID.toString());
    assertTrue(attemptDir + " doesn't exist!", attemptDir.exists());

    // Finish the task and the JVM too.
    logsMonitor.addProcessForLogTruncation(attemptID, Arrays.asList(task));

    // There should be no truncation of the log-file.
    logsMonitor.monitorTaskLogs();
    assertTrue(attemptDir.exists());
    File logFile = TaskLog.getTaskLogFile(attemptID, LogName.SYSLOG);
    assertEquals(500, logFile.length());
    // The index file should also be proper.
    assertEquals(500, getAllLogsFileLengths(attemptID, false).get(
        LogName.SYSLOG).longValue());

    logsMonitor.monitorTaskLogs();
    assertEquals(500, logFile.length());
  }

  /**
   * Test the disabling of truncation of log-file.
   * 
   * @throws IOException
   */
  @Test
  public void testDisabledLogTruncation() throws IOException {
    TaskTracker taskTracker = new TaskTracker();
    // Anything less than 0 disables the truncation.
    TaskLogsMonitor logsMonitor = new TaskLogsMonitor(-1L, -1L);
    taskTracker.setTaskLogsMonitor(logsMonitor);

    TaskID baseId = new TaskID();
    int taskcount = 0;

    TaskAttemptID attemptID = new TaskAttemptID(baseId, taskcount++);
    Task task = new MapTask(null, attemptID, 0, null, null, 0, null);

    // Let the tasks write some logs
    writeRealBytes(attemptID, attemptID, LogName.SYSLOG, 1500, 'H');

    logsMonitor.monitorTaskLogs();
    File attemptDir = TaskLog.getBaseDir(attemptID.toString());
    assertTrue(attemptDir + " doesn't exist!", attemptDir.exists());

    // Finish the task and the JVM too.
    logsMonitor.addProcessForLogTruncation(attemptID, Arrays.asList(task));

    // The log-file should not be truncated.
    logsMonitor.monitorTaskLogs();
    assertTrue(attemptDir.exists());
    File logFile = TaskLog.getTaskLogFile(attemptID, LogName.SYSLOG);
    assertEquals(1500, logFile.length());
    // The index file should also be proper.
    assertEquals(1500, getAllLogsFileLengths(attemptID, false).get(
        LogName.SYSLOG).longValue());
  }

  /**
   * Test the truncation of log-file when JVMs are not reused.
   * 
   * @throws IOException
   */
  @Test
  public void testLogTruncationOnFinishing() throws IOException {
    TaskTracker taskTracker = new TaskTracker();
    TaskLogsMonitor logsMonitor = new TaskLogsMonitor(1000L, 1000L);
    taskTracker.setTaskLogsMonitor(logsMonitor);

    TaskID baseId = new TaskID();
    int taskcount = 0;

    TaskAttemptID attemptID = new TaskAttemptID(baseId, taskcount++);
    Task task = new MapTask(null, attemptID, 0, null, null, 0, null);

    // Let the tasks write logs more than retain-size
    writeRealBytes(attemptID, attemptID, LogName.SYSLOG, 1500, 'H');

    logsMonitor.monitorTaskLogs();
    File attemptDir = TaskLog.getBaseDir(attemptID.toString());
    assertTrue(attemptDir + " doesn't exist!", attemptDir.exists());

    // Finish the task and the JVM too.
    logsMonitor.addProcessForLogTruncation(attemptID, Arrays.asList(task));

    // The log-file should now be truncated.
    logsMonitor.monitorTaskLogs();
    assertTrue(attemptDir.exists());
    File logFile = TaskLog.getTaskLogFile(attemptID, LogName.SYSLOG);
    assertEquals(1000, logFile.length());
    // The index file should also be proper.
    assertEquals(1000, getAllLogsFileLengths(attemptID, false).get(
        LogName.SYSLOG).longValue());

    logsMonitor.monitorTaskLogs();
    assertEquals(1000, logFile.length());
  }

  /**
   * Test the truncation of log-file when JVM-reuse is enabled.
   * 
   * @throws IOException
   */
  @Test
  public void testLogTruncationOnFinishingWithJVMReuse() throws IOException {
    TaskTracker taskTracker = new TaskTracker();
    TaskLogsMonitor logsMonitor = new TaskLogsMonitor(150L, 150L);
    taskTracker.setTaskLogsMonitor(logsMonitor);

    TaskID baseTaskID = new TaskID();
    int attemptsCount = 0;

    // Assuming the job's retain size is 150
    TaskAttemptID attempt1 = new TaskAttemptID(baseTaskID, attemptsCount++);
    Task task1 = new MapTask(null, attempt1, 0, null, null, 0, null);

    // Let the tasks write logs more than retain-size
    writeRealBytes(attempt1, attempt1, LogName.SYSLOG, 200, 'A');

    logsMonitor.monitorTaskLogs();

    File attemptDir = TaskLog.getBaseDir(attempt1.toString());
    assertTrue(attemptDir + " doesn't exist!", attemptDir.exists());

    // Start another attempt in the same JVM
    TaskAttemptID attempt2 = new TaskAttemptID(baseTaskID, attemptsCount++);
    Task task2 = new MapTask(null, attempt2, 0, null, null, 0, null);
    logsMonitor.monitorTaskLogs();

    // Let attempt2 also write some logs
    writeRealBytes(attempt1, attempt2, LogName.SYSLOG, 100, 'B');
    logsMonitor.monitorTaskLogs();

    // Start yet another attempt in the same JVM
    TaskAttemptID attempt3 = new TaskAttemptID(baseTaskID, attemptsCount++);
    Task task3 = new MapTask(null, attempt3, 0, null, null, 0, null);
    logsMonitor.monitorTaskLogs();

    // Let attempt3 also write some logs
    writeRealBytes(attempt1, attempt3, LogName.SYSLOG, 225, 'C');
    logsMonitor.monitorTaskLogs();

    // Finish the JVM.
    logsMonitor.addProcessForLogTruncation(attempt1,
        Arrays.asList((new Task[] { task1, task2, task3 })));

    // The log-file should now be truncated.
    logsMonitor.monitorTaskLogs();
    assertTrue(attemptDir.exists());
    File logFile = TaskLog.getTaskLogFile(attempt1, LogName.SYSLOG);
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
        new FileReader(TaskLog.getTaskLogFile(attempt1, LogName.SYSLOG));
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

    logsMonitor.monitorTaskLogs();
    assertEquals(400, logFile.length());
  }

  private static String TEST_ROOT_DIR =
      new File(System.getProperty("test.build.data", "/tmp")).toURI().toString().replace(
          ' ', '+');

  public static class LoggingMapper<K, V> extends IdentityMapper<K, V> {

    public void map(K key, V val, OutputCollector<K, V> output,
        Reporter reporter) throws IOException {
      // Write lots of logs
      for (int i = 0; i < 1000; i++) {
        System.out.println("Lots of logs! Lots of logs! "
            + "Waiting to be truncated! Lots of logs!");
      }
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
      clusterConf.setLong(TaskTracker.MAP_USERLOG_RETAIN_SIZE, 10000L);
      clusterConf.setLong(TaskTracker.REDUCE_USERLOG_RETAIN_SIZE, 10000L);
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
            TaskLog.getTaskLogFile(tce.getTaskAttemptId(),
                TaskLog.LogName.STDOUT).length();
        assertTrue("STDOUT log file length for " + tce.getTaskAttemptId()
            + " is " + length + " and not <=10000", length <= 10000);
      }
    } finally {
      if (mr != null) {
        mr.shutdown();
      }
    }
  }

  /**
   * Test the truncation of DEBUGOUT file by {@link TaskLogsMonitor}
   * @throws IOException 
   */
  @Test
  public void testDebugLogsTruncationWithMiniMR() throws IOException {

    MiniMRCluster mr = null;
    try {
      JobConf clusterConf = new JobConf();
      clusterConf.setLong(TaskTracker.MAP_USERLOG_RETAIN_SIZE, 10000L);
      clusterConf.setLong(TaskTracker.REDUCE_USERLOG_RETAIN_SIZE, 10000L);
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
              TaskLog.getTaskLogFile(tce.getTaskAttemptId(),
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
