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
package org.apache.hadoop.mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.tools.CLI;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 test CLI class. CLI class implemented  the Tool interface. 
 Here test that CLI sends correct command with options and parameters. 
 */
public class TestMRJobClient extends ClusterMapReduceTestCase {

  private static final Log LOG = LogFactory.getLog(TestMRJobClient.class);

  private Job runJob(Configuration conf) throws Exception {
    String input = "hello1\nhello2\nhello3\n";

    Job job = MapReduceTestUtil.createJob(conf, getInputDir(), getOutputDir(),
        1, 1, input);
    job.setJobName("mr");
    job.setPriority(JobPriority.NORMAL);
    job.waitForCompletion(true);
    return job;
  }

  private Job runJobInBackGround(Configuration conf) throws Exception {
    String input = "hello1\nhello2\nhello3\n";

    Job job = MapReduceTestUtil.createJob(conf, getInputDir(), getOutputDir(),
        1, 1, input);
    job.setJobName("mr");
    job.setPriority(JobPriority.NORMAL);
    job.submit();
    int i = 0;
    while (i++ < 200 && job.getJobID() == null) {
      LOG.info("waiting for jobId...");
      Thread.sleep(100);
    }
    return job;
  }

  public static int runTool(Configuration conf, Tool tool, String[] args,
      OutputStream out) throws Exception {
    LOG.info("args = " + Arrays.toString(args));
    PrintStream oldOut = System.out;
    PrintStream newOut = new PrintStream(out, true);
    try {
      System.setOut(newOut);
      return ToolRunner.run(conf, tool, args);
    } finally {
      System.setOut(oldOut);
    }
  }

  private static class BadOutputFormat extends TextOutputFormat<Object, Object> {
    @Override
    public void checkOutputSpecs(JobContext job) throws IOException {
      throw new IOException();
    }
  }
  @Test
  public void testJobSubmissionSpecsAndFiles() throws Exception {
    Configuration conf = createJobConf();
    Job job = MapReduceTestUtil.createJob(conf, getInputDir(), getOutputDir(),
        1, 1);
    job.setOutputFormatClass(BadOutputFormat.class);
    try {
      job.submit();
      fail("Should've thrown an exception while checking output specs.");
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
    }
    Cluster cluster = new Cluster(conf);
    Path jobStagingArea = JobSubmissionFiles.getStagingDir(cluster,
        job.getConfiguration());
    Path submitJobDir = new Path(jobStagingArea, "JobId");
    Path submitJobFile = JobSubmissionFiles.getJobConfPath(submitJobDir);
    assertFalse("Shouldn't have created a job file if job specs failed.",
        FileSystem.get(conf).exists(submitJobFile));
  }

  /**
   * main test method
   */
  @Test
  public void testJobClient() throws Exception {
    Configuration conf = createJobConf();
    Job job = runJob(conf);

    String jobId = job.getJobID().toString();
    // test all jobs list
    testAllJobList(jobId, conf);
    // test only submitted jobs list
    testSubmittedJobList(conf);
    // test job counter
    testGetCounter(jobId, conf);
    // status
    testJobStatus(jobId, conf);
    // test list of events
    testJobEvents(jobId, conf);
    // test job history
    testJobHistory(jobId, conf);
    // test tracker list
    testListTrackers(conf);
    // attempts list
    testListAttemptIds(jobId, conf);
    // black list
    testListBlackList(conf);
    // test method main and help screen
    startStop();
    // test a change job priority .
    testChangingJobPriority(jobId, conf);
    // submit job from file
    testSubmit(conf);
    // kill a task
    testKillTask(conf);
    // fail a task
    testfailTask(conf);
    // kill job
    testKillJob(conf);
    // download job config
    testConfig(jobId, conf);
  }

  /**
   * test fail task
   */
  private void testfailTask(Configuration conf) throws Exception {
    Job job = runJobInBackGround(conf);
    CLI jc = createJobClient();
    TaskID tid = new TaskID(job.getJobID(), TaskType.MAP, 0);
    TaskAttemptID taid = new TaskAttemptID(tid, 1);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // TaskAttemptId is not set
    int exitCode = runTool(conf, jc, new String[] { "-fail-task" }, out);
    assertEquals("Exit code", -1, exitCode);

    runTool(conf, jc, new String[] { "-fail-task", taid.toString() }, out);
    String answer = new String(out.toByteArray(), "UTF-8");
    assertTrue(answer.contains("Killed task " + taid + " by failing it"));
  }

  /**
   * test a kill task
   */ 
  private void testKillTask(Configuration conf) throws Exception {
    Job job = runJobInBackGround(conf);
    CLI jc = createJobClient();
    TaskID tid = new TaskID(job.getJobID(), TaskType.MAP, 0);
    TaskAttemptID taid = new TaskAttemptID(tid, 1);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // bad parameters
    int exitCode = runTool(conf, jc, new String[] { "-kill-task" }, out);
    assertEquals("Exit code", -1, exitCode);

    runTool(conf, jc, new String[] { "-kill-task", taid.toString() }, out);
    String answer = new String(out.toByteArray(), "UTF-8");
    assertTrue(answer.contains("Killed task " + taid));
  }
  
  /**
   * test a kill job
   */
  private void testKillJob(Configuration conf) throws Exception {
    Job job = runJobInBackGround(conf);
    String jobId = job.getJobID().toString();
    CLI jc = createJobClient();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // without jobId
    int exitCode = runTool(conf, jc, new String[] { "-kill" }, out);
    assertEquals("Exit code", -1, exitCode);
    // good parameters
    exitCode = runTool(conf, jc, new String[] { "-kill", jobId }, out);
    assertEquals("Exit code", 0, exitCode);
    
    String answer = new String(out.toByteArray(), "UTF-8");
    assertTrue(answer.contains("Killed job " + jobId));
  }

  /**
   * test submit task from file
   */
  private void testSubmit(Configuration conf) throws Exception {
    CLI jc = createJobClient();

    Job job = MapReduceTestUtil.createJob(conf, getInputDir(), getOutputDir(),
        1, 1, "ping");
    job.setJobName("mr");
    job.setPriority(JobPriority.NORMAL);

    File fcon = File.createTempFile("config", ".xml");
    FileSystem localFs = FileSystem.getLocal(conf);
    String fconUri = new Path(fcon.getAbsolutePath())
        .makeQualified(localFs.getUri(), localFs.getWorkingDirectory()).toUri()
        .toString();

    job.getConfiguration().writeXml(new FileOutputStream(fcon));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // bad parameters
    int exitCode = runTool(conf, jc, new String[] { "-submit" }, out);
    assertEquals("Exit code", -1, exitCode);
    
    
    exitCode = runTool(conf, jc,
        new String[] { "-submit", fconUri }, out);
    assertEquals("Exit code", 0, exitCode);
    String answer = new String(out.toByteArray());
    // in console was written
    assertTrue(answer.contains("Created job "));
  }
  /**
   * test start form console command without options
   */
  private void startStop() {
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    PrintStream error = System.err;
    System.setErr(new PrintStream(data));
    ExitUtil.disableSystemExit();
    try {
      CLI.main(new String[0]);
      fail(" CLI.main should call System.exit");

    } catch (ExitUtil.ExitException e) {
      ExitUtil.resetFirstExitException();
      assertEquals(-1, e.status);
    } catch (Exception e) {

    } finally {
      System.setErr(error);
    }
    // in console should be written help text 
    String s = new String(data.toByteArray());
    assertTrue(s.contains("-submit"));
    assertTrue(s.contains("-status"));
    assertTrue(s.contains("-kill"));
    assertTrue(s.contains("-set-priority"));
    assertTrue(s.contains("-events"));
    assertTrue(s.contains("-history"));
    assertTrue(s.contains("-list"));
    assertTrue(s.contains("-list-active-trackers"));
    assertTrue(s.contains("-list-blacklisted-trackers"));
    assertTrue(s.contains("-list-attempt-ids"));
    assertTrue(s.contains("-kill-task"));
    assertTrue(s.contains("-fail-task"));
    assertTrue(s.contains("-logs"));

  }
  /**
   * black list 
   */
  private void testListBlackList(Configuration conf) throws Exception {
    CLI jc = createJobClient();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int exitCode = runTool(conf, jc, new String[] {
        "-list-blacklisted-trackers", "second in" }, out);
    assertEquals("Exit code", -1, exitCode);
    exitCode = runTool(conf, jc, new String[] { "-list-blacklisted-trackers" },
        out);
    assertEquals("Exit code", 0, exitCode);
    String line;
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(out.toByteArray())));
    int counter = 0;
    while ((line = br.readLine()) != null) {
      LOG.info("line = " + line);
      counter++;
    }
    assertEquals(0, counter);
  }
  /**
   * print AttemptIds list 
   */
  private void testListAttemptIds(String jobId, Configuration conf)
      throws Exception {
    CLI jc = createJobClient();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int exitCode = runTool(conf, jc, new String[] { "-list-attempt-ids" }, out);
    assertEquals("Exit code", -1, exitCode);
    exitCode = runTool(conf, jc, new String[] { "-list-attempt-ids", jobId,
        "MAP", "completed" }, out);
    assertEquals("Exit code", 0, exitCode);
    String line;
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(out.toByteArray())));
    int counter = 0;
    while ((line = br.readLine()) != null) {
      LOG.info("line = " + line);
      counter++;
    }
    assertEquals(1, counter);
  }
  /**
   * print tracker list
   */
  private void testListTrackers(Configuration conf) throws Exception {
    CLI jc = createJobClient();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int exitCode = runTool(conf, jc, new String[] { "-list-active-trackers",
        "second parameter" }, out);
    assertEquals("Exit code", -1, exitCode);
    exitCode = runTool(conf, jc, new String[] { "-list-active-trackers" }, out);
    assertEquals("Exit code", 0, exitCode);
    String line;
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(out.toByteArray())));
    int counter = 0;
    while ((line = br.readLine()) != null) {
      LOG.info("line = " + line);
      counter++;
    }
    assertEquals(2, counter);
  }
  /**
   * print job history from file 
   */
  private void testJobHistory(String jobId, Configuration conf)
      throws Exception {
    CLI jc = createJobClient();
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    // Find jhist file
    String historyFileUri = null;
    RemoteIterator<LocatedFileStatus> it =
        getFileSystem().listFiles(new Path("/"), true);
    while (it.hasNext() && historyFileUri == null) {
      LocatedFileStatus file = it.next();
      if (file.getPath().getName().endsWith(".jhist")) {
        historyFileUri = file.getPath().toUri().toString();
      }
    }
    assertNotNull("Could not find jhist file", historyFileUri);

    for (String historyFileOrJobId : new String[]{historyFileUri, jobId}) {
      // Try a bunch of different valid combinations of the command
      int exitCode = runTool(conf, jc, new String[]{
          "-history",
          "all",
          historyFileOrJobId,
      }, out);
      assertEquals("Exit code", 0, exitCode);
      checkHistoryHumanOutput(jobId, out);
      File outFile = File.createTempFile("myout", ".txt");
      exitCode = runTool(conf, jc, new String[]{
          "-history",
          "all",
          historyFileOrJobId,
          "-outfile",
          outFile.getAbsolutePath()
      }, out);
      assertEquals("Exit code", 0, exitCode);
      checkHistoryHumanFileOutput(jobId, out, outFile);
      outFile = File.createTempFile("myout", ".txt");
      exitCode = runTool(conf, jc, new String[]{
          "-history",
          "all",
          historyFileOrJobId,
          "-outfile",
          outFile.getAbsolutePath(),
          "-format",
          "human"
      }, out);
      assertEquals("Exit code", 0, exitCode);
      checkHistoryHumanFileOutput(jobId, out, outFile);
      exitCode = runTool(conf, jc, new String[]{
          "-history",
          historyFileOrJobId,
          "-format",
          "human"
      }, out);
      assertEquals("Exit code", 0, exitCode);
      checkHistoryHumanOutput(jobId, out);
      exitCode = runTool(conf, jc, new String[]{
          "-history",
          "all",
          historyFileOrJobId,
          "-format",
          "json"
      }, out);
      assertEquals("Exit code", 0, exitCode);
      checkHistoryJSONOutput(jobId, out);
      outFile = File.createTempFile("myout", ".txt");
      exitCode = runTool(conf, jc, new String[]{
          "-history",
          "all",
          historyFileOrJobId,
          "-outfile",
          outFile.getAbsolutePath(),
          "-format",
          "json"
      }, out);
      assertEquals("Exit code", 0, exitCode);
      checkHistoryJSONFileOutput(jobId, out, outFile);
      exitCode = runTool(conf, jc, new String[]{
          "-history",
          historyFileOrJobId,
          "-format",
          "json"
      }, out);
      assertEquals("Exit code", 0, exitCode);
      checkHistoryJSONOutput(jobId, out);

      // Check some bad arguments
      exitCode = runTool(conf, jc, new String[]{
          "-history",
          historyFileOrJobId,
          "foo"
      }, out);
      assertEquals("Exit code", -1, exitCode);
      exitCode = runTool(conf, jc, new String[]{
          "-history",
          historyFileOrJobId,
          "-format"
      }, out);
      assertEquals("Exit code", -1, exitCode);
      exitCode = runTool(conf, jc, new String[]{
          "-history",
          historyFileOrJobId,
          "-outfile",
      }, out);
      assertEquals("Exit code", -1, exitCode);
      try {
        runTool(conf, jc, new String[]{
            "-history",
            historyFileOrJobId,
            "-format",
            "foo"
        }, out);
        fail();
      } catch (IllegalArgumentException e) {
        // Expected
      }
    }
    try {
      runTool(conf, jc, new String[]{
          "-history",
          "not_a_valid_history_file_or_job_id",
      }, out);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  private void checkHistoryHumanOutput(String jobId, ByteArrayOutputStream out)
      throws IOException, JSONException {
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(out.toByteArray())));
    br.readLine();
    String line = br.readLine();
    br.close();
    assertEquals("Hadoop job: " + jobId, line);
    out.reset();
  }

  private void checkHistoryJSONOutput(String jobId, ByteArrayOutputStream out)
      throws IOException, JSONException {
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(out.toByteArray())));
    String line = org.apache.commons.io.IOUtils.toString(br);
    br.close();
    JSONObject json = new JSONObject(line);
    assertEquals(jobId, json.getString("hadoopJob"));
    out.reset();
  }

  private void checkHistoryHumanFileOutput(String jobId,
      ByteArrayOutputStream out, File outFile)
      throws IOException, JSONException {
    BufferedReader br = new BufferedReader(new FileReader(outFile));
    br.readLine();
    String line = br.readLine();
    br.close();
    assertEquals("Hadoop job: " + jobId, line);
    assertEquals(0, out.size());
  }

  private void checkHistoryJSONFileOutput(String jobId,
      ByteArrayOutputStream out, File outFile)
      throws IOException, JSONException {
    BufferedReader br = new BufferedReader(new FileReader(outFile));
    String line = org.apache.commons.io.IOUtils.toString(br);
    br.close();
    JSONObject json = new JSONObject(line);
    assertEquals(jobId, json.getString("hadoopJob"));
    assertEquals(0, out.size());
  }

  /**
   * download job config
   */
  private void testConfig(String jobId, Configuration conf) throws Exception {
    CLI jc = createJobClient();
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    // bad arguments
    int exitCode = runTool(conf, jc, new String[] { "-config" }, out);
    assertEquals("Exit code", -1, exitCode);
    exitCode = runTool(conf, jc, new String[] { "-config job_invalid foo.xml" },
        out);
    assertEquals("Exit code", -1, exitCode);

    // good arguments
    File outFile = File.createTempFile("config", ".xml");
    exitCode = runTool(conf, jc, new String[] { "-config", jobId,
        outFile.toString()}, out);
    assertEquals("Exit code", 0, exitCode);
    BufferedReader br = new BufferedReader(new FileReader(outFile));
    String line = br.readLine();
    br.close();
    assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\" " +
        "standalone=\"no\"?><configuration>", line);
  }

  /**
   * print job events list 
   */
  private void testJobEvents(String jobId, Configuration conf) throws Exception {
    CLI jc = createJobClient();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int exitCode = runTool(conf, jc, new String[] { "-events" }, out);
    assertEquals("Exit code", -1, exitCode);

    exitCode = runTool(conf, jc, new String[] { "-events", jobId, "0", "100" },
        out);
    assertEquals("Exit code", 0, exitCode);
    String line;
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(out.toByteArray())));
    int counter = 0;
    String attemptId = ("attempt" + jobId.substring(3));
    while ((line = br.readLine()) != null) {
      LOG.info("line = " + line);
      if (line.contains(attemptId)) {
        counter++;
      }
    }
    assertEquals(2, counter);
  }
  /**
   * print job status 
   */
  private void testJobStatus(String jobId, Configuration conf) throws Exception {
    CLI jc = createJobClient();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // bad options
    int exitCode = runTool(conf, jc, new String[] { "-status" }, out);
    assertEquals("Exit code", -1, exitCode);

    exitCode = runTool(conf, jc, new String[] { "-status", jobId }, out);
    assertEquals("Exit code", 0, exitCode);
    String line;
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(out.toByteArray())));

    while ((line = br.readLine()) != null) {
      LOG.info("line = " + line);
      if (!line.contains("Job state:")) {
        continue;
      }
      break;
    }
    assertNotNull(line);
    assertTrue(line.contains("SUCCEEDED"));
  }
  /**
   * print counters 
   */
  public void testGetCounter(String jobId, Configuration conf) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // bad command 
    int exitCode = runTool(conf, createJobClient(),
        new String[] { "-counter", }, out);
    assertEquals("Exit code", -1, exitCode);
    
    exitCode = runTool(conf, createJobClient(),
        new String[] { "-counter", jobId,
            "org.apache.hadoop.mapreduce.TaskCounter", "MAP_INPUT_RECORDS" },
        out);
    assertEquals("Exit code", 0, exitCode);
    assertEquals("Counter", "3", out.toString().trim());
  }
  /**
   * print a job list 
   */
  protected void testAllJobList(String jobId, Configuration conf)
      throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // bad options

    int exitCode = runTool(conf, createJobClient(), new String[] { "-list",
        "alldata" }, out);
    assertEquals("Exit code", -1, exitCode);
    exitCode = runTool(conf, createJobClient(),
        // all jobs
        new String[] { "-list", "all" }, out);
    assertEquals("Exit code", 0, exitCode);
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(out.toByteArray())));
    String line;
    int counter = 0;
    while ((line = br.readLine()) != null) {
      LOG.info("line = " + line);
      if (line.contains(jobId)) {
        counter++;
      }
    }
    assertEquals(1, counter);
    out.reset();
  }

  protected void testSubmittedJobList(Configuration conf) throws Exception {
    Job job = runJobInBackGround(conf);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    String line;
    int counter = 0;
    // only submitted
    int exitCode =
        runTool(conf, createJobClient(), new String[] { "-list" }, out);
    assertEquals("Exit code", 0, exitCode);
    BufferedReader br =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(
          out.toByteArray())));
    counter = 0;
    while ((line = br.readLine()) != null) {
      LOG.info("line = " + line);
      if (line.contains(job.getJobID().toString())) {
        counter++;
      }
    }
    // all jobs submitted! no current
    assertEquals(1, counter);
  }

  protected void verifyJobPriority(String jobId, String priority,
      Configuration conf, CLI jc) throws Exception {
    PipedInputStream pis = new PipedInputStream();
    PipedOutputStream pos = new PipedOutputStream(pis);
    int exitCode = runTool(conf, jc, new String[] { "-list", "all" }, pos);
    assertEquals("Exit code", 0, exitCode);
    BufferedReader br = new BufferedReader(new InputStreamReader(pis));
    String line;
    while ((line = br.readLine()) != null) {
      LOG.info("line = " + line);
      if (!line.contains(jobId)) {
        continue;
      }
      assertTrue(line.contains(priority));
      break;
    }
    pis.close();
  }

  public void testChangingJobPriority(String jobId, Configuration conf)
      throws Exception {
    int exitCode = runTool(conf, createJobClient(),
        new String[] { "-set-priority" }, new ByteArrayOutputStream());
    assertEquals("Exit code", -1, exitCode);
    exitCode = runTool(conf, createJobClient(), new String[] { "-set-priority",
        jobId, "VERY_LOW" }, new ByteArrayOutputStream());
    assertEquals("Exit code", 0, exitCode);
    // set-priority is fired after job is completed in YARN, hence need not
    // have to update the priority.
    verifyJobPriority(jobId, "DEFAULT", conf, createJobClient());
  }

  /**
   * Test -list option displays job name.
   * The name is capped to 20 characters for display.
   */
  @Test
  public void testJobName() throws Exception {
    Configuration conf = createJobConf();
    CLI jc = createJobClient();
    Job job = MapReduceTestUtil.createJob(conf, getInputDir(), getOutputDir(),
        1, 1, "short_name");
    job.setJobName("mapreduce");
    job.setPriority(JobPriority.NORMAL);
    job.waitForCompletion(true);
    String jobId = job.getJobID().toString();
    verifyJobName(jobId, "mapreduce", conf, jc);
    Job job2 = MapReduceTestUtil.createJob(conf, getInputDir(), getOutputDir(),
        1, 1, "long_name");
    job2.setJobName("mapreduce_job_with_long_name");
    job2.setPriority(JobPriority.NORMAL);
    job2.waitForCompletion(true);
    jobId = job2.getJobID().toString();
    verifyJobName(jobId, "mapreduce_job_with_l", conf, jc);
  }

  protected void verifyJobName(String jobId, String name,
      Configuration conf, CLI jc) throws Exception {
    PipedInputStream pis = new PipedInputStream();
    PipedOutputStream pos = new PipedOutputStream(pis);
    int exitCode = runTool(conf, jc,
        new String[] { "-list", "all" }, pos);
    assertEquals("Exit code", 0, exitCode);
    BufferedReader br = new BufferedReader(new InputStreamReader(pis));
    String line = null;
    while ((line = br.readLine()) != null) {
      LOG.info("line = " + line);
      if (!line.contains(jobId)) {
        continue;
      }
      assertTrue(line.contains(name));
      break;
    }
    pis.close();
  }

  protected CLI createJobClient() throws IOException {
    return new CLI();
  }

}
