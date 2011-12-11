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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.apache.hadoop.mapreduce.tools.CLI;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.junit.Test;

public class TestMRJobClient extends ClusterMapReduceTestCase {
  
  private static final Log LOG = LogFactory.getLog(TestMRJobClient.class);
  
  private Job runJob(Configuration conf) throws Exception {
    String input = "hello1\nhello2\nhello3\n";

    Job job = MapReduceTestUtil.createJob(conf,
      getInputDir(), getOutputDir(), 1, 1, input);
    job.setJobName("mr");
    job.setPriority(JobPriority.HIGH);
    job.waitForCompletion(true);
    return job;
  }
  
  public static int runTool(Configuration conf, Tool tool,
      String[] args, OutputStream out) throws Exception {
    PrintStream oldOut = System.out;
    PrintStream newOut = new PrintStream(out, true);
    try {
      System.setOut(newOut);
      return ToolRunner.run(conf, tool, args);
    } finally {
      System.setOut(oldOut);
    }
  }

  @Test
  public void testJobClient() throws Exception {
    Configuration conf = createJobConf();
    Job job = runJob(conf);
    String jobId = job.getJobID().toString();
    testGetCounter(jobId, conf);
    testJobList(jobId, conf);
    testChangingJobPriority(jobId, conf);
  }

  @Test
  public void testGetCounter(String jobId,
      Configuration conf) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int exitCode = runTool(conf, createJobClient(),
        new String[] { "-counter", jobId,
        "org.apache.hadoop.mapreduce.TaskCounter", "MAP_INPUT_RECORDS" },
        out);
    assertEquals("Exit code", 0, exitCode);
    assertEquals("Counter", "3", out.toString().trim());
  }

  @Test
  public void testJobList(String jobId,
      Configuration conf) throws Exception {
    verifyJobPriority(jobId, "HIGH", conf, createJobClient());
  }

  protected void verifyJobPriority(String jobId, String priority,
      Configuration conf, CLI jc) throws Exception {
    PipedInputStream pis = new PipedInputStream();
    PipedOutputStream pos = new PipedOutputStream(pis);
    int exitCode = runTool(conf, jc,
        new String[] { "-list", "all" },
        pos);
    assertEquals("Exit code", 0, exitCode);
    BufferedReader br = new BufferedReader(new InputStreamReader(pis));
    String line = null;
    while ((line = br.readLine()) != null) {
      LOG.info("line = " + line);
      if (!line.startsWith(jobId)) {
        continue;
      }
      assertTrue(line.contains(priority));
      break;
    }
    pis.close();
  }

  @Test
  public void testChangingJobPriority(String jobId, Configuration conf)
      throws Exception {
    int exitCode = runTool(conf, createJobClient(),
        new String[] { "-set-priority", jobId, "VERY_LOW" },
        new ByteArrayOutputStream());
    assertEquals("Exit code", 0, exitCode);
    verifyJobPriority(jobId, "VERY_LOW", conf, createJobClient());
  }

  @Test
  public void testMissingProfileOutput() throws Exception {
    Configuration conf = createJobConf();
    final String input = "hello1\n";

    // Set a job to be profiled with an empty agentlib parameter.
    // This will fail to create profile.out files for tasks.
    // This will succeed by skipping the HTTP fetch of the
    // profiler output.
    Job job = MapReduceTestUtil.createJob(conf,
        getInputDir(), getOutputDir(), 1, 1, input);
    job.setJobName("disable-profile-fetch");
    job.setProfileEnabled(true);
    job.setProfileParams("-agentlib:,verbose=n,file=%s");
    job.setMaxMapAttempts(1);
    job.setMaxReduceAttempts(1);
    job.setJobSetupCleanupNeeded(false);
    job.waitForCompletion(true);

    // Run another job with an hprof agentlib param; verify
    // that the HTTP fetch works here.
    Job job2 = MapReduceTestUtil.createJob(conf,
        getInputDir(), getOutputDir(), 1, 1, input);
    job2.setJobName("enable-profile-fetch");
    job2.setProfileEnabled(true);
    job2.setProfileParams(
        "-agentlib:hprof=cpu=samples,heap=sites,force=n,"
        + "thread=y,verbose=n,file=%s");
    job2.setProfileTaskRange(true, "0-1");
    job2.setProfileTaskRange(false, "");
    job2.setMaxMapAttempts(1);
    job2.setMaxReduceAttempts(1);
    job2.setJobSetupCleanupNeeded(false);
    job2.waitForCompletion(true);

    // Find the first map task, verify that we got its profile output file.
    TaskReport [] reports = job2.getTaskReports(TaskType.MAP);
    assertTrue("No task reports found!", reports.length > 0);
    TaskReport report = reports[0];
    TaskID id = report.getTaskId();
    assertTrue(TaskType.MAP == id.getTaskType());
    System.out.println("Using task id: " + id);
    TaskAttemptID attemptId = new TaskAttemptID(id, 0);

    File profileOutFile = new File(attemptId.toString() + ".profile");
    assertTrue("Couldn't find profiler output", profileOutFile.exists());
    assertTrue("Couldn't remove profiler output", profileOutFile.delete());
  }

  protected CLI createJobClient() throws IOException {
    return new CLI();
  }

}
