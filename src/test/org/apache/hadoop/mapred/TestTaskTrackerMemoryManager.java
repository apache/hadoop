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

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.util.ProcfsBasedProcessTree;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import junit.framework.TestCase;

public class TestTaskTrackerMemoryManager extends TestCase {

  private static final Log LOG = LogFactory.getLog(TestTaskTrackerMemoryManager.class);
  private MiniDFSCluster miniDFSCluster;
  private MiniMRCluster miniMRCluster;

  private void startCluster(JobConf conf) throws Exception {
    miniDFSCluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fileSys = miniDFSCluster.getFileSystem();
    String namenode = fileSys.getUri().toString();
    miniMRCluster = new MiniMRCluster(1, namenode, 1, null, null, conf);
  }

  @Override
  protected void tearDown() {
    if (miniMRCluster != null) {
      miniMRCluster.shutdown();
    }
    if (miniDFSCluster != null) {
      miniDFSCluster.shutdown();
    }
  }

  private void runWordCount(JobConf conf) throws Exception {
    Path input = new Path("input.txt");
    Path output = new Path("output");

    OutputStream os = miniDFSCluster.getFileSystem().create(input);
    Writer wr = new OutputStreamWriter(os);
    wr.write("hello1\n");
    wr.write("hello2\n");
    wr.write("hello3\n");
    wr.write("hello4\n");
    wr.close();

    Tool WordCount = new WordCount();
    if (conf != null) {
      WordCount.setConf(conf);
    }
    ToolRunner.run(WordCount, new String[] { input.toString(),
        output.toString() });
  }

  public void testNormalTaskAndLimitedTT() throws Exception {
    // Run the test only if memory management is enabled

    try {
      if (!ProcfsBasedProcessTree.isAvailable()) {
        LOG.info("Currently ProcessTree has only one implementation "
            + "ProcfsBasedProcessTree, which is not available on this "
            + "system. Not testing");
        return;
      }
    } catch (Exception e) {
      LOG.info(StringUtils.stringifyException(e));
      return;
    }

    Pattern diagMsgPattern = Pattern
        .compile("TaskTree \\[pid=[0-9]*,tipID=.*\\] is running beyond "
            + "memory-limits. Current usage : [0-9]*kB. Limit : [0-9]*kB. Killing task.");
    Matcher mat = null;

    // Start cluster with proper configuration.
    JobConf fConf = new JobConf();

    fConf.setLong("mapred.tasktracker.tasks.maxmemory", 
                      Long.valueOf(10000000000L)); // Fairly large value for WordCount to succeed
    startCluster(fConf);

    // Set up job.
    JobConf conf = new JobConf();
    JobTracker jt = miniMRCluster.getJobTrackerRunner().getJobTracker();
    conf.set("mapred.job.tracker", jt.getJobTrackerMachine() + ":"
        + jt.getTrackerPort());
    NameNode nn = miniDFSCluster.getNameNode();
    conf.set("fs.default.name", "hdfs://"
        + nn.getNameNodeAddress().getHostName() + ":"
        + nn.getNameNodeAddress().getPort());

    // Start the job.
    boolean success = true;
    try {
      runWordCount(conf);
      success = true;
    } catch (Exception e) {
      success = false;
    }

    // Job has to succeed
    assertTrue(success);

    // Alas, we don't have a way to get job id/Task completion events from
    // WordCount
    JobClient jClient = new JobClient(conf);
    JobStatus[] jStatus = jClient.getAllJobs();
    JobStatus js = jStatus[0]; // Our only job
    RunningJob rj = jClient.getJob(js.getJobID());

    // All events
    TaskCompletionEvent[] taskComplEvents = rj.getTaskCompletionEvents(0);

    for (TaskCompletionEvent tce : taskComplEvents) {
      String[] diagnostics = jClient.jobSubmitClient.getTaskDiagnostics(tce
          .getTaskAttemptId());

      if (diagnostics != null) {
        for (String str : diagnostics) {
          mat = diagMsgPattern.matcher(str);
          // The error pattern shouldn't be there in any TIP's diagnostics
          assertFalse(mat.find());
        }
      }
    }
  }

  public void testOOMTaskAndLimitedTT() throws Exception {

    // Run the test only if memory management is enabled

    try {
      if (!ProcfsBasedProcessTree.isAvailable()) {
        LOG.info("Currently ProcessTree has only one implementation "
            + "ProcfsBasedProcessTree, which is not available on this "
            + "system. Not testing");
        return;
      }
    } catch (Exception e) {
      LOG.info(StringUtils.stringifyException(e));
      return;
    }

    long PER_TASK_LIMIT = 444; // Enough to kill off WordCount.
    Pattern diagMsgPattern = Pattern
        .compile("TaskTree \\[pid=[0-9]*,tipID=.*\\] is running beyond "
            + "memory-limits. Current usage : [0-9]*kB. Limit : "
            + PER_TASK_LIMIT + "kB. Killing task.");
    Matcher mat = null;

    // Start cluster with proper configuration.
    JobConf fConf = new JobConf();
    fConf.setLong("mapred.tasktracker.tasks.maxmemory", Long.valueOf(100000));
    fConf.set("mapred.tasktracker.taskmemorymanager.monitoring-interval", String.valueOf(300));
            //very small value, so that no task escapes to successful completion.
    startCluster(fConf);

    // Set up job.
    JobConf conf = new JobConf();
    conf.setMaxVirtualMemoryForTask(PER_TASK_LIMIT);
    JobTracker jt = miniMRCluster.getJobTrackerRunner().getJobTracker();
    conf.set("mapred.job.tracker", jt.getJobTrackerMachine() + ":"
        + jt.getTrackerPort());
    NameNode nn = miniDFSCluster.getNameNode();
    conf.set("fs.default.name", "hdfs://"
        + nn.getNameNodeAddress().getHostName() + ":"
        + nn.getNameNodeAddress().getPort());

    // Start the job.
    boolean success = true;
    try {
      runWordCount(conf);
      success = true;
    } catch (Exception e) {
      success = false;
    }

    // Job has to fail
    assertFalse(success);

    // Alas, we don't have a way to get job id/Task completion events from
    // WordCount
    JobClient jClient = new JobClient(conf);
    JobStatus[] jStatus = jClient.getAllJobs();
    JobStatus js = jStatus[0]; // Our only job
    RunningJob rj = jClient.getJob(js.getJobID());

    // All events
    TaskCompletionEvent[] taskComplEvents = rj.getTaskCompletionEvents(0);

    for (TaskCompletionEvent tce : taskComplEvents) {
      // Every task HAS to fail
      assert (tce.getTaskStatus() == TaskCompletionEvent.Status.TIPFAILED || tce
          .getTaskStatus() == TaskCompletionEvent.Status.FAILED);

      String[] diagnostics = jClient.jobSubmitClient.getTaskDiagnostics(tce
          .getTaskAttemptId());

      // Every task HAS to spit out the out-of-memory errors
      assert (diagnostics != null);

      for (String str : diagnostics) {
        mat = diagMsgPattern.matcher(str);
        // Every task HAS to spit out the out-of-memory errors in the same
        // format. And these are the only diagnostic messages.
        assertTrue(mat.find());
      }
    }
  }
}
