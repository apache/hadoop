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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mapreduce.jobhistory.JobHistory;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TestNoJobSetupCleanup.MyOutputFormat;

import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.ProgressSplitsBlock;

import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;

import org.apache.hadoop.mapreduce.jobhistory.JobHistory.JobHistoryRecordRetriever;

import org.apache.hadoop.tools.rumen.TraceBuilder;
import org.apache.hadoop.tools.rumen.ZombieJobProducer;
import org.apache.hadoop.tools.rumen.ZombieJob;
import org.apache.hadoop.tools.rumen.LoggedJob;
import org.apache.hadoop.tools.rumen.LoggedTask;
import org.apache.hadoop.tools.rumen.LoggedTaskAttempt;
import org.apache.hadoop.tools.rumen.TaskAttemptInfo;
import org.apache.hadoop.tools.rumen.ZombieCluster;
import org.apache.hadoop.tools.rumen.MachineNode;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestTaskPerformanceSplitTranscription {
  // This testcase runs a job in a mini cluster, and then it verifies
  //  that splits are stored in the resulting trace, and also
  //  retrievable from the ZombieJob resulting from reading the trace.
  // 
  // We can't test for any particular values, unfortunately.
  @Test
  public void testTranscription() throws Exception {
    final Configuration conf = new Configuration();
    final FileSystem lfs = FileSystem.getLocal(conf);

    final Path rootTempDir =
      new Path(System.getProperty("test.build.data", "/tmp")).makeQualified(
          lfs.getUri(), lfs.getWorkingDirectory());

    final Path tempDir = new Path(rootTempDir, "testTranscription");
    lfs.delete(tempDir, true);
    
    // Run a MR job
    // create a MR cluster
    conf.setInt(TTConfig.TT_MAP_SLOTS, 1);
    conf.setInt(TTConfig.TT_REDUCE_SLOTS, 1);
    final MiniMRCluster mrCluster
      = new MiniMRCluster(1, "file:///", 1, null, null, 
                          new JobConf(conf));

    final JobTracker tracker = mrCluster.getJobTrackerRunner().getJobTracker();
    final JobHistory history = tracker.getJobHistory();
    
    // run a job
    Path inDir = new Path(tempDir, "input");
    Path outDir = new Path(tempDir, "output");

    ZombieJobProducer story = null;

    boolean success = false;
    
    try {
      JobConf jConf = mrCluster.createJobConf();
      // disable uberization (MR-1220) until JobHistory supports uber-specific
      // events with complete split info (for Rumen network topology)
      jConf.setBoolean(JobContext.JOB_UBERTASK_ENABLE, false);
      // construct a job with 1 map and 1 reduce task.
      Job job = MapReduceTestUtil.createJob(jConf, inDir, outDir, 1, 1);
      // disable setup/cleanup
      job.setJobSetupCleanupNeeded(false);
      // set the output format to take care of the _temporary folder
      job.setOutputFormatClass(MyOutputFormat.class);
      // wait for the job to complete
      job.waitForCompletion(false);
      
      assertTrue("Job failed", job.isSuccessful());

      JobID id = job.getJobID();

      Path inputPath = null;
      // wait for 10 secs for the jobhistory file to move into the done folder
      for (int i = 0; i < 100; ++i) {
        JobHistoryRecordRetriever retriever 
          = history.getMatchingJobs(null, "", null, id.toString());
        if (retriever.hasNext()) {
          inputPath = retriever.next().getPath();
          System.out.println("History file = " + inputPath);
          break;
        }
        Thread.currentThread().sleep(100);
      }
    
      assertTrue("Missing job history file", lfs.exists(inputPath));

      System.out.println("testTranscription() input history file is "
                         + inputPath.toString());

      final Path topologyPath = new Path(tempDir, "dispatch-topology.json");
      final Path tracePath = new Path(tempDir, "dispatch-trace.json");

      System.err.println("testTranscription() output .json file is "
                         + tracePath.toString());

      String[] args =
        { tracePath.toString(), topologyPath.toString(), inputPath.toString() };

      Tool analyzer = new TraceBuilder();
      int result = ToolRunner.run(analyzer, args);
      assertEquals("Non-zero exit", 0, result);

      MachineNode.Builder builder = new MachineNode.Builder("node.megacorp.com", 0);
      MachineNode node = builder.build();
      ZombieCluster cluster = new ZombieCluster(topologyPath, node, jConf);

      story = new ZombieJobProducer(tracePath, cluster, jConf);

      // test that the logged* has everything down to the split vector

      ZombieJob theZombieJob = story.getNextJob();
      LoggedJob theJob = theZombieJob.getLoggedJob();
      LoggedTask firstMapTask = theJob.getMapTasks().get(0);
      LoggedTaskAttempt firstAttempt = firstMapTask.getAttempts().get(0);

      assertTrue("No clock splits were stored",
                 firstAttempt.getClockSplits().size() > 0);

      TaskAttemptInfo attemptInfo
        = theZombieJob.getTaskAttemptInfo(TaskType.MAP, 0, 0);

      assertEquals("Can't retrieve clock splits from the LoggedTaskAttempt",
                   attemptInfo.getSplitVector
                         (LoggedTaskAttempt.SplitVectorKind.WALLCLOCK_TIME)
                      .size(),
                    ProgressSplitsBlock.DEFAULT_NUMBER_PROGRESS_SPLITS);

      // test that the ZombieJob can deliver splits vectors

      TaskAttemptInfo tinfo = theZombieJob.getTaskAttemptInfo(TaskType.MAP, 0, 0);
      List<Integer> splitVector
        = tinfo.getSplitVector(LoggedTaskAttempt.SplitVectorKind.WALLCLOCK_TIME);

      assertEquals("Can't retrieve clock splits from the ZombieJob",
                   splitVector.size(), ProgressSplitsBlock.DEFAULT_NUMBER_PROGRESS_SPLITS);
      
      success = true;
    } finally {
      if (success) {
        lfs.delete(tempDir, true);
      }

      if (story != null) {
        story.close();
      }
    }
  }
}
