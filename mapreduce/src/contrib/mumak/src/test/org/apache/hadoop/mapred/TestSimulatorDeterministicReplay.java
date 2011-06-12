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

import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.util.ToolRunner;

import org.junit.Test;

public class TestSimulatorDeterministicReplay {

  public static final Log LOG = LogFactory.getLog(
      TestSimulatorDeterministicReplay.class);
  protected SimulatorJobSubmissionPolicy policy = SimulatorJobSubmissionPolicy.REPLAY;
  
  @Test
  public void testMain() throws Exception {
    Path hadoopLogDir = new Path(
        System.getProperty("test.build.data"), "mumak-replay");
    Path hadoopLogDir1 = new Path(hadoopLogDir, "run1");
    Path hadoopLogDir2 = new Path(hadoopLogDir, "run2");
    runMumak(hadoopLogDir1, 50031);
    LOG.info("Run1 done");
    runMumak(hadoopLogDir2, 50032);
    compareLogDirs(hadoopLogDir1.toString(), hadoopLogDir2.toString());
  }
  
  void compareLogDirs(String dir1, String dir2) {
    try {
      Runtime runtime = Runtime.getRuntime();
      Process process = runtime.exec("diff -r /dev/null /dev/null");
      process.waitFor();
      // If there is no diff available, we skip the test and end up in 
      // the catch block
      // Make sure diff understands the -r option
      if (process.exitValue() != 0) {
        LOG.warn("diff -r is not working, skipping the test");
        return;
      }
      // Run the real comparison
      process = runtime.exec("diff -r " + dir1 + " " + dir2);
      process.waitFor();
      Assert.assertEquals("Job history logs differ, diff returned", 
                          0, process.exitValue());
    } catch (Exception e) {
      LOG.warn("Exception while diffing: " + e);
    }                        
  }
  
  // We need a different http port parameter for each run as the socket
  // is not closed properly in hadoop
  void runMumak(Path hadoopLogDir, int jobTrackerHttpPort) 
      throws Exception {
    final Configuration conf = new Configuration();
    conf.set(SimulatorJobSubmissionPolicy.JOB_SUBMISSION_POLICY, policy.name());
    final FileSystem lfs = FileSystem.getLocal(conf);
    final Path rootInputDir = new Path(
        System.getProperty("src.test.data", "data")).makeQualified(lfs);
    final Path traceFile = new Path(rootInputDir, "19-jobs.trace.json.gz");
    final Path topologyFile = new Path(rootInputDir, "19-jobs.topology.json.gz");

    LOG.info("traceFile = " + traceFile + " topology = " + topologyFile);
    
    conf.setLong("mumak.start.time", 10);
    // Run 20 minutes of the simulation
    conf.setLong("mumak.terminate.time", 10 + 20*60*1000);
    conf.setLong("mumak.random.seed", 42);
    // SimulatorEngine reads conf and the system property too (!)
    System.setProperty("hadoop.log.dir", hadoopLogDir.toString());
    conf.set("hadoop.log.dir", hadoopLogDir.toString());
    conf.set("mapred.job.tracker.http.address",
             "0.0.0.0:" + jobTrackerHttpPort);
    conf.setBoolean(JTConfig.JT_PERSIST_JOBSTATUS, false);
    String[] args = { traceFile.toString(), topologyFile.toString() };
    int res = ToolRunner.run(conf, new SimulatorEngine(), args);
    Assert.assertEquals(0, res);
  }
}
