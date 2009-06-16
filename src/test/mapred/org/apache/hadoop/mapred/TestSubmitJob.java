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

import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.ToolRunner;

import junit.framework.TestCase;

public class TestSubmitJob extends TestCase {
  private MiniMRCluster miniMRCluster;

  @Override
  protected void tearDown()
      throws Exception {
    if (miniMRCluster != null) {
      miniMRCluster.shutdown();
    }
  }

  /**
   * Test to verify that jobs with invalid memory requirements are killed at the
   * JT.
   * 
   * @throws Exception
   */
  public void testJobWithInvalidMemoryReqs()
      throws Exception {
    JobConf jtConf = new JobConf();
    jtConf
        .setLong(JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024L);
    jtConf.setLong(JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY,
        2 * 1024L);
    jtConf.setLong(JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY,
        3 * 1024L);
    jtConf.setLong(JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY,
        4 * 1024L);

    miniMRCluster = new MiniMRCluster(0, "file:///", 0, null, null, jtConf);

    JobConf clusterConf = miniMRCluster.createJobConf();

    // No map-memory configuration
    JobConf jobConf = new JobConf(clusterConf);
    jobConf.setMemoryForReduceTask(1 * 1024L);
    runJobAndVerifyFailure(jobConf, JobConf.DISABLED_MEMORY_LIMIT, 1 * 1024L,
        "Invalid job requirements.");

    // No reduce-memory configuration
    jobConf = new JobConf(clusterConf);
    jobConf.setMemoryForMapTask(1 * 1024L);
    runJobAndVerifyFailure(jobConf, 1 * 1024L, JobConf.DISABLED_MEMORY_LIMIT,
        "Invalid job requirements.");

    // Invalid map-memory configuration
    jobConf = new JobConf(clusterConf);
    jobConf.setMemoryForMapTask(4 * 1024L);
    jobConf.setMemoryForReduceTask(1 * 1024L);
    runJobAndVerifyFailure(jobConf, 4 * 1024L, 1 * 1024L,
        "Exceeds the cluster's max-memory-limit.");

    // No reduce-memory configuration
    jobConf = new JobConf(clusterConf);
    jobConf.setMemoryForMapTask(1 * 1024L);
    jobConf.setMemoryForReduceTask(5 * 1024L);
    runJobAndVerifyFailure(jobConf, 1 * 1024L, 5 * 1024L,
        "Exceeds the cluster's max-memory-limit.");
  }

  private void runJobAndVerifyFailure(JobConf jobConf, long memForMapTasks,
      long memForReduceTasks, String expectedMsg)
      throws Exception,
      IOException {
    String[] args = { "-m", "0", "-r", "0", "-mt", "0", "-rt", "0" };
    boolean throwsException = false;
    String msg = null;
    try {
      ToolRunner.run(jobConf, new SleepJob(), args);
    } catch (RemoteException re) {
      throwsException = true;
      msg = re.unwrapRemoteException().getMessage();
    }
    assertTrue(throwsException);
    assertNotNull(msg);

    String overallExpectedMsg =
        "(" + memForMapTasks + " memForMapTasks " + memForReduceTasks
            + " memForReduceTasks): " + expectedMsg;
    assertTrue("Observed message - " + msg
        + " - doesn't contain expected message - " + overallExpectedMsg, msg
        .contains(overallExpectedMsg));
  }
}
