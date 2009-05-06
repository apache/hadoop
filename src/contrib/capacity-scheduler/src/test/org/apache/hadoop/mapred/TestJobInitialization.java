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

import java.util.Properties;
import org.apache.hadoop.mapred.ControlledMapReduceJob.ControlledMapReduceJobRunner;

public class TestJobInitialization extends ClusterWithCapacityScheduler {
 
  public void testFailingJobInitalization() throws Exception {
    Properties schedulerProps = new Properties();
    schedulerProps.put(
        "mapred.capacity-scheduler.queue.default.capacity", "100");
    Properties clusterProps = new Properties();
    clusterProps
        .put("mapred.tasktracker.map.tasks.maximum", String.valueOf(1));
    clusterProps.put("mapred.tasktracker.reduce.tasks.maximum", String
        .valueOf(1));
    clusterProps.put("mapred.jobtracker.maxtasks.per.job", String
        .valueOf(1));
    // cluster capacity 1 maps, 1 reduces
    startCluster(1, clusterProps, schedulerProps);
    ControlledMapReduceJobRunner jobRunner =
      ControlledMapReduceJobRunner.getControlledMapReduceJobRunner(
          getJobConf(), 3, 3);
    jobRunner.start();
    JobID myJobID = jobRunner.getJobID();
    JobInProgress myJob = getJobTracker().getJob(myJobID);
    while(!myJob.isComplete()) {
      Thread.sleep(1000);
    }
    assertTrue("The submitted job successfully completed", 
        myJob.status.getRunState() == JobStatus.FAILED);
    CapacityTaskScheduler scheduler = (CapacityTaskScheduler) getJobTracker().getTaskScheduler();
    JobQueuesManager mgr = scheduler.jobQueuesManager;
    assertEquals("Failed job present in Waiting queue", 
        0, mgr.getWaitingJobCount("default"));
    assertFalse("Failed job present in Waiting queue", 
        mgr.getWaitingJobs("default").contains(myJob));
  }
}
