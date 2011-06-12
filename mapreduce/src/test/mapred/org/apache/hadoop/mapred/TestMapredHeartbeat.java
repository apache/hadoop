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

import junit.framework.TestCase;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;

public class TestMapredHeartbeat extends TestCase {
  public void testJobDirCleanup() throws IOException {
    MiniMRCluster mr = null;
    try {
      // test the default heartbeat interval
      int taskTrackers = 2;
      JobConf conf = new JobConf();
      mr = new MiniMRCluster(taskTrackers, "file:///", 3, 
          null, null, conf);
      JobClient jc = new JobClient(mr.createJobConf());
      while(jc.getClusterStatus().getTaskTrackers() != taskTrackers) {
        UtilsForTests.waitFor(100);
      }
      assertEquals(MRConstants.HEARTBEAT_INTERVAL_MIN, 
        mr.getJobTrackerRunner().getJobTracker().getNextHeartbeatInterval());
      mr.shutdown(); 
      
      // test configured heartbeat interval
      taskTrackers = 5;
      conf.setInt(JTConfig.JT_HEARTBEATS_IN_SECOND, 1);
      mr = new MiniMRCluster(taskTrackers, "file:///", 3, 
          null, null, conf);
      jc = new JobClient(mr.createJobConf());
      while(jc.getClusterStatus().getTaskTrackers() != taskTrackers) {
        UtilsForTests.waitFor(100);
      }
      assertEquals(taskTrackers * 1000, 
        mr.getJobTrackerRunner().getJobTracker().getNextHeartbeatInterval());
      mr.shutdown(); 
      
      // test configured heartbeat interval is capped with min value
      taskTrackers = 5;
      conf.setInt(JTConfig.JT_HEARTBEATS_IN_SECOND, 10);
      mr = new MiniMRCluster(taskTrackers, "file:///", 3, 
          null, null, conf);
      jc = new JobClient(mr.createJobConf());
      while(jc.getClusterStatus().getTaskTrackers() != taskTrackers) {
        UtilsForTests.waitFor(100);
      }
      assertEquals(MRConstants.HEARTBEAT_INTERVAL_MIN, 
        mr.getJobTrackerRunner().getJobTracker().getNextHeartbeatInterval());
    } finally {
      if (mr != null) { mr.shutdown(); }
    }
  }
}


