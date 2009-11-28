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

import junit.framework.TestCase;
import java.io.IOException;

import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;

/**
 * A JUnit test to test configured task limits.
 */
public class TestTaskLimits extends TestCase {

  static void runTest(int maxTasks, int numMaps, int numReds, 
                      boolean shouldFail) throws Exception {
    JobConf conf = new JobConf();
    conf.setInt(JTConfig.JT_TASKS_PER_JOB, maxTasks);
    conf.set(JTConfig.JT_IPC_HANDLER_COUNT, "1");
    MiniMRCluster mr = new MiniMRCluster(0, "file:///", 1, null, null, conf);
    JobTracker jt = mr.getJobTrackerRunner().getJobTracker();
    JobConf jc = mr.createJobConf();
    jc.setNumMapTasks(numMaps);
    jc.setNumReduceTasks(numReds);
    JobInProgress jip = new JobInProgress(new JobID(), jc, jt);
    boolean failed = false;
    try {
      jip.checkTaskLimits();
    } catch (IOException e) {
      failed = true;
    }
    assertEquals(shouldFail, failed);
    mr.shutdown();
  }
  
  public void testBeyondLimits() throws Exception {
    // Max tasks is 4, Requested is 8, shouldFail = true
    runTest(4, 8, 0, true);
  }
  
  public void testTaskWithinLimits() throws Exception {
    // Max tasks is 4, requested is 4, shouldFail = false
    runTest(4, 4, 0, false);
  }


  public void testTaskWithoutLimits() throws Exception {
    // No task limit, requested is 16, shouldFail = false
    runTest(-1, 8, 8, false);
  }

}
