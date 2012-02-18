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

import org.apache.hadoop.mapred.FakeObjectUtilities.FakeJobTracker;
import org.apache.hadoop.mapred.TestRackAwareTaskPlacement.MyFakeJobInProgress;
import org.apache.hadoop.mapred.UtilsForTests.FakeClock;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;

/**
 * A JUnit test to test that killing completed jobs does not move them
 * to the failed sate - See JIRA HADOOP-2132
 */
public class TestKillCompletedJob extends TestCase {

  MyFakeJobInProgress job;
  static FakeJobTracker jobTracker;
 
  static FakeClock clock;
  
  static String trackers[] = new String[] {"tracker_tracker1:1000"};

  @Override
  protected void setUp() throws Exception {
    JobConf conf = new JobConf();
    conf.set(JTConfig.JT_IPC_ADDRESS, "localhost:0");
    conf.set(JTConfig.JT_HTTP_ADDRESS, "0.0.0.0:0");
    conf.setLong(JTConfig.JT_TRACKER_EXPIRY_INTERVAL, 1000);
    jobTracker = new FakeJobTracker(conf, (clock = new FakeClock()), trackers);
  }

  
  @SuppressWarnings("deprecation")
  public void testKillCompletedJob() throws IOException, InterruptedException {
    job = new MyFakeJobInProgress(new JobConf(), jobTracker);
    jobTracker.addJob(job.getJobID(), (JobInProgress)job);
    job.status.setRunState(JobStatus.SUCCEEDED);

    jobTracker.killJob(job.getJobID());

    assertTrue("Run state changed when killing completed job" ,
        job.status.getRunState() == JobStatus.SUCCEEDED);

  }

}
 
