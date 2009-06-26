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
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.UtilsForTests.FakeClock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Test if the job expiry works fine. 
 */
public class TestJobRetire extends TestCase {
  static final Path testDir = 
    new Path(System.getProperty("test.build.data","/tmp"), 
             "job-expiry-testing");
  private static final Log LOG = LogFactory.getLog(TestJobRetire.class);

  /** Test if the job after completion waits for atleast 
   *  mapred.jobtracker.retirejob.interval.min amount of time.
   */
  public void testMinIntervalBeforeRetire() throws Exception {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    int min = 10000;
    int max = 5000;
    try {
      FakeClock clock = new FakeClock();
      JobConf conf = new JobConf();
      dfs = new MiniDFSCluster(conf, 1, true, null);
      FileSystem fileSys = dfs.getFileSystem();
      String namenode = fileSys.getUri().toString();

      conf.setLong("mapred.jobtracker.retirejob.check", 1000); // 1 sec
      conf.setInt("mapred.jobtracker.retirejob.interval.min", min); //10 secs
      conf.setInt("mapred.jobtracker.retirejob.interval", max); // 5 secs
      conf.setBoolean("mapred.job.tracker.persist.jobstatus.active", false);
      mr = new MiniMRCluster(0, 0, 1, namenode, 1, null, null, null, conf, 0, 
                             clock);
      JobConf jobConf = mr.createJobConf();
      JobTracker jobtracker = mr.getJobTrackerRunner().getJobTracker();

      Path inDir = new Path(testDir, "input");
      Path outDir = new Path(testDir, "output");
      RunningJob rj = UtilsForTests.runJob(jobConf, inDir, outDir, 0, 0);
      rj.waitForCompletion();
      JobID id = rj.getID();
      JobClient jc = new JobClient(jobConf);

      // check if the job is there in the memory for min time
      assertTrue(rj.isSuccessful());

      // snapshot expiry thread count
      int snapshot = jobtracker.retireJobs.runCount;
      clock.advance(max + 1); // adv to expiry max time

      // wait for the thread to run
      while (jobtracker.retireJobs.runCount == snapshot) {
        // wait for the thread to run
        UtilsForTests.waitFor(1000);
      }

      assertNotNull(jc.getJob(id));

      // snapshot expiry thread count
      snapshot = jobtracker.retireJobs.runCount;
      clock.advance(min - max); // adv to expiry min time

      while (jobtracker.retireJobs.runCount == snapshot) {
        // wait for the thread to run
        UtilsForTests.waitFor(1000);
      }

      // check if the job is missing
      assertNull(jc.getJob(id));
    } finally {
      if (mr != null) { mr.shutdown();}
      if (dfs != null) { dfs.shutdown();}
    }
  }

  /** Test if the job after completion get expired after
   *  mapred.jobtracker.retirejob.interval amount after the time.
   */
  public void testJobRetire() throws Exception {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    int min = 10000;
    int max = 20000;
    try {
      FakeClock clock = new FakeClock();
      JobConf conf = new JobConf();
      dfs = new MiniDFSCluster(conf, 1, true, null);
      FileSystem fileSys = dfs.getFileSystem();
      String namenode = fileSys.getUri().toString();

      conf.setLong("mapred.jobtracker.retirejob.check", 1000); // 1 sec
      conf.setInt("mapred.jobtracker.retirejob.interval.min", min); // 10 secs
      conf.setInt("mapred.jobtracker.retirejob.interval", max); // 20 secs
      conf.setBoolean("mapred.job.tracker.persist.jobstatus.active", false);
      mr = new MiniMRCluster(0, 0, 1, namenode, 1, null, null, null, conf, 0, 
                             clock);
      JobConf jobConf = mr.createJobConf();
      JobTracker jobtracker = mr.getJobTrackerRunner().getJobTracker();
      
      Path inDir = new Path(testDir, "input1");
      Path outDir = new Path(testDir, "output1");
      RunningJob rj = UtilsForTests.runJob(jobConf, inDir, outDir, 0, 0);
      rj.waitForCompletion();
      JobID id = rj.getID();
      JobClient jc = new JobClient(jobConf);

      // check if the job is there in the memory for min time
      assertTrue(rj.isSuccessful());

      // snapshot expiry thread count
      int snapshot = jobtracker.retireJobs.runCount;
      clock.advance(max + 1); // adv to expiry max time

      while (jobtracker.retireJobs.runCount == snapshot) {
        // wait for the thread to run
        UtilsForTests.waitFor(1000);
      }

      // check if the job is missing
      assertNull(jc.getJob(id));
    } finally {
      if (mr != null) { mr.shutdown();}
      if (dfs != null) { dfs.shutdown();}
    }
  }
}
