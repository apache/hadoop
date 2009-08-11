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

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;
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

  private void testJobConfFile(JobID id, boolean exists) {
    // get the job conf filename
    String name = JobHistory.JobInfo.getLocalJobFilePath(id);
    File file = new File(name);
 
    assertEquals("JobConf file check failed", exists, file.exists());
  }

  /** Test if the job after completion waits for atleast 
   *  mapred.jobtracker.retirejob.interval.min amount of time.
   */
  public void testMinIntervalBeforeRetire() throws Exception {
    MiniMRCluster mr = null;
    int min = 10000;
    int max = 5000;
    try {
      FakeClock clock = new FakeClock();
      JobConf conf = new JobConf();

      conf.setLong("mapred.jobtracker.retirejob.check", 1000); // 1 sec
      conf.setInt("mapred.jobtracker.retirejob.interval.min", min); //10 secs
      conf.setInt("mapred.jobtracker.retirejob.interval", max); // 5 secs
      conf.setBoolean("mapred.job.tracker.persist.jobstatus.active", false);
      conf.setInt("mapred.job.tracker.retiredjobs.cache.size", 0);
      mr = new MiniMRCluster(0, 0, 1, "file:///", 1, null, null, null, conf, 0, 
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

      //check that the job is not retired
      assertFalse(jc.getJob(id).isRetired());
      
      // snapshot expiry thread count
      snapshot = jobtracker.retireJobs.runCount;
      clock.advance(min - max); // adv to expiry min time

      while (jobtracker.retireJobs.runCount == snapshot) {
        // wait for the thread to run
        UtilsForTests.waitFor(1000);
      }

      // check if the job is missing
      assertNull(jc.getJob(id));
      assertNull(jobtracker.getJob(id));

      testJobConfFile(id, false);
    } finally {
      if (mr != null) { mr.shutdown();}
    }
  }

  /** Test if the job after completion get expired after
   *  mapred.jobtracker.retirejob.interval amount after the time.
   */
  public void testJobRetire() throws Exception {
    MiniMRCluster mr = null;
    int min = 10000;
    int max = 20000;
    try {
      FakeClock clock = new FakeClock();
      JobConf conf = new JobConf();

      conf.setLong("mapred.jobtracker.retirejob.check", 1000); // 1 sec
      conf.setInt("mapred.jobtracker.retirejob.interval.min", min); // 10 secs
      conf.setInt("mapred.jobtracker.retirejob.interval", max); // 20 secs
      conf.setBoolean("mapred.job.tracker.persist.jobstatus.active", false);
      conf.setInt("mapred.job.tracker.retiredjobs.cache.size", 0);
      mr = new MiniMRCluster(0, 0, 1, "file:///", 1, null, null, null, conf, 0, 
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
      assertNull(jobtracker.getJob(id));

      testJobConfFile(id, false);
    } finally {
      if (mr != null) { mr.shutdown();}
    }
  }

  /** Test if the job after gets expired after
   *  mapred.jobtracker.completeuserjobs.maximum jobs.
   */
  public void testMaxJobRetire() throws Exception {
    MiniMRCluster mr = null;
    int min = 10000;
    int max = 20000;
    try {
      FakeClock clock = new FakeClock();
      JobConf conf = new JobConf();
      
      conf.setLong("mapred.jobtracker.retirejob.check", 1000); // 1 sec
      conf.setInt("mapred.jobtracker.retirejob.interval.min", min); // 10 secs
      conf.setInt("mapred.jobtracker.retirejob.interval", max); // 20 secs
      conf.setBoolean("mapred.job.tracker.persist.jobstatus.active", false);
      conf.setInt("mapred.jobtracker.completeuserjobs.maximum", 1);
      conf.setInt("mapred.job.tracker.retiredjobs.cache.size", 0);
      mr = new MiniMRCluster(0, 0, 1, "file:///", 1, null, null, null, conf, 0, 
                             clock);
      JobConf jobConf = mr.createJobConf();
      JobTracker jobtracker = mr.getJobTrackerRunner().getJobTracker();
      
      Path inDir = new Path(testDir, "input2.1");
      Path outDir = new Path(testDir, "output2.1");
      RunningJob rj = UtilsForTests.runJob(jobConf, inDir, outDir, 0, 0);
      rj.waitForCompletion();
      JobID id = rj.getID();
      JobClient jc = new JobClient(jobConf);

      // check if the job is successful
      assertTrue(rj.isSuccessful());

      clock.advance(min + 1); // adv to expiry min time

      inDir = new Path(testDir, "input2.2");
      outDir = new Path(testDir, "output2.2");
      RunningJob rj2 = UtilsForTests.runJob(jobConf, inDir, outDir, 0, 0);
      rj2.waitForCompletion();
      JobID id2 = rj2.getID();

      // check if the job#1 is missing
      assertNull(jc.getJob(id));
      assertNull("Job still not missing from jobtracker", jobtracker.getJob(id));
      
      // check if the job#2 exists
      assertNotNull(jc.getJob(id2));
      assertNotNull("Job " + id2 + " missing at the jobtracker before expiry", 
                    jobtracker.getJob(id2));

      testJobConfFile(id, false);
      testJobConfFile(id2, true);
    } finally {
      if (mr != null) {mr.shutdown();}
    }
  }

  /** Test if the job after gets expired but basic info is cached with jobtracker
   */
  public void testRetiredJobCache() throws Exception {
    MiniMRCluster mr = null;
    int min = 10000;
    int max = 20000;
    try {
      FakeClock clock = new FakeClock();
      JobConf conf = new JobConf();

      conf.setLong("mapred.jobtracker.retirejob.check", 1000); // 1 sec
      conf.setInt("mapred.jobtracker.retirejob.interval.min", min); // 10 secs
      conf.setInt("mapred.jobtracker.retirejob.interval", max); // 20 secs
      conf.setBoolean("mapred.job.tracker.persist.jobstatus.active", false);
      conf.setInt("mapred.jobtracker.completeuserjobs.maximum", 1);
      conf.setInt("mapred.job.tracker.retiredjobs.cache.size", 1);
      mr = new MiniMRCluster(0, 0, 1, "file:///", 1, null, null, null, conf, 0,
                             clock);
      JobConf jobConf = mr.createJobConf();
      JobTracker jobtracker = mr.getJobTrackerRunner().getJobTracker();

      Path inDir = new Path(testDir, "input3.1");
      Path outDir = new Path(testDir, "output3.1");
      RunningJob rj = UtilsForTests.runJob(jobConf, inDir, outDir, 0, 0);
      rj.waitForCompletion();
      JobID id = rj.getID();
      JobClient jc = new JobClient(jobConf);

      // check if the job is successful
      assertTrue(rj.isSuccessful());
      JobStatus status1 = jobtracker.getJobStatus(id);

      clock.advance(min + 1); // adv to expiry min time

      inDir = new Path(testDir, "input3.2");
      outDir = new Path(testDir, "output3.2");
      RunningJob rj2 = UtilsForTests.runJob(jobConf, inDir, outDir, 0, 0);
      rj2.waitForCompletion();
      JobID id2 = rj2.getID();
      JobStatus status2 = jobtracker.getJobStatus(id2);

      // check if the job#1 is missing in jt but cached status
      assertNotNull("Job status missing from status cache", jc.getJob(id));
      // check the status at jobtracker
      assertEquals("Status mismatch for job " + id, status1.toString(), 
                   jobtracker.getJobStatus(id).toString());
      testRetiredCachedJobStatus(status1, rj);
      assertNull("Job still not missing from jobtracker", jobtracker.getJob(id));

      // check if the job#2 exists
      assertNotNull(jc.getJob(id2));
      // check the status .. 
      
      assertNotNull("Job " + id2 + " missing at the jobtracker before expiry",
                    jobtracker.getJob(id2));

      testJobConfFile(id, false);
      testJobConfFile(id2, true);

      clock.advance(min + 1); // adv to expiry min time

      inDir = new Path(testDir, "input3.3");
      outDir = new Path(testDir, "output3.3");
      RunningJob rj3 = UtilsForTests.runJob(jobConf, inDir, outDir, 0, 0);
      rj3.waitForCompletion();
      JobID id3 = rj3.getID();

      // check if the job#1 is missing in all the caches
      assertNull("Job status still in status cache", jc.getJob(id));
      // check if the job#2 is missing in jt but cached status
      assertNotNull(jc.getJob(id2));
      assertEquals("Status mismatch for job " + id2, status2.toString(), 
                   jobtracker.getJobStatus(id2).toString());
      testRetiredCachedJobStatus(status2, rj2);
      assertNull("Job " + id2 + " missing at the jobtracker before expiry",
                 jobtracker.getJob(id2));
      // check if the job#3 exists
      assertNotNull(jc.getJob(id3));
      assertNotNull("Job " + id3 + " missing at the jobtracker before expiry",
                    jobtracker.getJob(id3));
    } finally {
      if (mr != null) {mr.shutdown();}
    }
  }

  private static void testRetiredCachedJobStatus(JobStatus status, 
                                                 RunningJob rj) 
  throws IOException {
    assertEquals(status.getJobID(), rj.getID());
    assertEquals(status.mapProgress(), rj.mapProgress());
    assertEquals(status.reduceProgress(), rj.reduceProgress());
    assertEquals(status.setupProgress(), rj.setupProgress());
    assertEquals(status.cleanupProgress(), rj.cleanupProgress());
    assertEquals(status.getRunState(), rj.getJobState());
    assertEquals(status.getJobName(), rj.getJobName());
    assertEquals(status.getTrackingUrl(), rj.getTrackingURL());
    assertEquals(status.isRetired(), true);
  }
}
