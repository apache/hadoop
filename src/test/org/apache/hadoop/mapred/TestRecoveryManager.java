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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobTracker.RecoveryManager;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Test whether the {@link RecoveryManager} is able to tolerate job-recovery 
 * failures and the jobtracker is able to tolerate {@link RecoveryManager}
 * failure.
 */
public class TestRecoveryManager extends TestCase {
  private static final Log LOG = 
    LogFactory.getLog(TestRecoveryManager.class);
  private static final Path TEST_DIR = 
    new Path(System.getProperty("test.build.data", "/tmp"), 
             "test-recovery-manager");
  
  /**
   * Tests the {@link JobTracker} against the exceptions thrown in 
   * {@link JobTracker.RecoveryManager}. It does the following :
   *  - submits 2 jobs
   *  - kills the jobtracker
   *  - Garble job.xml for one job causing it to fail in constructor 
   *    and job.split for another causing it to fail in init.
   *  - restarts the jobtracker
   *  - checks if the jobtraker starts normally
   */
  public void testJobTracker() throws Exception {
    LOG.info("Testing jobtracker restart with faulty job");
    String signalFile = new Path(TEST_DIR, "signal").toString();
    JobConf conf = new JobConf();
    
    FileSystem fs = FileSystem.get(new Configuration());
    fs.delete(TEST_DIR, true); // cleanup
    
    conf.set("mapred.jobtracker.job.history.block.size", "1024");
    conf.set("mapred.jobtracker.job.history.buffer.size", "1024");
    
    MiniMRCluster mr = new MiniMRCluster(1, "file:///", 1, null, null, conf);
    
    JobConf job1 = mr.createJobConf();
    
    UtilsForTests.configureWaitingJobConf(job1, 
        new Path(TEST_DIR, "input"), new Path(TEST_DIR, "output1"), 2, 0, 
        "test-recovery-manager", signalFile, signalFile);
    
    // submit the faulty job
    RunningJob rJob1 = (new JobClient(job1)).submitJob(job1);
    LOG.info("Submitted job " + rJob1.getID());
    
    while (rJob1.mapProgress() < 0.5f) {
      LOG.info("Waiting for job " + rJob1.getID() + " to be 50% done");
      UtilsForTests.waitFor(100);
    }
    
    JobConf job2 = mr.createJobConf();
    
    UtilsForTests.configureWaitingJobConf(job2, 
        new Path(TEST_DIR, "input"), new Path(TEST_DIR, "output2"), 30, 0, 
        "test-recovery-manager", signalFile, signalFile);
    
    // submit the faulty job
    RunningJob rJob2 = (new JobClient(job2)).submitJob(job2);
    LOG.info("Submitted job " + rJob2.getID());
    
    while (rJob2.mapProgress() < 0.5f) {
      LOG.info("Waiting for job " + rJob2.getID() + " to be 50% done");
      UtilsForTests.waitFor(100);
    }
    
    // kill the jobtracker
    LOG.info("Stopping jobtracker");
    String sysDir = mr.getJobTrackerRunner().getJobTracker().getSystemDir();
    mr.stopJobTracker();
    
    // delete the job.xml of job #1 causing the job to fail in constructor
    Path jobFile = 
      new Path(sysDir, rJob1.getID().toString() + Path.SEPARATOR + "job.xml");
    LOG.info("Deleting job.xml file : " + jobFile.toString());
    fs.delete(jobFile, false); // delete the job.xml file
    
    // create the job.xml file with 0 bytes
    FSDataOutputStream out = fs.create(jobFile);
    out.write(1);
    out.close();

    // delete the job.split of job #2 causing the job to fail in initTasks
    Path jobSplitFile = 
      new Path(sysDir, rJob2.getID().toString() + Path.SEPARATOR + "job.split");
    LOG.info("Deleting job.split file : " + jobSplitFile.toString());
    fs.delete(jobSplitFile, false); // delete the job.split file
    
    // create the job.split file with 0 bytes
    out = fs.create(jobSplitFile);
    out.write(1);
    out.close();

    // make sure that the jobtracker is in recovery mode
    mr.getJobTrackerConf().setBoolean("mapred.jobtracker.restart.recover", 
                                      true);
    // start the jobtracker
    LOG.info("Starting jobtracker");
    mr.startJobTracker();
    ClusterStatus status = 
      mr.getJobTrackerRunner().getJobTracker().getClusterStatus(false);
    
    // check if the jobtracker came up or not
    assertEquals("JobTracker crashed!", 
                 JobTracker.State.RUNNING, status.getJobTrackerState());
    
    mr.shutdown();
  }
  
  /**
   * Tests the {@link JobTracker.RecoveryManager} against the exceptions thrown 
   * during recovery. It does the following :
   *  - submits a job with HIGH priority and x tasks
   *  - allows it to complete 50%
   *  - submits another job with normal priority and y tasks
   *  - kills the jobtracker
   *  - restarts the jobtracker with max-tasks-per-job such that 
   *        y < max-tasks-per-job < x
   *  - checks if the jobtraker starts normally and job#2 is recovered while 
   *    job#1 is failed.
   */
  public void testRecoveryManager() throws Exception {
    LOG.info("Testing recovery-manager");
    String signalFile = new Path(TEST_DIR, "signal").toString();
    
    // clean up
    FileSystem fs = FileSystem.get(new Configuration());
    fs.delete(TEST_DIR, true);
    
    JobConf conf = new JobConf();
    conf.set("mapred.jobtracker.job.history.block.size", "1024");
    conf.set("mapred.jobtracker.job.history.buffer.size", "1024");
    
    MiniMRCluster mr = new MiniMRCluster(1, "file:///", 1, null, null, conf);
    JobTracker jobtracker = mr.getJobTrackerRunner().getJobTracker();
    
    JobConf job1 = mr.createJobConf();
    //  set the high priority
    job1.setJobPriority(JobPriority.HIGH);
    
    UtilsForTests.configureWaitingJobConf(job1, 
        new Path(TEST_DIR, "input"), new Path(TEST_DIR, "output3"), 30, 0, 
        "test-recovery-manager", signalFile, signalFile);
    
    // submit the faulty job
    JobClient jc = new JobClient(job1);
    RunningJob rJob1 = jc.submitJob(job1);
    LOG.info("Submitted first job " + rJob1.getID());
    
    while (rJob1.mapProgress() < 0.5f) {
      LOG.info("Waiting for job " + rJob1.getID() + " to be 50% done");
      UtilsForTests.waitFor(100);
    }
    
    // now submit job2
    JobConf job2 = mr.createJobConf();

    String signalFile1 = new Path(TEST_DIR, "signal1").toString();
    UtilsForTests.configureWaitingJobConf(job2, 
        new Path(TEST_DIR, "input"), new Path(TEST_DIR, "output4"), 20, 0, 
        "test-recovery-manager", signalFile1, signalFile1);
    
    // submit the job
    RunningJob rJob2 = (new JobClient(job2)).submitJob(job2);
    LOG.info("Submitted job " + rJob2.getID());
    
    // wait for it to init
    JobInProgress jip = jobtracker.getJob(rJob2.getID());
    
    while (!jip.inited()) {
      LOG.info("Waiting for job " + jip.getJobID() + " to be inited");
      UtilsForTests.waitFor(100);
    }
    
    // now submit job3 with inappropriate acls
    JobConf job3 = mr.createJobConf();
    job3.set("hadoop.job.ugi","abc,users");

    UtilsForTests.configureWaitingJobConf(job3, 
        new Path(TEST_DIR, "input"), new Path(TEST_DIR, "output5"), 1, 0, 
        "test-recovery-manager", signalFile, signalFile);
    
    // submit the job
    RunningJob rJob3 = (new JobClient(job3)).submitJob(job3);
    LOG.info("Submitted job " + rJob3.getID() + " with different user");
    
    jip = jobtracker.getJob(rJob3.getID());

    while (!jip.inited()) {
      LOG.info("Waiting for job " + jip.getJobID() + " to be inited");
      UtilsForTests.waitFor(100);
    }

    // kill the jobtracker
    LOG.info("Stopping jobtracker");
    mr.stopJobTracker();
    
    // make sure that the jobtracker is in recovery mode
    mr.getJobTrackerConf().setBoolean("mapred.jobtracker.restart.recover", 
                                      true);
    mr.getJobTrackerConf().setInt("mapred.jobtracker.maxtasks.per.job", 25);
    
    mr.getJobTrackerConf().setBoolean("mapred.acls.enabled" , true);
    UserGroupInformation ugi = UserGroupInformation.readFrom(job1);
    mr.getJobTrackerConf().set("mapred.queue.default.acl-submit-job", 
                               ugi.getUserName());

    // start the jobtracker
    LOG.info("Starting jobtracker");
    mr.startJobTracker();
    UtilsForTests.waitForJobTracker(jc);
    
    jobtracker = mr.getJobTrackerRunner().getJobTracker();
    
    // assert that job2 is recovered by the jobtracker as job1 would fail
    assertEquals("Recovery manager failed to tolerate job failures",
                 2, jobtracker.getAllJobs().length);
    
    // check if the job#1 has failed
    JobStatus status = jobtracker.getJobStatus(rJob1.getID());
    assertEquals("Faulty job not failed", 
                 JobStatus.FAILED, status.getRunState());
    
    jip = jobtracker.getJob(rJob2.getID());
    assertFalse("Job should be running", jip.isComplete());
    
    status = jobtracker.getJobStatus(rJob3.getID());
    assertNull("Job should be missing", status);
    
    mr.shutdown();
  }
}
