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
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobTracker.RecoveryManager;
import org.apache.hadoop.mapred.MiniMRCluster.JobTrackerRunner;
import org.apache.hadoop.mapred.QueueManager.QueueACL;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test whether the {@link RecoveryManager} is able to tolerate job-recovery 
 * failures and the jobtracker is able to tolerate {@link RecoveryManager}
 * failure.
 */

public class TestRecoveryManager {
  private static final Log LOG = 
    LogFactory.getLog(TestRecoveryManager.class);
  private static final Path TEST_DIR = 
    new Path(System.getProperty("test.build.data", "/tmp"), 
             "test-recovery-manager");
  
  private FileSystem fs;
  private MiniDFSCluster dfs;
  private MiniMRCluster mr;

  @Before
  public void setUp() throws IOException {
    fs = FileSystem.get(new Configuration());
    fs.delete(TEST_DIR, true);
  }

  private void startCluster() throws IOException {
    startCluster(new JobConf());
  }

  private void startCluster(JobConf conf) throws IOException {
    mr = new MiniMRCluster(1, "file:///", 1, null, null, conf);
  }

  @After
  public void tearDown() {
    try {
      if (mr != null) {
        ClusterStatus status = mr.getJobTrackerRunner().getJobTracker()
            .getClusterStatus(false);
        if (status.getJobTrackerState() == JobTracker.State.RUNNING) {
          mr.shutdown();
        }
      }
    } finally {
      mr = null;
      
      try {
        if (dfs != null) {
          dfs.shutdown();
          dfs = null;
        }
      } finally {
        dfs = null;
      }
    }
  }
  
  /**
   * Tests the {@link JobTracker} against the exceptions thrown in 
   * {@link JobTracker.RecoveryManager}. It does the following :
   *  - submits 2 jobs
   *  - kills the jobtracker
   *  - deletes the info file for one job
   *  - restarts the jobtracker
   *  - checks if the jobtraker starts normally
   */
  @Test(timeout=120000)
  public void testJobTrackerRestartsWithMissingJobFile() throws Exception {
    LOG.info("Testing jobtracker restart with faulty job");
    startCluster();
    String signalFile = new Path(TEST_DIR, "signal").toString();

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
    
    // submit another job
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
      new Path(sysDir, rJob1.getID().toString() + "/" + JobTracker.JOB_INFO_FILE);
    LOG.info("Deleting job token file : " + jobFile.toString());
    Assert.assertTrue(fs.delete(jobFile, false)); // delete the job.xml file
    
    // create the job.xml file with 1 bytes
    FSDataOutputStream out = fs.create(jobFile);
    out.write(1);
    out.close();

    // make sure that the jobtracker is in recovery mode
    mr.getJobTrackerConf().setBoolean("mapred.jobtracker.restart.recover", 
                                      true);
    // start the jobtracker
    LOG.info("Starting jobtracker");
    mr.startJobTracker();
    JobTracker jobtracker = mr.getJobTrackerRunner().getJobTracker();
    ClusterStatus status = jobtracker.getClusterStatus(false);
    
    // check if the jobtracker came up or not
    Assert.assertEquals("JobTracker crashed!", 
                 JobTracker.State.RUNNING, status.getJobTrackerState());

    // wait for job 2 to complete
    JobInProgress jip = jobtracker.getJob(rJob2.getID());
    while (!jip.isComplete()) {
      LOG.info("Waiting for job " + rJob2.getID() + " to be successful");
      // Signaling Map task to complete
      fs.create(new Path(TEST_DIR, "signal"));
      UtilsForTests.waitFor(100);
    }
    Assert.assertTrue("Job should be successful", rJob2.isSuccessful());
  }
  
  /**
   * Tests the re-submission of the job in case of jobtracker died/restart  
   *  - submits a job and let it be inited.
   *  - kills the jobtracker
   *  - checks if the jobtraker starts normally and job is recovered while 
   */
  @Test(timeout=120000)
  public void testJobResubmission() throws Exception {
    LOG.info("Testing Job Resubmission");
    startCluster();
    String signalFile = new Path(TEST_DIR, "signal").toString();

    // make sure that the jobtracker is in recovery mode
    mr.getJobTrackerConf()
        .setBoolean("mapred.jobtracker.restart.recover", true);

    JobTracker jobtracker = mr.getJobTrackerRunner().getJobTracker();

    JobConf job1 = mr.createJobConf();
    UtilsForTests.configureWaitingJobConf(job1, new Path(TEST_DIR, "input"),
        new Path(TEST_DIR, "output3"), 2, 0, "test-resubmission", signalFile,
        signalFile);

    JobClient jc1 = new JobClient(job1);
    RunningJob rJob1 = jc1.submitJob(job1);
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
    job2.setBoolean(JobConf.MAPREDUCE_RECOVER_JOB, false); // don't recover
    
    // submit the job
    RunningJob rJob2 = (new JobClient(job2)).submitJob(job2);
    LOG.info("Submitted job " + rJob2.getID());
    
    // wait for it to init
    JobInProgress jip2 = jobtracker.getJob(rJob2.getID());
    
    while (!jip2.inited()) {
      LOG.info("Waiting for job " + jip2.getJobID() + " to be inited");
      UtilsForTests.waitFor(100);
    }

    // kill the jobtracker
    LOG.info("Stopping jobtracker");
    mr.stopJobTracker();

    // start the jobtracker
    LOG.info("Starting jobtracker");
    mr.startJobTracker();
    UtilsForTests.waitForJobTracker(jc1);

    jobtracker = mr.getJobTrackerRunner().getJobTracker();

    // assert that only job1 is recovered by the jobtracker
    Assert.assertEquals("Resubmission failed ", 1, 
        jobtracker.getAllJobs().length);

    // wait for job 1 to complete
    JobInProgress jip = jobtracker.getJob(rJob1.getID());
    while (!jip.isComplete()) {
      LOG.info("Waiting for job " + rJob1.getID() + " to be successful");
      // Signaling Map task to complete
      fs.create(new Path(TEST_DIR, "signal"));
      UtilsForTests.waitFor(100);
    }
    Assert.assertTrue("Task should be successful", rJob1.isSuccessful());
    Assert.assertTrue("Job should be cleaned up", !fs.exists(new Path(job1.get("mapreduce.job.dir"))));
    Assert.assertTrue("Job should be cleaned up", !fs.exists(new Path(job2.get("mapreduce.job.dir"))));
  }

  public static class TestJobTrackerInstrumentation extends JobTrackerInstrumentation {
    static CountDownLatch finalizeCall = new CountDownLatch(1);

    public TestJobTrackerInstrumentation(JobTracker jt, JobConf conf) {
      super(jt, conf);
    }

    public void finalizeJob(JobConf conf, JobID id) {
      if (finalizeCall.getCount() == 0) {
        return;
      }
      finalizeCall.countDown();
      throw new IllegalStateException("Controlled error finalizing job");
    }
  }

  @Test
  public void testJobTrackerRestartBeforeJobFinalization() throws Exception {
    LOG.info("Testing Job Resubmission");

    JobConf conf = new JobConf();
    // make sure that the jobtracker is in recovery mode
    conf.setBoolean("mapred.jobtracker.restart.recover", true);

    // use a test JobTrackerInstrumentation implementation to shut down
    // the jobtracker after the tasks have all completed, but
    // before the job is finalized and check that it can be recovered correctly
    conf.setClass("mapred.jobtracker.instrumentation", TestJobTrackerInstrumentation.class,
            JobTrackerInstrumentation.class);

    startCluster(conf);

    JobTracker jobtracker = mr.getJobTrackerRunner().getJobTracker();

    SleepJob job = new SleepJob();
    job.setConf(mr.createJobConf());
    JobConf job1 = job.setupJobConf(1, 0, 1, 1, 1, 1);
    JobClient jc = new JobClient(job1);
    RunningJob rJob1 = jc.submitJob(job1);
    LOG.info("Submitted first job " + rJob1.getID());

    TestJobTrackerInstrumentation.finalizeCall.await();

    // kill the jobtracker
    LOG.info("Stopping jobtracker");
    mr.stopJobTracker();

    // start the jobtracker
    LOG.info("Starting jobtracker");
    mr.startJobTracker();
    UtilsForTests.waitForJobTracker(jc);

    jobtracker = mr.getJobTrackerRunner().getJobTracker();

    // assert that job is recovered by the jobtracker
    Assert.assertEquals("Resubmission failed ", 1,
        jobtracker.getAllJobs().length);

    // wait for job 1 to complete
    JobInProgress jip = jobtracker.getJob(rJob1.getID());
    while (!jip.isComplete()) {
      LOG.info("Waiting for job " + rJob1.getID() + " to be successful");
      UtilsForTests.waitFor(100);
    }
    Assert.assertTrue("Task should be successful", rJob1.isSuccessful());
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
  @Test(timeout=120000)
  public void testJobTrackerRestartWithBadJobs() throws Exception {
    LOG.info("Testing recovery-manager");
    startCluster();
    String signalFile = new Path(TEST_DIR, "signal").toString();
    // make sure that the jobtracker is in recovery mode
    mr.getJobTrackerConf()
        .setBoolean("mapred.jobtracker.restart.recover", true);
    
    JobTracker jobtracker = mr.getJobTrackerRunner().getJobTracker();
    
    JobConf job1 = mr.createJobConf();
    //  set the high priority
    job1.setJobPriority(JobPriority.HIGH);
    
    UtilsForTests.configureWaitingJobConf(job1, 
        new Path(TEST_DIR, "input"), new Path(TEST_DIR, "output4"), 30, 0,
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
        new Path(TEST_DIR, "input"), new Path(TEST_DIR, "output5"), 20, 0,
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
    final JobConf job3 = mr.createJobConf();
    UserGroupInformation ugi3 = 
      UserGroupInformation.createUserForTesting("abc", new String[]{"users"});
    
    UtilsForTests.configureWaitingJobConf(job3, 
        new Path(TEST_DIR, "input"), new Path(TEST_DIR, "output6"), 1, 0,
        "test-recovery-manager", signalFile, signalFile);
    
    // submit the job
    RunningJob rJob3 = ugi3.doAs(new PrivilegedExceptionAction<RunningJob>() {
      public RunningJob run() throws IOException {
        return (new JobClient(job3)).submitJob(job3); 
      }
    });
      
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
    
    mr.getJobTrackerConf().setBoolean(JobConf.MR_ACLS_ENABLED, true);
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    mr.getJobTrackerConf().set(QueueManager.toFullPropertyName(
        "default", QueueACL.SUBMIT_JOB.getAclName()), ugi.getUserName());

    // start the jobtracker
    LOG.info("Starting jobtracker");
    mr.startJobTracker();
    UtilsForTests.waitForJobTracker(jc);
    
    jobtracker = mr.getJobTrackerRunner().getJobTracker();
    
    // assert that job2 is recovered by the jobtracker as job1 would fail
    Assert.assertEquals("Recovery manager failed to tolerate job failures", 1,
        jobtracker.getAllJobs().length);
    
    // check if the job#1 has failed
    JobStatus status = jobtracker.getJobStatus(rJob1.getID());
    Assert.assertNull("Faulty job should not be resubmitted", status);
    
    jip = jobtracker.getJob(rJob2.getID());
    Assert.assertFalse("Job should be running", jip.isComplete());
    
    status = jobtracker.getJobStatus(rJob3.getID());
    Assert.assertNull("Job should be missing because of ACL changed", status);

    // wait for job 2 to complete
    while (!jip.isComplete()) {
      LOG.info("Waiting for job " + rJob2.getID() + " to be successful");
      // Signaling Map task to complete
      fs.create(new Path(TEST_DIR, "signal1"));
      UtilsForTests.waitFor(100);
    }
    Assert.assertTrue("Job should be successful", rJob2.isSuccessful());
  }
  
  /**
   * Test if restart count of the jobtracker is correctly managed.
   * Steps are as follows :
   *   - start the jobtracker and check if the info file gets created.
   *   - stops the jobtracker, deletes the jobtracker.info file and checks if
   *     upon restart the recovery is 'off'
   *   - submit a job to the jobtracker.
   *   - restart the jobtracker k times and check if the restart count on ith 
   *     iteration is i.
   *   - submit a new job and check if its restart count is 0.
   *   - garble the jobtracker.info file and restart he jobtracker, the 
   *     jobtracker should crash.
   */
  @Test(timeout=120000)
  public void testRestartCount() throws Exception {
    LOG.info("Testing Job Restart Count");
    startCluster();
    String signalFile = new Path(TEST_DIR, "signal").toString();
    // make sure that the jobtracker is in recovery mode
    mr.getJobTrackerConf()
        .setBoolean("mapred.jobtracker.restart.recover", true);

    JobTracker jobtracker = mr.getJobTrackerRunner().getJobTracker();

    JobConf job1 = mr.createJobConf();
    // set the high priority
    job1.setJobPriority(JobPriority.HIGH);

    UtilsForTests.configureWaitingJobConf(job1, new Path(TEST_DIR, "input"),
        new Path(TEST_DIR, "output7"), 30, 0, "test-restart", signalFile,
        signalFile);

    // submit the faulty job
    JobClient jc = new JobClient(job1);
    RunningJob rJob1 = jc.submitJob(job1);
    LOG.info("Submitted first job " + rJob1.getID());

    JobInProgress jip = jobtracker.getJob(rJob1.getID());

    while (!jip.inited()) {
      LOG.info("Waiting for job " + jip.getJobID() + " to be inited");
      UtilsForTests.waitFor(100);
    }

    for (int i = 1; i <= 2; ++i) {
      LOG.info("Stopping jobtracker for " + i + " time");
      mr.stopJobTracker();

      // start the jobtracker
      LOG.info("Starting jobtracker for " + i + " time");
      mr.startJobTracker();

      UtilsForTests.waitForJobTracker(jc);

      jobtracker = mr.getJobTrackerRunner().getJobTracker();

      // assert if restart count is correct
      // It should always be 0 now as its resubmit everytime then restart.
      Assert.assertEquals("Recovery manager failed to recover restart count", 
          0, jip.getNumRestarts());
    }

    // kill the old job
    rJob1.killJob();

    // II. Submit a new job and check if the restart count is 0
    JobConf job2 = mr.createJobConf();

    UtilsForTests.configureWaitingJobConf(job2, new Path(TEST_DIR, "input"),
        new Path(TEST_DIR, "output8"), 50, 0, "test-restart-manager",
        signalFile, signalFile);

    // submit a new job
    RunningJob rJob2 = jc.submitJob(job2);
    LOG.info("Submitted first job after restart" + rJob2.getID());

    // assert if restart count is correct
    jip = jobtracker.getJob(rJob2.getID());
    Assert.assertEquals("Restart count for new job is incorrect", 0, jip
        .getNumRestarts());

    LOG.info("Stopping jobtracker for testing the fs errors");
    mr.stopJobTracker();

    // check if system.dir problems in recovery kills the jobtracker
    Path rFile = jobtracker.recoveryManager.getRestartCountFile();
    fs.delete(rFile, false);
    FSDataOutputStream out = fs.create(rFile);
    out.writeBoolean(true);
    out.close();

    // start the jobtracker
    LOG.info("Starting jobtracker with fs errors");
    mr.startJobTracker();
    JobTrackerRunner runner = mr.getJobTrackerRunner();
    Assert.assertFalse("JobTracker is still alive", runner.isActive());

  }

  /**
   * Test if the jobtracker waits for the info file to be created before 
   * starting.
   */
  @Test(timeout=120000)
  public void testJobTrackerInfoCreation() throws Exception {
    LOG.info("Testing jobtracker.info file");
    dfs = new MiniDFSCluster(new Configuration(), 1, true, null);
    String namenode = (dfs.getFileSystem()).getUri().getHost() + ":"
                      + (dfs.getFileSystem()).getUri().getPort();
    // shut down the data nodes
    dfs.shutdownDataNodes();

    // start the jobtracker
    JobConf conf = new JobConf();
    FileSystem.setDefaultUri(conf, namenode);
    conf.set("mapred.job.tracker", "localhost:0");
    conf.set("mapred.job.tracker.http.address", "127.0.0.1:0");

    JobTracker jobtracker = new JobTracker(conf);
    jobtracker.setSafeModeInternal(JobTracker.SafeModeAction.SAFEMODE_ENTER);
    jobtracker.initializeFilesystem();
    jobtracker.setSafeModeInternal(JobTracker.SafeModeAction.SAFEMODE_LEAVE);
    jobtracker.initialize();

    // now check if the update restart count works fine or not
    boolean failed = false;
    try {
      jobtracker.recoveryManager.updateRestartCount();
    } catch (IOException ioe) {
      failed = true;
    }
    Assert.assertTrue("JobTracker created info files without datanodes!!!", 
        failed);

    Path restartFile = jobtracker.recoveryManager.getRestartCountFile();
    Path tmpRestartFile = jobtracker.recoveryManager.getTempRestartCountFile();
    FileSystem fs = dfs.getFileSystem();
    Assert.assertFalse("Info file exists after update failure", 
                fs.exists(restartFile));
    Assert.assertFalse("Temporary restart-file exists after update failure", 
                fs.exists(restartFile));

    // start 1 data node
    dfs.startDataNodes(conf, 1, true, null, null, null, null);
    dfs.waitActive();

    failed = false;
    try {
      jobtracker.recoveryManager.updateRestartCount();
    } catch (IOException ioe) {
      failed = true;
    }
    Assert.assertFalse("JobTracker failed to create info files with datanodes!", 
        failed);
  }
  
  static void mkdirWithPerms(FileSystem fs, String dir, short mode) throws IOException {
    Path p = new Path(dir);
    fs.mkdirs(p);
    fs.setPermission(p, new FsPermission(mode));
  }
  
  @Test(timeout=120000)
  public void testJobResubmissionAsDifferentUser() throws Exception {
    LOG.info("Testing Job Resubmission as a different user to the jobtracker");
    
    final Path HDFS_TEST_DIR = new Path("/tmp");
    
    JobConf conf = new JobConf();

    dfs = new MiniDFSCluster(conf, 1, true, null);
    fs = dfs.getFileSystem();

    conf.set("mapreduce.jobtracker.staging.root.dir", "/user");
    conf.set("mapred.system.dir", "/mapred");
    
    String mapredSysDir =  conf.get("mapred.system.dir");
    mkdirWithPerms(fs, mapredSysDir, (short)0700);
    fs.setOwner(new Path(mapredSysDir),
        UserGroupInformation.getCurrentUser().getUserName(), "mrgroup");

    mkdirWithPerms(fs, "/user", (short)0777);
    mkdirWithPerms(fs, "/mapred", (short)0777);
    mkdirWithPerms(fs, "/tmp", (short)0777);

    mr = 
        new MiniMRCluster(
            1, dfs.getFileSystem().getUri().toString(), 1, null, null, conf);

    String signalFile = new Path(HDFS_TEST_DIR, "signal").toString();

    // make sure that the jobtracker is in recovery mode
    mr.getJobTrackerConf()
    .setBoolean("mapred.jobtracker.restart.recover", true);

    JobTracker jobtracker = mr.getJobTrackerRunner().getJobTracker();

    final JobConf job1 = mr.createJobConf();
    UtilsForTests.configureWaitingJobConf(job1, new Path(HDFS_TEST_DIR, "input"),
        new Path(HDFS_TEST_DIR, "output3"), 2, 0, "test-resubmission", signalFile,
        signalFile);

    UserGroupInformation ugi =
        UserGroupInformation.createUserForTesting("bob", new String[]{"users"});
    job1.setUser(ugi.getUserName());

    JobClient jc = new JobClient(job1);
    RunningJob rJob1 = ugi.doAs(new PrivilegedExceptionAction<RunningJob>() {
      public RunningJob run() throws IOException {
        JobClient jc = new JobClient(job1);
        return jc.submitJob(job1);
      }
    });
    LOG.info("Submitted first job " + rJob1.getID());

    while (rJob1.mapProgress() < 0.5f) {
      LOG.info("Waiting for job " + rJob1.getID() + " to be 50% done");
      UtilsForTests.waitFor(100);
    }

    // kill the jobtracker
    LOG.info("Stopping jobtracker");
    mr.stopJobTracker();

    // Blocking JT INIT on restart
    mr.getJobTrackerConf().setBoolean(
        JobTracker.JT_INIT_CONFIG_KEY_FOR_TESTS, false);
    

    // start the jobtracker
    LOG.info("Starting jobtracker");
    mr.startJobTracker(false);

    while (!mr.getJobTrackerRunner().isUp()) {
      Thread.sleep(100);
    }
    jobtracker = mr.getJobTrackerRunner().getJobTracker();
    Assert.assertNotNull(jobtracker);
    
    // now check for job status ... 
    // should throw JobTrackerNotYetInitializedException
    boolean gotJTNYIException = false;
    try {
      jobtracker.getJobStatus(rJob1.getID());
    } catch (JobTrackerNotYetInitializedException jtnyie) {
      LOG.info("Caught JobTrackerNotYetInitializedException", jtnyie);
      gotJTNYIException = true;
    }
    Assert.assertTrue(gotJTNYIException);
    
    jobtracker.setInitDone(true);

    UtilsForTests.waitForJobTracker(jc);

    // assert that job is recovered by the jobtracker
    Assert.assertEquals("Resubmission failed ", 1, jobtracker.getAllJobs().length);
    JobInProgress jip = jobtracker.getJob(rJob1.getID());

    // Signaling Map task to complete
    fs.create(new Path(HDFS_TEST_DIR, "signal"));
    while (!jip.isComplete()) {
      LOG.info("Waiting for job " + rJob1.getID() + " to be successful");
      UtilsForTests.waitFor(100);
    }
    rJob1 = jc.getJob(rJob1.getID());
    Assert.assertTrue("Task should be successful", rJob1.isSuccessful());
  }

  @Test(timeout=120000)
  public void testJobInitError() throws Exception {
    LOG.info("Testing error during Job submission");
    
    final Path HDFS_TEST_DIR = new Path("/tmp");
    
    JobConf conf = new JobConf();
    
    dfs = new MiniDFSCluster(conf, 1, true, null);
    fs = dfs.getFileSystem();

    conf.set("mapreduce.jobtracker.staging.root.dir", "/user");
    conf.set("mapred.system.dir", "/mapred");
    
    String mapredSysDir =  conf.get("mapred.system.dir");
    mkdirWithPerms(fs, mapredSysDir, (short)0700);
    fs.setOwner(new Path(mapredSysDir),
        UserGroupInformation.getCurrentUser().getUserName(), "mrgroup");

    mkdirWithPerms(fs, "/user", (short)0777);
    mkdirWithPerms(fs, "/mapred", (short)0777);
    mkdirWithPerms(fs, "/tmp", (short)0777);

    mr = 
        new MiniMRCluster(
            1, dfs.getFileSystem().getUri().toString(), 1, null, null, conf);

    String signalFile = new Path(HDFS_TEST_DIR, "signal").toString();

    // make sure that the jobtracker is in recovery mode
    mr.getJobTrackerConf()
    .setBoolean("mapred.jobtracker.restart.recover", true);

    JobTracker jobtracker = mr.getJobTrackerRunner().getJobTracker();

    final JobConf job1 = mr.createJobConf();
    UtilsForTests.configureWaitingJobConf(job1, new Path(HDFS_TEST_DIR, "input"),
        new Path(HDFS_TEST_DIR, "output3"), 2, 0, "test-resubmission", signalFile,
        signalFile);

    UserGroupInformation ugi =
        UserGroupInformation.createUserForTesting("bob", new String[]{"users"});
    job1.setUser(ugi.getUserName());

    JobClient jc = new JobClient(job1);
    RunningJob rJob1 = ugi.doAs(new PrivilegedExceptionAction<RunningJob>() {
      public RunningJob run() throws IOException {
        // Job 1 init should fail
        job1.setBoolean(JobInProgress.JOB_INIT_EXCEPTION, true);
        JobClient jc = new JobClient(job1);
        return jc.submitJob(job1);
      }
    });
    LOG.info("Submitted first job " + rJob1.getID());

    // kill the jobtracker
    LOG.info("Stopping jobtracker");
    mr.stopJobTracker();

    // start the jobtracker, after turning off job-init exception
    LOG.info("Starting jobtracker");
    mr.getJobTrackerConf().setBoolean(
        JobInProgress.JT_JOB_INIT_EXCEPTION_OVERRIDE, true);
    mr.startJobTracker(false);

    while (!mr.getJobTrackerRunner().isUp()) {
      Thread.sleep(100);
    }
    jobtracker = mr.getJobTrackerRunner().getJobTracker();
    Assert.assertNotNull(jobtracker);
    
    UtilsForTests.waitForJobTracker(jc);

    // assert that job is recovered by the jobtracker
    Assert.assertEquals("Resubmission failed ", 1, jobtracker.getAllJobs().length);
    JobInProgress jip = jobtracker.getJob(rJob1.getID());

    // Signaling Map task to complete
    fs.create(new Path(HDFS_TEST_DIR, "signal"));
    while (!jip.isComplete()) {
      LOG.info("Waiting for job " + rJob1.getID() + " to be successful");
      UtilsForTests.waitFor(100);
    }
    rJob1 = jc.getJob(rJob1.getID());
    Assert.assertTrue("Task should be successful", rJob1.isSuccessful());
  }
}
