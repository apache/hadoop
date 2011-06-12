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

package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobACLsManager;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Verify the job-ACLs
 * 
 */
public class TestJobACLs {

  static final Log LOG = LogFactory.getLog(TestJobACLs.class);

  private MiniMRCluster mr = null;

  private static final Path TEST_DIR =
      new Path(System.getProperty("test.build.data", "/tmp"),
          TestJobACLs.class.getCanonicalName() + Path.SEPARATOR
              + "completed-job-store");

  /**
   * Start the cluster before running the actual test.
   * 
   * @throws IOException
   */
  @Before
  public void setup() throws IOException {
    // Start the cluster
    startCluster(false);
  }

  private void startCluster(boolean reStart) throws IOException {
    UserGroupInformation MR_UGI = UserGroupInformation.getLoginUser();
    JobConf conf = new JobConf();

    // Enable job-level authorization
    conf.setBoolean(MRConfig.JOB_LEVEL_AUTHORIZATION_ENABLING_FLAG, true);

    // Enable CompletedJobStore
    FileSystem fs = FileSystem.getLocal(conf);
    if (!reStart) {
      fs.delete(TEST_DIR, true);
    }
    conf.set(JTConfig.JT_PERSIST_JOBSTATUS_DIR,
        fs.makeQualified(TEST_DIR).toString());
    conf.setBoolean(JTConfig.JT_PERSIST_JOBSTATUS, true);
    conf.set(JTConfig.JT_PERSIST_JOBSTATUS_HOURS, "1");

    mr = new MiniMRCluster(0, 0, 1, "file:///", 1, null, null, MR_UGI, conf);
  }

  /**
   * Kill the cluster after the test is done.
   */
  @After
  public void tearDown() {
    if (mr != null) {
      mr.shutdown();
    }
  }

  /**
   * Test view-job-acl, modify-job-acl and acl persistence to the
   * completed-jobs-store.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  @Test
  public void testACLS() throws IOException, InterruptedException,
      ClassNotFoundException {
    verifyACLViewJob();
    verifyACLModifyJob();
    verifyACLPersistence();
  }

  /**
   * Verify JobContext.JOB_ACL_VIEW_JOB
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  private void verifyACLViewJob() throws IOException, InterruptedException {

    // Set the job up.
    final Configuration myConf = mr.createJobConf();
    myConf.set(MRJobConfig.JOB_ACL_VIEW_JOB, "user1,user3");

    // Submit the job as user1
    Job job = submitJobAsUser(myConf, "user1");

    final JobID jobId = job.getJobID();

    // Try operations as an unauthorized user.
    verifyViewJobAsUnauthorizedUser(myConf, jobId, "user2");

    // Try operations as an authorized user.
    verifyViewJobAsAuthorizedUser(myConf, jobId, "user3");

    // Clean up the job
    job.killJob();
  }

  /**
   * Submits a sleep job with 1 map task that runs for a long time(60 sec) and
   * wait for the job to go into RUNNING state.
   * @param clusterConf
   * @param user the jobOwner
   * @return Job that is started
   * @throws IOException
   * @throws InterruptedException
   */
  private Job submitJobAsUser(final Configuration clusterConf, String user)
      throws IOException, InterruptedException {
    UserGroupInformation ugi =
        UserGroupInformation.createUserForTesting(user, new String[] {});
    Job job = (Job) ugi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        SleepJob sleepJob = new SleepJob();
        sleepJob.setConf(clusterConf);
        // Disable setup/cleanup tasks at the job level
        sleepJob.getConf().setBoolean(MRJobConfig.SETUP_CLEANUP_NEEDED, false);
        Job myJob = sleepJob.createJob(1, 0, 60000, 1, 1, 1);
        myJob.submit();
        return myJob;
      }
    });

    // Make the job go into RUNNING state by forceful initialization.
    JobTracker jt = mr.getJobTrackerRunner().getJobTracker();
    JobInProgress jip =
        jt.getJob(org.apache.hadoop.mapred.JobID.downgrade(job.getJobID()));
    jt.initJob(jip);

    return job;
  }

  private void verifyViewJobAsAuthorizedUser(final Configuration myConf,
      final JobID jobId, String authorizedUser) throws IOException,
      InterruptedException {
    UserGroupInformation authorizedUGI =
        UserGroupInformation.createUserForTesting(authorizedUser,
            new String[] {});
    authorizedUGI.doAs(new PrivilegedExceptionAction<Object>() {
      @SuppressWarnings("null")
      @Override
      public Object run() throws Exception {
        Job myJob = null;
        try {
          Cluster cluster = new Cluster(myConf);
          myJob = cluster.getJob(jobId);
        } catch (Exception e) {
          fail("Exception .." + e);
        }

        assertNotNull("Job " + jobId + " is not known to the JobTracker!",
            myJob);

        // Tests authorization with getCounters
        try {
          myJob.getCounters();
        } catch (IOException ioe) {
          fail("Unexpected.. exception.. " + ioe);
        }

        // Tests authorization  with getTaskReports
        try {
          myJob.getTaskReports(TaskType.JOB_CLEANUP);
        } catch (IOException ioe) {
          fail("Unexpected.. exception.. " + ioe);
        }

        return null;
      }
    });
  }

  private void verifyViewJobAsUnauthorizedUser(final Configuration myConf,
      final JobID jobId, String unauthorizedUser) throws IOException,
      InterruptedException {
    UserGroupInformation unauthorizedUGI =
        UserGroupInformation.createUserForTesting(unauthorizedUser,
            new String[] {});
    unauthorizedUGI.doAs(new PrivilegedExceptionAction<Object>() {
      @SuppressWarnings("null")
      @Override
      public Object run() {
        Job myJob = null;
        try {
          Cluster cluster = new Cluster(myConf);
          myJob = cluster.getJob(jobId);
        } catch (Exception e) {
          fail("Exception .." + e);
        }

        assertNotNull("Job " + jobId + " is not known to the JobTracker!",
            myJob);

        // Tests authorization failure with getCounters
        try {
          myJob.getCounters();
          fail("AccessControlException expected..");
        } catch (IOException ioe) {
          assertTrue(ioe.getMessage().contains(
              JobACLsManager.UNAUTHORIZED_JOB_ACCESS_ERROR + JobACL.VIEW_JOB));
        } catch (InterruptedException e) {
          fail("Exception .. interrupted.." + e);
        }

        // Tests authorization failure with getTaskReports
        try {
          myJob.getTaskReports(TaskType.JOB_SETUP);
          fail("AccessControlException expected..");
        } catch (IOException ioe) {
          assertTrue(ioe.getMessage().contains(
              JobACLsManager.UNAUTHORIZED_JOB_ACCESS_ERROR + JobACL.VIEW_JOB));
        } catch (InterruptedException e) {
          fail("Exception .. interrupted.." + e);
        }

        return null;
      }
    });
  }

  /**
   * Verify JobContext.Job_ACL_MODIFY_JOB
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  private void verifyACLModifyJob() throws IOException,
      InterruptedException, ClassNotFoundException {

    // Set the job up.
    final Configuration myConf = mr.createJobConf();
    myConf.set(MRJobConfig.JOB_ACL_MODIFY_JOB, "user1,user3");

    // Submit the job as user1
    Job job = submitJobAsUser(myConf, "user1");

    final JobID jobId = job.getJobID();

    // Try operations as an unauthorized user.
    verifyModifyJobAsUnauthorizedUser(myConf, jobId, "user2");

    // Try operations as an authorized user.
    verifyModifyJobAsAuthorizedUser(myConf, jobId, "user3");
  }

  private void verifyModifyJobAsAuthorizedUser(
      final Configuration clusterConf, final JobID jobId,
      String authorizedUser) throws IOException, InterruptedException {
    UserGroupInformation authorizedUGI =
        UserGroupInformation.createUserForTesting(authorizedUser,
            new String[] {});
    authorizedUGI.doAs(new PrivilegedExceptionAction<Object>() {
      @SuppressWarnings("null")
      @Override
      public Object run() throws Exception {
        Job myJob = null;
        try {
          Cluster cluster = new Cluster(clusterConf);
          myJob = cluster.getJob(jobId);
        } catch (Exception e) {
          fail("Exception .." + e);
        }

        assertNotNull("Job " + jobId + " is not known to the JobTracker!",
            myJob);

        // Test authorization success with setJobPriority
        try {
          myJob.setPriority(JobPriority.HIGH);
          assertEquals(myJob.getPriority(), JobPriority.HIGH);
        } catch (IOException ioe) {
          fail("Unexpected.. exception.. " + ioe);
        }

        // Test authorization success with killJob
        try {
          myJob.killJob();
        } catch (IOException ioe) {
          fail("Unexpected.. exception.. " + ioe);
        }

        return null;
      }
    });
  }

  private void verifyModifyJobAsUnauthorizedUser(
      final Configuration clusterConf, final JobID jobId,
      String unauthorizedUser) throws IOException, InterruptedException {
    UserGroupInformation unauthorizedUGI =
        UserGroupInformation.createUserForTesting(unauthorizedUser,
            new String[] {});
    unauthorizedUGI.doAs(new PrivilegedExceptionAction<Object>() {
      @SuppressWarnings("null")
      @Override
      public Object run() {
        Job myJob = null;
        try {
          Cluster cluster = new Cluster(clusterConf);
          myJob = cluster.getJob(jobId);
        } catch (Exception e) {
          fail("Exception .." + e);
        }

        assertNotNull("Job " + jobId + " is not known to the JobTracker!",
            myJob);

        // Tests authorization failure with killJob
        try {
          myJob.killJob();
          fail("AccessControlException expected..");
        } catch (IOException ioe) {
          assertTrue(ioe.getMessage().contains(
            JobACLsManager.UNAUTHORIZED_JOB_ACCESS_ERROR + JobACL.MODIFY_JOB));
        } catch (InterruptedException e) {
          fail("Exception .. interrupted.." + e);
        }

        // Tests authorization failure with setJobPriority
        try {
          myJob.setPriority(JobPriority.HIGH);
          fail("AccessControlException expected..");
        } catch (IOException ioe) {
          assertTrue(ioe.getMessage().contains(
            JobACLsManager.UNAUTHORIZED_JOB_ACCESS_ERROR + JobACL.MODIFY_JOB));
        } catch (InterruptedException e) {
          fail("Exception .. interrupted.." + e);
        }

        return null;
      }
    });
  }

  private void verifyACLPersistence() throws IOException,
      InterruptedException {

    // Set the job up.
    final Configuration myConf = mr.createJobConf();
    myConf.set(MRJobConfig.JOB_ACL_VIEW_JOB, "user1,user2");

    // Submit the job as user1
    Job job = submitJobAsUser(myConf, "user1");

    final JobID jobId = job.getJobID();

    // Kill the job and wait till it is actually killed so that it is written to
    // CompletedJobStore
    job.killJob();
    while (job.getJobState() != JobStatus.State.KILLED) {
      LOG.info("Waiting for the job to be killed successfully..");
      Thread.sleep(200);
    }

    // Now kill the cluster, so that the job is 'forgotten'
    tearDown();

    // Re-start the cluster
    startCluster(true);

    final Configuration myNewJobConf = mr.createJobConf();
    // Now verify view-job works off CompletedJobStore
    verifyViewJobAsAuthorizedUser(myNewJobConf, jobId, "user2");

    // Only JobCounters is persisted on the JobStore. So test counters only.
    UserGroupInformation unauthorizedUGI =
        UserGroupInformation.createUserForTesting("user3", new String[] {});
    unauthorizedUGI.doAs(new PrivilegedExceptionAction<Object>() {
      @SuppressWarnings("null")
      @Override
      public Object run() {
        Job myJob = null;
        try {
          Cluster cluster = new Cluster(myNewJobConf);
          myJob = cluster.getJob(jobId);
        } catch (Exception e) {
          fail("Exception .." + e);
        }

        assertNotNull("Job " + jobId + " is not known to the JobTracker!",
            myJob);

        // Tests authorization failure with getCounters
        try {
          myJob.getCounters();
          fail("AccessControlException expected..");
        } catch (IOException ioe) {
          assertTrue(ioe.getMessage().contains(
              JobACLsManager.UNAUTHORIZED_JOB_ACCESS_ERROR + JobACL.VIEW_JOB));
        } catch (InterruptedException e) {
          fail("Exception .. interrupted.." + e);
        }

        return null;
      }
    });

  }
}
