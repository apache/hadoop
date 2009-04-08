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
import java.util.Set;
import java.util.TreeSet;

import javax.security.auth.login.LoginException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UnixUserGroupInformation;

public class TestQueueManager extends TestCase {

  private static final Log LOG = LogFactory.getLog(TestQueueManager.class);
  
  private MiniDFSCluster miniDFSCluster;
  private MiniMRCluster miniMRCluster;

  public void testDefaultQueueConfiguration() {
    JobConf conf = new JobConf();
    QueueManager qMgr = new QueueManager(conf);
    Set<String> expQueues = new TreeSet<String>();
    expQueues.add("default");
    verifyQueues(expQueues, qMgr.getQueues());
    // pass true so it will fail if the key is not found.
    assertFalse(conf.getBoolean("mapred.acls.enabled", true));
  }
  
  public void testMultipleQueues() {
    JobConf conf = new JobConf();
    conf.set("mapred.queue.names", "q1,q2,Q3");
    QueueManager qMgr = new QueueManager(conf);
    Set<String> expQueues = new TreeSet<String>();
    expQueues.add("q1");
    expQueues.add("q2");
    expQueues.add("Q3");
    verifyQueues(expQueues, qMgr.getQueues());
  }
  
  public void testSchedulerInfo() {
    JobConf conf = new JobConf();
    conf.set("mapred.queue.names", "qq1,qq2");
    QueueManager qMgr = new QueueManager(conf);
    qMgr.setSchedulerInfo("qq1", "queueInfoForqq1");
    qMgr.setSchedulerInfo("qq2", "queueInfoForqq2");
    assertEquals(qMgr.getSchedulerInfo("qq2"), "queueInfoForqq2");
    assertEquals(qMgr.getSchedulerInfo("qq1"), "queueInfoForqq1");
  }
  
  public void testAllEnabledACLForJobSubmission() throws IOException {
    JobConf conf = setupConf("mapred.queue.default.acl-submit-job", "*");
    verifyJobSubmission(conf, true);
  }
  
  public void testAllDisabledACLForJobSubmission() throws IOException {
    JobConf conf = setupConf("mapred.queue.default.acl-submit-job", "");
    verifyJobSubmission(conf, false);
  }
  
  public void testUserDisabledACLForJobSubmission() throws IOException {
    JobConf conf = setupConf("mapred.queue.default.acl-submit-job", 
                                "3698-non-existent-user");
    verifyJobSubmission(conf, false);
  }
  
  public void testDisabledACLForNonDefaultQueue() throws IOException {
    // allow everyone in default queue
    JobConf conf = setupConf("mapred.queue.default.acl-submit-job", "*");
    // setup a different queue
    conf.set("mapred.queue.names", "default,q1");
    // setup a different acl for this queue.
    conf.set("mapred.queue.q1.acl-submit-job", "dummy-user");
    // verify job submission to other queue fails.
    verifyJobSubmission(conf, false, "q1");
  }
  
  public void testSubmissionToInvalidQueue() throws IOException{
    JobConf conf = new JobConf();
    conf.set("mapred.queue.names","default");
    setUpCluster(conf);
    String queueName = "q1";
    try {
      RunningJob rjob = submitSleepJob(1, 1, 100, 100, true, null, queueName);
    } catch (IOException ioe) {      
       assertTrue(ioe.getMessage().contains("Queue \"" + queueName + "\" does not exist"));
       return;
    } finally {
      tearDownCluster();
    }
    fail("Job submission to invalid queue job shouldnot complete , it should fail with proper exception ");   
  }
  
  public void testEnabledACLForNonDefaultQueue() throws IOException,
                                                          LoginException {
    // login as self...
    UserGroupInformation ugi = UnixUserGroupInformation.login();
    String userName = ugi.getUserName();
    // allow everyone in default queue
    JobConf conf = setupConf("mapred.queue.default.acl-submit-job", "*");
    // setup a different queue
    conf.set("mapred.queue.names", "default,q2");
    // setup a different acl for this queue.
    conf.set("mapred.queue.q2.acl-submit-job", userName);
    // verify job submission to other queue fails.
    verifyJobSubmission(conf, true, "q2");
  }
  
  public void testUserEnabledACLForJobSubmission() 
                                    throws IOException, LoginException {
    // login as self...
    UserGroupInformation ugi = UnixUserGroupInformation.login();
    String userName = ugi.getUserName();
    JobConf conf = setupConf("mapred.queue.default.acl-submit-job",
                                  "3698-junk-user," + userName 
                                    + " 3698-junk-group1,3698-junk-group2");
    verifyJobSubmission(conf, true);
  }
  
  public void testGroupsEnabledACLForJobSubmission() 
                                    throws IOException, LoginException {
    // login as self, get one group, and add in allowed list.
    UserGroupInformation ugi = UnixUserGroupInformation.login();
    String[] groups = ugi.getGroupNames();
    assertTrue(groups.length > 0);
    JobConf conf = setupConf("mapred.queue.default.acl-submit-job",
                                "3698-junk-user1,3698-junk-user2 " 
                                  + groups[groups.length-1] 
                                           + ",3698-junk-group");
    verifyJobSubmission(conf, true);
  }
  
  public void testAllEnabledACLForJobKill() throws IOException {
    JobConf conf = setupConf("mapred.queue.default.acl-administer-jobs", "*");
    verifyJobKill(conf, true);
  }

  public void testAllDisabledACLForJobKill() throws IOException {
    JobConf conf = setupConf("mapred.queue.default.acl-administer-jobs", "");
    verifyJobKillAsOtherUser(conf, false, "dummy-user,dummy-user-group");
  }
  
  public void testOwnerAllowedForJobKill() throws IOException {
    JobConf conf = setupConf("mapred.queue.default.acl-administer-jobs", 
                                              "junk-user");
    verifyJobKill(conf, true);
  }
  
  public void testUserDisabledACLForJobKill() throws IOException {
    //setup a cluster allowing a user to submit
    JobConf conf = setupConf("mapred.queue.default.acl-administer-jobs", 
                                              "dummy-user");
    verifyJobKillAsOtherUser(conf, false, "dummy-user,dummy-user-group");
  }
  
  public void testUserEnabledACLForJobKill() throws IOException, 
                                                    LoginException {
    // login as self...
    UserGroupInformation ugi = UnixUserGroupInformation.login();
    String userName = ugi.getUserName();
    JobConf conf = setupConf("mapred.queue.default.acl-administer-jobs",
                                              "dummy-user,"+userName);
    verifyJobKillAsOtherUser(conf, true, "dummy-user,dummy-user-group");
  }
  
  public void testUserDisabledForJobPriorityChange() throws IOException {
    JobConf conf = setupConf("mapred.queue.default.acl-administer-jobs",
                              "junk-user");
    verifyJobPriorityChangeAsOtherUser(conf, false, 
                              "junk-user,junk-user-group");
  }
  
  private JobConf setupConf(String aclName, String aclValue) {
    JobConf conf = new JobConf();
    conf.setBoolean("mapred.acls.enabled", true);
    conf.set(aclName, aclValue);
    return conf;
  }
  
  private void verifyQueues(Set<String> expectedQueues, 
                                          Set<String> actualQueues) {
    assertEquals(expectedQueues.size(), actualQueues.size());
    for (String queue : expectedQueues) {
      assertTrue(actualQueues.contains(queue));
    }
  }
  
  private void verifyJobSubmission(JobConf conf, boolean shouldSucceed) 
                                              throws IOException {
    verifyJobSubmission(conf, shouldSucceed, "default");
  }

  private void verifyJobSubmission(JobConf conf, boolean shouldSucceed, 
                                    String queue) throws IOException {
    setUpCluster(conf);
    try {
      RunningJob rjob = submitSleepJob(1, 1, 100, 100, true, null, queue);
      if (shouldSucceed) {
        assertTrue(rjob.isSuccessful());
      } else {
        fail("Job submission should have failed.");
      }
    } catch (IOException ioe) {
      if (shouldSucceed) {
        throw ioe;
      } else {
        LOG.info("exception while submitting job: " + ioe.getMessage());
        assertTrue(ioe.getMessage().
            contains("cannot perform operation " +
            "SUBMIT_JOB on queue " + queue));
        // check if the system directory gets cleaned up or not
        JobTracker jobtracker = miniMRCluster.getJobTrackerRunner().getJobTracker();
        Path sysDir = new Path(jobtracker.getSystemDir());
        FileSystem fs = sysDir.getFileSystem(conf);
        int size = fs.listStatus(sysDir).length;
        while (size > 1) { // ignore the jobtracker.info file
          System.out.println("Waiting for the job files in sys directory to be cleaned up");
          UtilsForTests.waitFor(100);
          size = fs.listStatus(sysDir).length;
        }
      }
    } finally {
      tearDownCluster();
    }
}

  private void verifyJobKill(JobConf conf, boolean shouldSucceed) 
                                      throws IOException {
    setUpCluster(conf);
    try {
      RunningJob rjob = submitSleepJob(1, 1, 1000, 1000, false);
      assertFalse(rjob.isComplete());
      while(rjob.mapProgress() == 0.0f) {
        try {
          Thread.sleep(10);  
        } catch (InterruptedException ie) {
          break;
        }
      }
      rjob.killJob();
      while(rjob.cleanupProgress() == 0.0f) {
        try {
          Thread.sleep(10);  
        } catch (InterruptedException ie) {
          break;
        }
      }
      if (shouldSucceed) {
        assertTrue(rjob.isComplete());
      } else {
        fail("Job kill should have failed.");
      }
    } catch (IOException ioe) {
      if (shouldSucceed) {
        throw ioe;
      } else {
        LOG.info("exception while submitting job: " + ioe.getMessage());
        assertTrue(ioe.getMessage().
                        contains("cannot perform operation " +
                                    "ADMINISTER_JOBS on queue default"));
      }
    } finally {
      tearDownCluster();
    }
  }

  
  private void verifyJobKillAsOtherUser(JobConf conf, boolean shouldSucceed,
                                        String otherUserInfo) 
                        throws IOException {
    setUpCluster(conf);
    try {
      // submit a job as another user.
      String userInfo = otherUserInfo;
      RunningJob rjob = submitSleepJob(1, 1, 1000, 1000, false, userInfo);
      assertFalse(rjob.isComplete());

      //try to kill as self
      try {
        rjob.killJob();
        if (!shouldSucceed) {
          fail("should fail kill operation");  
        }
      } catch (IOException ioe) {
        if (shouldSucceed) {
          throw ioe;
        }
        //verify it fails
        LOG.info("exception while submitting job: " + ioe.getMessage());
        assertTrue(ioe.getMessage().
                        contains("cannot perform operation " +
                                    "ADMINISTER_JOBS on queue default"));
      }
      //wait for job to complete on its own
      while (!rjob.isComplete()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          break;
        }
      }
    } finally {
      tearDownCluster();
    }
  }
  
  private void verifyJobPriorityChangeAsOtherUser(JobConf conf, 
                          boolean shouldSucceed, String otherUserInfo)
                            throws IOException {
    setUpCluster(conf);
    try {
      // submit job as another user.
      String userInfo = otherUserInfo;
      RunningJob rjob = submitSleepJob(1, 1, 1000, 1000, false, userInfo);
      assertFalse(rjob.isComplete());
      
      // try to change priority as self
      try {
        rjob.setJobPriority("VERY_LOW");
        if (!shouldSucceed) {
          fail("changing priority should fail.");
        }
      } catch (IOException ioe) {
        //verify it fails
        LOG.info("exception while submitting job: " + ioe.getMessage());
        assertTrue(ioe.getMessage().
                        contains("cannot perform operation " +
                                    "ADMINISTER_JOBS on queue default"));
      }
      //wait for job to complete on its own
      while (!rjob.isComplete()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          break;
        }
      }
    } finally {
      tearDownCluster();
    }
  }
  
  private void setUpCluster(JobConf conf) throws IOException {
    miniDFSCluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fileSys = miniDFSCluster.getFileSystem();
    String namenode = fileSys.getUri().toString();
    miniMRCluster = new MiniMRCluster(1, namenode, 3, 
                      null, null, conf);
  }
  
  private void tearDownCluster() throws IOException {
    if (miniMRCluster != null) { miniMRCluster.shutdown(); }
    if (miniDFSCluster != null) { miniDFSCluster.shutdown(); }
  }
  
  private RunningJob submitSleepJob(int numMappers, int numReducers, 
                            long mapSleepTime, long reduceSleepTime,
                            boolean shouldComplete) 
                              throws IOException {
    return submitSleepJob(numMappers, numReducers, mapSleepTime,
                          reduceSleepTime, shouldComplete, null);
  }
  
  private RunningJob submitSleepJob(int numMappers, int numReducers, 
                                      long mapSleepTime, long reduceSleepTime,
                                      boolean shouldComplete, String userInfo) 
                                            throws IOException {
    return submitSleepJob(numMappers, numReducers, mapSleepTime, 
                          reduceSleepTime, shouldComplete, userInfo, null);
  }

  private RunningJob submitSleepJob(int numMappers, int numReducers, 
                                    long mapSleepTime, long reduceSleepTime,
                                    boolean shouldComplete, String userInfo,
                                    String queueName) 
                                      throws IOException {
    JobConf clientConf = new JobConf();
    clientConf.set("mapred.job.tracker", "localhost:"
        + miniMRCluster.getJobTrackerPort());
    SleepJob job = new SleepJob();
    job.setConf(clientConf);
    clientConf = job.setupJobConf(numMappers, numReducers, 
        mapSleepTime, (int)mapSleepTime/100,
        reduceSleepTime, (int)reduceSleepTime/100);
    if (queueName != null) {
      clientConf.setQueueName(queueName);
    }
    RunningJob rJob = null;
    if (shouldComplete) {
      rJob = JobClient.runJob(clientConf);  
    } else {
      JobConf jc = new JobConf(clientConf);
      if (userInfo != null) {
        jc.set(UnixUserGroupInformation.UGI_PROPERTY_NAME, userInfo);
      }
      rJob = new JobClient(clientConf).submitJob(jc);
    }
    return rJob;
  }

}
