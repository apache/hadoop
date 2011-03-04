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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import javax.security.auth.login.LoginException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SleepJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;

public class TestQueueManager extends TestCase {

  private static final Log LOG = LogFactory.getLog(TestQueueManager.class);
  
  private MiniDFSCluster miniDFSCluster;
  private MiniMRCluster miniMRCluster;
  
  /**
   * For some tests it is necessary to sandbox them in a doAs with a fake user
   * due to bug HADOOP-6527, which wipes out real group mappings. It's also
   * necessary to then add the real user running the test to the fake users
   * so that child processes can write to the DFS.
   */
  private UserGroupInformation createNecessaryUsers() throws IOException {
    // Add real user to fake groups mapping so that child processes (tasks)
    // will have permissions on the dfs
    String j = UserGroupInformation.getCurrentUser().getUserName();
    UserGroupInformation.createUserForTesting(j, new String [] { "supergroup"});
    
    
    // Create a fake superuser for all processes to execute within
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting("Zork",
                                                 new String [] {"Zork"});
    return ugi;
  }
  
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
  
  public void testAllEnabledACLForJobSubmission() 
  throws IOException, InterruptedException {
    JobConf conf = setupConf("mapred.queue.default.acl-submit-job", "*");
    verifyJobSubmission(conf, true);
  }
  
  public void testAllDisabledACLForJobSubmission() 
  throws IOException, InterruptedException {
    JobConf conf = setupConf("mapred.queue.default.acl-submit-job", "");
    verifyJobSubmission(conf, false);
  }
  
  public void testUserDisabledACLForJobSubmission() 
  throws IOException, InterruptedException {
    JobConf conf = setupConf("mapred.queue.default.acl-submit-job", 
                                "3698-non-existent-user");
    verifyJobSubmission(conf, false);
  }
  
  public void testDisabledACLForNonDefaultQueue() 
  throws IOException, InterruptedException {
    // allow everyone in default queue
    JobConf conf = setupConf("mapred.queue.default.acl-submit-job", "*");
    // setup a different queue
    conf.set("mapred.queue.names", "default,q1");
    // setup a different acl for this queue.
    conf.set("mapred.queue.q1.acl-submit-job", "dummy-user");
    // verify job submission to other queue fails.
    verifyJobSubmission(conf, false, "q1");
  }
  
  public void testSubmissionToInvalidQueue() 
  throws IOException, InterruptedException{
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
  
  public void testEnabledACLForNonDefaultQueue() 
  throws IOException, LoginException, InterruptedException {
    // login as self...
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
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
  throws IOException, LoginException, InterruptedException {
    // login as self...
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String userName = ugi.getUserName();
    JobConf conf = setupConf("mapred.queue.default.acl-submit-job",
                                  "3698-junk-user," + userName 
                                    + " 3698-junk-group1,3698-junk-group2");
    verifyJobSubmission(conf, true);
  }
  
  public void testGroupsEnabledACLForJobSubmission() 
  throws IOException, LoginException, InterruptedException {
    // login as self, get one group, and add in allowed list.
    UserGroupInformation ugi = createNecessaryUsers();
    
    ugi.doAs(new PrivilegedExceptionAction<Object>() {

      @Override
      public Object run() throws Exception {
        String[] groups = UserGroupInformation.getCurrentUser().getGroupNames();
        JobConf conf = setupConf("mapred.queue.default.acl-submit-job",
                                    "3698-junk-user1,3698-junk-user2 " 
                                      + groups[groups.length-1] 
                                               + ",3698-junk-group");
        verifyJobSubmission(conf, true);
        
        return null;
      }
    });

  }
  
  public void testAllEnabledACLForJobKill() 
  throws IOException, InterruptedException {
    UserGroupInformation ugi = createNecessaryUsers();
    
    ugi.doAs(new PrivilegedExceptionAction<Object>() {

      @Override
      public Object run() throws Exception {
        JobConf conf = setupConf("mapred.queue.default.acl-administer-jobs", "*");
        verifyJobKill(conf, true);
        return null;
      }
    });
  }

  public void testAllDisabledACLForJobKill() 
  throws IOException, InterruptedException {
    // Create a fake superuser for all processes to execute within
    UserGroupInformation ugi = createNecessaryUsers();
    ugi.doAs(new PrivilegedExceptionAction<Object>() {

      @Override
      public Object run() throws Exception {
        // No one should be able to kill jobs
        JobConf conf = setupConf("mapred.queue.default.acl-administer-jobs", "");
        // Run as dummy-user, who (obviously) is not able to kill the job,
        // and expect him to fail
        verifyJobKillAsOtherUser(conf, false, "dummy-user,dummy-group");
        return null;
      }
    });
  }
  
  public void testOwnerAllowedForJobKill() 
  throws IOException, InterruptedException {
    UserGroupInformation ugi = createNecessaryUsers();
    
    ugi.doAs(new PrivilegedExceptionAction<Object>() {

      @Override
      public Object run() throws Exception {

        JobConf conf = setupConf("mapred.queue.default.acl-administer-jobs", 
                                              "junk-user");
        verifyJobKill(conf, true);
        return null;
      }
    });
  }
  
  public void testUserDisabledACLForJobKill() 
  throws IOException, InterruptedException {
    UserGroupInformation ugi = createNecessaryUsers();
    
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
      //setup a cluster allowing a user to submit
        JobConf conf = setupConf("mapred.queue.default.acl-administer-jobs", 
                                                "dummy-user");
        verifyJobKillAsOtherUser(conf, false, "dummy-user,dummy-group");
        return null;
      }
    });
   }
  
  public void testUserEnabledACLForJobKill() 
  throws IOException, LoginException, InterruptedException {
  UserGroupInformation ugi = createNecessaryUsers();
  
  ugi.doAs(new PrivilegedExceptionAction<Object>() {
    @Override
    public Object run() throws Exception {
      // login as self...
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      String userName = ugi.getUserName();
      JobConf conf = setupConf("mapred.queue.default.acl-administer-jobs",
                                                "dummy-user,"+userName);
      verifyJobKillAsOtherUser(conf, true, "dummy-user,dummy-group");
      return null;
      }
    });
  }

  public void testUserDisabledForJobPriorityChange() 
  throws IOException, InterruptedException {
    UserGroupInformation ugi = createNecessaryUsers();
    ugi.doAs(new PrivilegedExceptionAction<Object>() {

      @Override
      public Object run() throws Exception {

        JobConf conf = setupConf("mapred.queue.default.acl-administer-jobs",
                              "junk-user");
        verifyJobPriorityChangeAsOtherUser(conf, false, 
                              "junk-user,dummy-group");
        return null;
      }
    });
  }

  /**
   * Test to verify refreshing of queue properties by using MRAdmin tool.
   * 
   * @throws Exception
   */
  public void testACLRefresh() throws Exception {
    String queueConfigPath =
        System.getProperty("test.build.extraconf", "build/test/extraconf");
    File queueConfigFile =
        new File(queueConfigPath, QueueManager.QUEUE_ACLS_FILE_NAME);
    File hadoopConfigFile = new File(queueConfigPath, "mapred-site.xml");
    try {
      //Setting up default mapred-site.xml
      Properties hadoopConfProps = new Properties();
      //these properties should be retained.
      hadoopConfProps.put("mapred.queue.names", "default,q1,q2");
      hadoopConfProps.put("mapred.acls.enabled", "true");
      //These property should always be overridden
      hadoopConfProps.put("mapred.queue.default.acl-submit-job", "u1");
      hadoopConfProps.put("mapred.queue.q1.acl-submit-job", "u2");
      hadoopConfProps.put("mapred.queue.q2.acl-submit-job", "u1");
      UtilsForTests.setUpConfigFile(hadoopConfProps, hadoopConfigFile);
      
      //Actual property which would be used.
      Properties queueConfProps = new Properties();
      queueConfProps.put("mapred.queue.default.acl-submit-job", " ");
      //Writing out the queue configuration file.
      UtilsForTests.setUpConfigFile(queueConfProps, queueConfigFile);
      
      //Create a new configuration to be used with QueueManager
      JobConf conf = new JobConf();
      QueueManager queueManager = new QueueManager(conf);
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      //Job Submission should fail because ugi to be used is set to blank.
      assertFalse("User Job Submission Succeeded before refresh.",
          queueManager.hasAccess("default", QueueManager.QueueOperation.
              SUBMIT_JOB, ugi));
      assertFalse("User Job Submission Succeeded before refresh.",
          queueManager.hasAccess("q1", QueueManager.QueueOperation.
              SUBMIT_JOB, ugi));
      assertFalse("User Job Submission Succeeded before refresh.",
          queueManager.hasAccess("q2", QueueManager.QueueOperation.
              SUBMIT_JOB, ugi));
      
      //Test job submission as alternate user.
      UserGroupInformation alternateUgi = 
        UserGroupInformation.createUserForTesting("u1", new String[]{"user"});
      assertTrue("Alternate User Job Submission failed before refresh.",
          queueManager.hasAccess("q2", QueueManager.QueueOperation.
              SUBMIT_JOB, alternateUgi));
      
      //Set acl for the current user.
      queueConfProps.put("mapred.queue.default.acl-submit-job", ugi.getUserName());
      queueConfProps.put("mapred.queue.q1.acl-submit-job", ugi.getUserName());
      queueConfProps.put("mapred.queue.q2.acl-submit-job", ugi.getUserName());
      //write out queue-acls.xml.
      UtilsForTests.setUpConfigFile(queueConfProps, queueConfigFile);
      //refresh configuration
      queueManager.refreshAcls(conf);
      //Submission should succeed
      assertTrue("User Job Submission failed after refresh.",
          queueManager.hasAccess("default", QueueManager.QueueOperation.
              SUBMIT_JOB, ugi));
      assertTrue("User Job Submission failed after refresh.",
          queueManager.hasAccess("q1", QueueManager.QueueOperation.
              SUBMIT_JOB, ugi));
      assertTrue("User Job Submission failed after refresh.",
          queueManager.hasAccess("q2", QueueManager.QueueOperation.
              SUBMIT_JOB, ugi));
      assertFalse("Alternate User Job Submission succeeded after refresh.",
          queueManager.hasAccess("q2", QueueManager.QueueOperation.
              SUBMIT_JOB, alternateUgi));
      //delete the ACL file.
      queueConfigFile.delete();
      
      //rewrite the mapred-site.xml
      hadoopConfProps.put("mapred.acls.enabled", "true");
      hadoopConfProps.put("mapred.queue.default.acl-submit-job", ugi.getUserName());
      UtilsForTests.setUpConfigFile(hadoopConfProps, hadoopConfigFile);
      queueManager.refreshAcls(conf);
      assertTrue("User Job Submission failed after refresh and no queue acls file.",
          queueManager.hasAccess("default", QueueManager.QueueOperation.
              SUBMIT_JOB, ugi));
    } finally{
      if(queueConfigFile.exists()) {
        queueConfigFile.delete();
      }
      if(hadoopConfigFile.exists()) {
        hadoopConfigFile.delete();
      }
    }
  }

  public void testQueueAclRefreshWithInvalidConfFile() throws IOException {
    String queueConfigPath =
      System.getProperty("test.build.extraconf", "build/test/extraconf");
    File queueConfigFile =
      new File(queueConfigPath, QueueManager.QUEUE_ACLS_FILE_NAME);
    File hadoopConfigFile = new File(queueConfigPath, "hadoop-site.xml");
    try {
      // queue properties with which the cluster is started.
      Properties hadoopConfProps = new Properties();
      hadoopConfProps.put("mapred.queue.names", "default,q1,q2");
      hadoopConfProps.put("mapred.acls.enabled", "true");
      UtilsForTests.setUpConfigFile(hadoopConfProps, hadoopConfigFile);
      
      //properties for mapred-queue-acls.xml
      Properties queueConfProps = new Properties();
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      queueConfProps.put("mapred.queue.default.acl-submit-job", ugi.getUserName());
      queueConfProps.put("mapred.queue.q1.acl-submit-job", ugi.getUserName());
      queueConfProps.put("mapred.queue.q2.acl-submit-job", ugi.getUserName());
      UtilsForTests.setUpConfigFile(queueConfProps, queueConfigFile);
      
      Configuration conf = new JobConf();
      QueueManager queueManager = new QueueManager(conf);
      //Testing access to queue.
      assertTrue("User Job Submission failed.",
          queueManager.hasAccess("default", QueueManager.QueueOperation.
              SUBMIT_JOB, ugi));
      assertTrue("User Job Submission failed.",
          queueManager.hasAccess("q1", QueueManager.QueueOperation.
              SUBMIT_JOB, ugi));
      assertTrue("User Job Submission failed.",
          queueManager.hasAccess("q2", QueueManager.QueueOperation.
              SUBMIT_JOB, ugi));
      
      //Write out a new incomplete invalid configuration file.
      PrintWriter writer = new PrintWriter(new FileOutputStream(queueConfigFile));
      writer.println("<configuration>");
      writer.println("<property>");
      writer.flush();
      writer.close();
      try {
        //Exception to be thrown by queue manager because configuration passed
        //is invalid.
        queueManager.refreshAcls(conf);
        fail("Refresh of ACLs should have failed with invalid conf file.");
      } catch (Exception e) {
      }
      assertTrue("User Job Submission failed after invalid conf file refresh.",
          queueManager.hasAccess("default", QueueManager.QueueOperation.
              SUBMIT_JOB, ugi));
      assertTrue("User Job Submission failed after invalid conf file refresh.",
          queueManager.hasAccess("q1", QueueManager.QueueOperation.
              SUBMIT_JOB, ugi));
      assertTrue("User Job Submission failed after invalid conf file refresh.",
          queueManager.hasAccess("q2", QueueManager.QueueOperation.
              SUBMIT_JOB, ugi));
    } finally {
      //Cleanup the configuration files in all cases
      if(hadoopConfigFile.exists()) {
        hadoopConfigFile.delete();
      }
      if(queueConfigFile.exists()) {
        queueConfigFile.delete();
      }
    }
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
                                     throws IOException, InterruptedException {
    verifyJobSubmission(conf, shouldSucceed, "default");
  }

  private void verifyJobSubmission(JobConf conf, boolean shouldSucceed, 
      String queue) throws IOException, InterruptedException {
    setUpCluster(conf);
    try {
      runAndVerifySubmission(conf, shouldSucceed, queue, null);
    } finally {
      tearDownCluster();
    }
  }

  private void runAndVerifySubmission(JobConf conf, boolean shouldSucceed,
      String queue, String userInfo)
      throws IOException, InterruptedException {
    try {
      RunningJob rjob = submitSleepJob(1, 1, 100, 100, true, userInfo, queue);
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
                                      throws IOException, InterruptedException {
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
                        throws IOException, InterruptedException {
    setUpCluster(conf);
    try {
      // submit a job as another user.
      String userInfo = otherUserInfo;
      RunningJob rjob = submitSleepJob(1, 1, 1000, 1000, false, userInfo);
      assertFalse(rjob.isComplete());

      //try to kill as self
      try {
        conf.set("mapred.job.tracker", "localhost:"
            + miniMRCluster.getJobTrackerPort());
        JobClient client = new JobClient(miniMRCluster.createJobConf());
        client.getJob(rjob.getID()).killJob();
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
                            throws IOException, InterruptedException {
    setUpCluster(conf);
    try {
      // submit job as another user.
      String userInfo = otherUserInfo;
      RunningJob rjob = submitSleepJob(1, 1, 1000, 1000, false, userInfo);
      assertFalse(rjob.isComplete());
      
      // try to change priority as self
      try {
        conf.set("mapred.job.tracker", "localhost:"
            + miniMRCluster.getJobTrackerPort());
        JobClient client = new JobClient(miniMRCluster.createJobConf());
        client.getJob(rjob.getID()).setJobPriority("VERY_LOW");
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
    TestMiniMRWithDFSWithDistinctUsers.mkdir(fileSys, "/user");
    TestMiniMRWithDFSWithDistinctUsers.mkdir(fileSys,
        conf.get("mapreduce.jobtracker.staging.root.dir",
            "/tmp/hadoop/mapred/staging"));
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
                              throws IOException, InterruptedException {
    return submitSleepJob(numMappers, numReducers, mapSleepTime,
                          reduceSleepTime, shouldComplete, null);
  }
  
  private RunningJob submitSleepJob(int numMappers, int numReducers, 
                                      long mapSleepTime, long reduceSleepTime,
                                      boolean shouldComplete, String userInfo) 
                                     throws IOException, InterruptedException {
    return submitSleepJob(numMappers, numReducers, mapSleepTime, 
                          reduceSleepTime, shouldComplete, userInfo, null);
  }

  private RunningJob submitSleepJob(final int numMappers, final int numReducers, 
      final long mapSleepTime,
      final long reduceSleepTime, final boolean shouldComplete, String userInfo,
                                    String queueName) 
                                      throws IOException, InterruptedException {
    JobConf clientConf = new JobConf();
    clientConf.set("mapred.job.tracker", "localhost:"
        + miniMRCluster.getJobTrackerPort());
    UserGroupInformation ugi;
    SleepJob job = new SleepJob();
    job.setConf(clientConf);
    clientConf = job.setupJobConf(numMappers, numReducers, 
        mapSleepTime, (int)mapSleepTime/100,
        reduceSleepTime, (int)reduceSleepTime/100);
    if (queueName != null) {
      clientConf.setQueueName(queueName);
    }
    final JobConf jc = new JobConf(clientConf);
    if (userInfo != null) {
      String[] splits = userInfo.split(",");
      String[] groups = new String[splits.length - 1];
      System.arraycopy(splits, 1, groups, 0, splits.length - 1);
      ugi = UserGroupInformation.createUserForTesting(splits[0], groups);
    } else {
      ugi = UserGroupInformation.getCurrentUser();
    }
    RunningJob rJob = ugi.doAs(new PrivilegedExceptionAction<RunningJob>() {
      public RunningJob run() throws IOException {
        if (shouldComplete) {
          return JobClient.runJob(jc);  
        } else {
          // Job should be submitted as 'userInfo'. So both the client as well as
          // the configuration should point to the same UGI.
          return new JobClient(jc).submitJob(jc);
        }
      }
    });
    return rJob;
  }

}
