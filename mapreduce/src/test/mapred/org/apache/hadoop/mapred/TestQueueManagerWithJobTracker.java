/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import static org.apache.hadoop.mapred.QueueConfigurationParser.NAME_SEPARATOR;
import static org.apache.hadoop.mapred.QueueManagerTestUtils.CONFIG;
import static org.apache.hadoop.mapred.QueueManagerTestUtils.checkForConfigFile;
import static org.apache.hadoop.mapred.QueueManagerTestUtils.createAcls;
import static org.apache.hadoop.mapred.QueueManagerTestUtils.createDocument;
import static org.apache.hadoop.mapred.QueueManagerTestUtils.createProperties;
import static org.apache.hadoop.mapred.QueueManagerTestUtils.createQueue;
import static org.apache.hadoop.mapred.QueueManagerTestUtils.createQueuesNode;
import static org.apache.hadoop.mapred.QueueManagerTestUtils.createSimpleDocumentWithAcls;
import static org.apache.hadoop.mapred.QueueManagerTestUtils.createState;
import static org.apache.hadoop.mapred.QueueManagerTestUtils.miniMRCluster;
import static org.apache.hadoop.mapred.QueueManagerTestUtils.setUpCluster;
import static org.apache.hadoop.mapred.QueueManagerTestUtils.submitSleepJob;
import static org.apache.hadoop.mapred.QueueManagerTestUtils.writeToFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.tools.MRAdmin;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.QueueState;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class TestQueueManagerWithJobTracker {

  private static Configuration conf;

  @BeforeClass
  public static void setUp() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    createSimpleDocumentWithAcls(doc, "true");
    writeToFile(doc, CONFIG);
    conf = new Configuration();
    conf.addResource(CONFIG);
    conf.set("mapred.committer.job.setup.cleanup.needed", "false");
    setUpCluster(conf);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    new File(CONFIG).delete();
  }

  /**
   * Test to check that jobs cannot be submitted to a queue in STOPPED state
   * @throws Exception
   */
  @Test(expected = IOException.class)
  public void testSubmitJobForStoppedQueue() throws Exception {
    submitSleepJob(10, 10, 100, 100, false, null,
        "p1" + NAME_SEPARATOR + "p14", conf);
    fail("queue p1:p14 is in stopped state and should not accept jobs");
  }

  /**
   * Test to check that jobs cannot be submitted to a container queue
   * @throws Exception
   */
  @Test(expected = IOException.class)
  public void testSubmitJobForContainerQueue() throws Exception {
      submitSleepJob(10, 10, 100, 100, false, null, "p1", conf);
      fail("queue p1 is a container queue and cannot have jobs");
  }

  /**
   * Tests the submission of job with specified acls
   * @throws Exception
   */
  @Test
  public void testAclsForSubmitJob() throws Exception {
    Job job;
    // submit job to queue p1:p13 with unspecified acls 
    job = submitSleepJob(0, 0, 0, 0, true, "u1,g1", "p1" + NAME_SEPARATOR
        + "p13", conf);
    assertTrue("Job submission for u1 failed in queue : p1:p13.",
        job.isSuccessful());
    // check for access to submit the job
    try {
      job = submitSleepJob(0, 0, 0, 0, false, "u2,g1", "p1" + NAME_SEPARATOR
          + "p11", conf);
      fail("user u2 cannot submit jobs to queue p1:p11");
    } catch (Exception e) {
    }
    // submit job to queue p1:p11 with acls-submit-job as u1
    job = submitSleepJob(0, 0, 0, 0, true, "u1,g1", "p1"
        + NAME_SEPARATOR + "p11", conf);
    assertTrue("Job submission for u1 failed in queue : p1:p11.",
        job.isSuccessful());
  }

  /**
   * Tests the accessibility to kill a job
   * @throws Exception
   */
  @Test
  public void testAccessToKillJob() throws Exception {
    Job job = submitSleepJob(1, 1, 100, 100, false, "u1,g1", "p1"
        + NAME_SEPARATOR + "p11", conf);
    final JobConf jobConf = miniMRCluster.createJobConf();
    Cluster cluster = null;
    JobID jobID = job.getStatus().getJobID();
    //Ensure that the jobinprogress is initied before we issue a kill 
    //signal to the job.
    JobTracker tracker = miniMRCluster.getJobTrackerRunner().getJobTracker();
    JobInProgress jip = tracker.getJob(org.apache.hadoop.mapred.JobID
        .downgrade(jobID));
    tracker.initJob(jip);
    try {
      final Configuration userConf =
          new Configuration(miniMRCluster.createJobConf());
      UserGroupInformation ugi =
          UserGroupInformation.createUserForTesting("someRandomUser",
              new String[] { "someRandomGroup" });
      cluster = ugi.doAs(new PrivilegedExceptionAction<Cluster>() {
        public Cluster run() throws IOException {
          return new Cluster(userConf);
        }
      });
      cluster.getJob(jobID).killJob();
      fail("user 'someRandomeUser' is neither u1 nor in the administer group list");
    } catch (Exception e) {
      final Configuration userConf = new Configuration(miniMRCluster.createJobConf());
      UserGroupInformation ugi = 
        UserGroupInformation.createUserForTesting("u1",new String[]{"g1"});
      cluster = ugi.doAs(new PrivilegedExceptionAction<Cluster>() {
        public Cluster run() throws IOException {
          return new Cluster(userConf);
        }
      });
      cluster.getJob(jobID).killJob();
      // kill the running job
      assertEquals("job submitted for u1 and queue p1:p11 is not killed.",
          cluster.getJob(jobID).getStatus().getState(), (State.KILLED));
    }
    
    job = submitSleepJob(1, 1, 100, 100, false, "u1,g1", "p1" + NAME_SEPARATOR
        + "p12", conf);
    jobID = job.getStatus().getJobID();
    //Ensure that the jobinprogress is initied before we issue a kill 
    //signal to the job.
    jip =  tracker.getJob(org.apache.hadoop.mapred.JobID.downgrade(jobID));
    tracker.initJob(jip);
    tracker.killJob(job.getJobID());
    // kill the job by the user who submitted the job
    assertEquals("job submitted for u1 and queue p1:p11 is not killed.",
        cluster.getJob(jobID).getStatus().getState(), (State.KILLED));
    
    final Configuration userConf = new Configuration(miniMRCluster.createJobConf());
    UserGroupInformation ugi = 
      UserGroupInformation.createUserForTesting("u1",new String[]{"g1"});
    cluster = ugi.doAs(new PrivilegedExceptionAction<Cluster>() {
      public Cluster run() throws IOException {
        return new Cluster(userConf);
      }
    });
    job = submitSleepJob(1, 1, 10, 10, false, "u1,g1", "p1" + NAME_SEPARATOR
        + "p11", conf);
    jobID = job.getStatus().getJobID();
    //Ensure that the jobinprogress is initied before we issue a kill 
    //signal to the job.
    jip =  tracker.getJob(org.apache.hadoop.mapred.JobID.downgrade(jobID));
    tracker.initJob(jip);
    ugi = 
      UserGroupInformation.createUserForTesting("u3",new String[]{"g3"});
    cluster = ugi.doAs(new PrivilegedExceptionAction<Cluster>() {
      public Cluster run() throws IOException {
        return new Cluster(jobConf);
      }
    });
    // try killing job with user not in administer list
    try {
      cluster.getJob(jobID).killJob();
      fail("u3 not in administer list");
    } catch (Exception e) {
      ugi = 
        UserGroupInformation.createUserForTesting("u1",new String[]{"g1"});
      cluster = ugi.doAs(new PrivilegedExceptionAction<Cluster>() {
        public Cluster run() throws IOException {
          return new Cluster(jobConf);
        }
      });
      assertFalse(cluster.getJob(jobID).isComplete());
      cluster.getJob(jobID).killJob();
      // kill the running job
      assertEquals("job submitted for u1 and queue p1:p11 is not killed.",
          cluster.getJob(jobID).getStatus().getState(), (State.KILLED));
    }
  }

  /**
   * Tests job submission after refresh
   * @throws Exception
   */
  @Test
  public void testSubmitJobsAfterRefresh() throws Exception {
    // test for refresh
    checkForConfigFile();
    Document doc = createDocument();
    refreshDocument(doc);
    writeToFile(doc, CONFIG);
    MRAdmin admin = new MRAdmin(miniMRCluster.createJobConf());
    admin.run(new String[] { "-refreshQueues" });
    try {
      submitSleepJob(10, 10, 100, 100, false, "u1,g1", "p1"
          + NAME_SEPARATOR + "p11", conf);
      fail("user u1 is not in the submit jobs' list");
    } catch (Exception e) {
    }
    checkForConfigFile();
    doc = createDocument();
    createSimpleDocumentWithAcls(doc, "true");
    writeToFile(doc, CONFIG);
    admin.run(new String[] { "-refreshQueues" });
  }

  private void refreshDocument(Document doc) {
    Element queues = createQueuesNode(doc, "true");

    // Create parent level queue q1.
    Element q1 = createQueue(doc, "q1");
    Properties props = new Properties();
    props.setProperty("capacity", "10");
    props.setProperty("maxCapacity", "35");
    q1.appendChild(createProperties(doc, props));
    queues.appendChild(q1);

    // Create another parent level p1
    Element p1 = createQueue(doc, "p1");

    // append child p11 to p1
    Element p11 = createQueue(doc, "p11");
    p11.appendChild(createAcls(doc,
        QueueConfigurationParser.ACL_SUBMIT_JOB_TAG, " "));
    p11.appendChild(createAcls(doc,
        QueueConfigurationParser.ACL_ADMINISTER_JOB_TAG, "u2"));
    p1.appendChild(p11);

    Element p12 = createQueue(doc, "p12");

    p12.appendChild(createState(doc, QueueState.RUNNING.getStateName()));
    p12.appendChild(createAcls(doc,
        QueueConfigurationParser.ACL_SUBMIT_JOB_TAG, "*"));
    p12.appendChild(createAcls(doc,
        QueueConfigurationParser.ACL_ADMINISTER_JOB_TAG, "*"));

    // append p12 to p1.
    p1.appendChild(p12);
    // append child p13 to p1
    Element p13 = createQueue(doc, "p13");
    p13.appendChild(createState(doc, QueueState.RUNNING.getStateName()));
    p1.appendChild(p13);
    // append child p14 to p1
    Element p14 = createQueue(doc, "p14");
    p14.appendChild(createState(doc, QueueState.STOPPED.getStateName()));
    p1.appendChild(p14);
    queues.appendChild(p1);
  }

  /** 
   * Tests job submission when acls are disabled
   * @throws Exception
   */
  @Test
  public void testAclsDisabled() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    createSimpleDocumentWithAcls(doc, "false");
    writeToFile(doc, CONFIG);
    MRAdmin admin = new MRAdmin(miniMRCluster.createJobConf());
    admin.run(new String[] { "-refreshQueues" });

    // submit job to queue p1:p11 by any user not in acls-submit-job
    Job job = submitSleepJob(0, 0, 0, 0, true, "u2,g1", "p1" + NAME_SEPARATOR
        + "p11", conf);
    assertTrue("Job submitted for u2 in queue p1:p11 is not successful.",
        job.isSuccessful());
    
    // submit job to queue p1:p11 by user in acls-submit-job
    job = submitSleepJob(0, 0, 0, 0, true, "u1,g1", "p1" + NAME_SEPARATOR
        + "p11", conf);
    assertTrue("Job submitted for u2 in queue p1:p11 is not successful.",
        job.isSuccessful());

    job = submitSleepJob(1, 1, 0, 0, false, "u1,g1", "p1" + NAME_SEPARATOR
        + "p11", conf);
    // kill the job by any user    
    final JobConf jobConf = miniMRCluster.createJobConf();
    UserGroupInformation ugi = 
      UserGroupInformation.createUserForTesting("u3",new String[]{"g3"});
    Cluster cluster = ugi.doAs(new PrivilegedExceptionAction<Cluster>() {
      public Cluster run() throws IOException {
        return new Cluster(jobConf);
      }
    });
    JobID jobID = job.getStatus().getJobID();
    //Ensure that the jobinprogress is initied before we issue a kill 
    //signal to the job.
    JobInProgress jip = miniMRCluster.getJobTrackerRunner().getJobTracker()
        .getJob(org.apache.hadoop.mapred.JobID.downgrade(jobID));
    miniMRCluster.getJobTrackerRunner().getJobTracker().initJob(jip);
    cluster.getJob(jobID).killJob();
    assertEquals("job submitted for u1 and queue p1:p11 is not killed.",
        cluster.getJob(jobID).getStatus().getState(), (State.KILLED));
  }
}
