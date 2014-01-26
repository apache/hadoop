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

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus.BlackListInfo;
import org.apache.hadoop.mapred.JobClient.NetworkedJob;
import org.apache.hadoop.mapred.JobClient.TaskStatusFilter;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.junit.Test;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

public class TestNetworkedJob {
  private static String TEST_ROOT_DIR = new File(System.getProperty(
      "test.build.data", "/tmp")).toURI().toString().replace(' ', '+');
  private static Path testDir = new Path(TEST_ROOT_DIR + "/test_mini_mr_local");
  private static Path inFile = new Path(testDir, "in");
  private static Path outDir = new Path(testDir, "out");

  @Test (timeout=5000)
  public void testGetNullCounters() throws Exception {
    //mock creation
    Job mockJob = mock(Job.class);
    RunningJob underTest = new JobClient.NetworkedJob(mockJob); 

    when(mockJob.getCounters()).thenReturn(null);
    assertNull(underTest.getCounters());
    //verification
    verify(mockJob).getCounters();
  }
  
  @Test (timeout=500000)
  public void testGetJobStatus() throws IOException, InterruptedException,
      ClassNotFoundException {
    MiniMRClientCluster mr = null;
    FileSystem fileSys = null;

    try {
      mr = createMiniClusterWithCapacityScheduler();

      JobConf job = new JobConf(mr.getConfig());

      fileSys = FileSystem.get(job);
      fileSys.delete(testDir, true);
      FSDataOutputStream out = fileSys.create(inFile, true);
      out.writeBytes("This is a test file");
      out.close();

      FileInputFormat.setInputPaths(job, inFile);
      FileOutputFormat.setOutputPath(job, outDir);

      job.setInputFormat(TextInputFormat.class);
      job.setOutputFormat(TextOutputFormat.class);

      job.setMapperClass(IdentityMapper.class);
      job.setReducerClass(IdentityReducer.class);
      job.setNumReduceTasks(0);

      JobClient client = new JobClient(mr.getConfig());
      RunningJob rj = client.submitJob(job);
      JobID jobId = rj.getID();

      // The following asserts read JobStatus twice and ensure the returned
      // JobStatus objects correspond to the same Job.
      assertEquals("Expected matching JobIDs", jobId, client.getJob(jobId)
          .getJobStatus().getJobID());
      assertEquals("Expected matching startTimes", rj.getJobStatus()
          .getStartTime(), client.getJob(jobId).getJobStatus()
          .getStartTime());
    } finally {
      if (fileSys != null) {
        fileSys.delete(testDir, true);
      }
      if (mr != null) {
        mr.stop();
      }
    }
  }
/**
 * test JobConf 
 * @throws Exception
 */
  @SuppressWarnings( "deprecation" )
  @Test (timeout=500000)
  public void testNetworkedJob() throws Exception {
    // mock creation
    MiniMRClientCluster mr = null;
    FileSystem fileSys = null;

    try {
      mr = createMiniClusterWithCapacityScheduler();

      JobConf job = new JobConf(mr.getConfig());

      fileSys = FileSystem.get(job);
      fileSys.delete(testDir, true);
      FSDataOutputStream out = fileSys.create(inFile, true);
      out.writeBytes("This is a test file");
      out.close();

      FileInputFormat.setInputPaths(job, inFile);
      FileOutputFormat.setOutputPath(job, outDir);

      job.setInputFormat(TextInputFormat.class);
      job.setOutputFormat(TextOutputFormat.class);

      job.setMapperClass(IdentityMapper.class);
      job.setReducerClass(IdentityReducer.class);
      job.setNumReduceTasks(0);

      JobClient client = new JobClient(mr.getConfig());

      RunningJob rj = client.submitJob(job);
      JobID jobId = rj.getID();
      NetworkedJob runningJob = (NetworkedJob) client.getJob(jobId);
      runningJob.setJobPriority(JobPriority.HIGH.name());
      // test getters
      assertTrue(runningJob.getConfiguration().toString()
          .endsWith("0001/job.xml"));
      assertEquals(runningJob.getID(), jobId);
      assertEquals(runningJob.getJobID(), jobId.toString());
      assertEquals(runningJob.getJobName(), "N/A");
      assertTrue(runningJob.getJobFile().endsWith(
          ".staging/" + runningJob.getJobID() + "/job.xml"));
      assertTrue(runningJob.getTrackingURL().length() > 0);
      assertTrue(runningJob.mapProgress() == 0.0f);
      assertTrue(runningJob.reduceProgress() == 0.0f);
      assertTrue(runningJob.cleanupProgress() == 0.0f);
      assertTrue(runningJob.setupProgress() == 0.0f);

      TaskCompletionEvent[] tce = runningJob.getTaskCompletionEvents(0);
      assertEquals(tce.length, 0);

      assertEquals(runningJob.getHistoryUrl(),"");
      assertFalse(runningJob.isRetired());
      assertEquals( runningJob.getFailureInfo(),"");
      assertEquals(runningJob.getJobStatus().getJobName(), "N/A");
      assertEquals(client.getMapTaskReports(jobId).length, 0);
      
      try {
        client.getSetupTaskReports(jobId);
      } catch (YarnRuntimeException e) {
        assertEquals(e.getMessage(), "Unrecognized task type: JOB_SETUP");
      }
      try {
        client.getCleanupTaskReports(jobId);
      } catch (YarnRuntimeException e) {
        assertEquals(e.getMessage(), "Unrecognized task type: JOB_CLEANUP");
      }
      assertEquals(client.getReduceTaskReports(jobId).length, 0);
      // test ClusterStatus
      ClusterStatus status = client.getClusterStatus(true);
      assertEquals(status.getActiveTrackerNames().size(), 2);
      // it method does not implemented and always return empty array or null;
      assertEquals(status.getBlacklistedTrackers(), 0);
      assertEquals(status.getBlacklistedTrackerNames().size(), 0);
      assertEquals(status.getBlackListedTrackersInfo().size(), 0);
      assertEquals(status.getJobTrackerStatus(), JobTrackerStatus.RUNNING);
      assertEquals(status.getMapTasks(), 1);
      assertEquals(status.getMaxMapTasks(), 20);
      assertEquals(status.getMaxReduceTasks(), 4);
      assertEquals(status.getNumExcludedNodes(), 0);
      assertEquals(status.getReduceTasks(), 1);
      assertEquals(status.getTaskTrackers(), 2);
      assertEquals(status.getTTExpiryInterval(), 0);
      assertEquals(status.getJobTrackerStatus(), JobTrackerStatus.RUNNING);
      assertEquals(status.getGraylistedTrackers(), 0);

      // test read and write
      ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
      status.write(new DataOutputStream(dataOut));
      ClusterStatus status2 = new ClusterStatus();

      status2.readFields(new DataInputStream(new ByteArrayInputStream(dataOut
          .toByteArray())));
      assertEquals(status.getActiveTrackerNames(),
          status2.getActiveTrackerNames());
      assertEquals(status.getBlackListedTrackersInfo(),
          status2.getBlackListedTrackersInfo());
      assertEquals(status.getMapTasks(), status2.getMapTasks());

      try {
      } catch (RuntimeException e) {
        assertTrue(e.getMessage().endsWith("not found on CLASSPATH"));
      }

      // test taskStatusfilter
      JobClient.setTaskOutputFilter(job, TaskStatusFilter.ALL);
      assertEquals(JobClient.getTaskOutputFilter(job), TaskStatusFilter.ALL);

      // runningJob.setJobPriority(JobPriority.HIGH.name());

      // test default map
      assertEquals(client.getDefaultMaps(), 20);
      assertEquals(client.getDefaultReduces(), 4);
      assertEquals(client.getSystemDir().getName(), "jobSubmitDir");
      // test queue information
      JobQueueInfo[] rootQueueInfo = client.getRootQueues();
      assertEquals(rootQueueInfo.length, 1);
      assertEquals(rootQueueInfo[0].getQueueName(), "default");
      JobQueueInfo[] qinfo = client.getQueues();
      assertEquals(qinfo.length, 1);
      assertEquals(qinfo[0].getQueueName(), "default");
      assertEquals(client.getChildQueues("default").length, 0);
      assertEquals(client.getJobsFromQueue("default").length, 1);
      assertTrue(client.getJobsFromQueue("default")[0].getJobFile().endsWith(
          "/job.xml"));

      JobQueueInfo qi = client.getQueueInfo("default");
      assertEquals(qi.getQueueName(), "default");
      assertEquals(qi.getQueueState(), "running");

      QueueAclsInfo[] aai = client.getQueueAclsForCurrentUser();
      assertEquals(aai.length, 2);
      assertEquals(aai[0].getQueueName(), "root");
      assertEquals(aai[1].getQueueName(), "default");
      // test token
      Token<DelegationTokenIdentifier> token = client
          .getDelegationToken(new Text(UserGroupInformation.getCurrentUser()
              .getShortUserName()));
      assertEquals(token.getKind().toString(), "RM_DELEGATION_TOKEN");
      
      // test JobClient
      
   
      // The following asserts read JobStatus twice and ensure the returned
      // JobStatus objects correspond to the same Job.
      assertEquals("Expected matching JobIDs", jobId, client.getJob(jobId)
          .getJobStatus().getJobID());
      assertEquals("Expected matching startTimes", rj.getJobStatus()
          .getStartTime(), client.getJob(jobId).getJobStatus().getStartTime());
    } finally {
      if (fileSys != null) {
        fileSys.delete(testDir, true);
      }
      if (mr != null) {
        mr.stop();
      }
    }
  }

  /**
   * test BlackListInfo class
   * 
   * @throws IOException
   */
  @Test (timeout=5000)
  public void testBlackListInfo() throws IOException {
    BlackListInfo info = new BlackListInfo();
    info.setBlackListReport("blackListInfo");
    info.setReasonForBlackListing("reasonForBlackListing");
    info.setTrackerName("trackerName");
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(byteOut);
    info.write(out);
    BlackListInfo info2 = new BlackListInfo();
    info2.readFields(new DataInputStream(new ByteArrayInputStream(byteOut
        .toByteArray())));
    assertEquals(info, info);
    assertEquals(info.toString(), info.toString());
    assertEquals(info.getTrackerName(), "trackerName");
    assertEquals(info.getReasonForBlackListing(), "reasonForBlackListing");
    assertEquals(info.getBlackListReport(), "blackListInfo");

  }
/**
 *  test run from command line JobQueueClient
 * @throws Exception
 */
  @Test (timeout=500000)
  public void testJobQueueClient() throws Exception {
        MiniMRClientCluster mr = null;
    FileSystem fileSys = null;
    PrintStream oldOut = System.out;
    try {
      mr = createMiniClusterWithCapacityScheduler();

      JobConf job = new JobConf(mr.getConfig());

      fileSys = FileSystem.get(job);
      fileSys.delete(testDir, true);
      FSDataOutputStream out = fileSys.create(inFile, true);
      out.writeBytes("This is a test file");
      out.close();

      FileInputFormat.setInputPaths(job, inFile);
      FileOutputFormat.setOutputPath(job, outDir);

      job.setInputFormat(TextInputFormat.class);
      job.setOutputFormat(TextOutputFormat.class);

      job.setMapperClass(IdentityMapper.class);
      job.setReducerClass(IdentityReducer.class);
      job.setNumReduceTasks(0);

      JobClient client = new JobClient(mr.getConfig());

      client.submitJob(job);

      JobQueueClient jobClient = new JobQueueClient(job);

      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      System.setOut(new PrintStream(bytes));
      String[] arg = { "-list" };
      jobClient.run(arg);
      assertTrue(bytes.toString().contains("Queue Name : default"));
      assertTrue(bytes.toString().contains("Queue State : running"));
      bytes = new ByteArrayOutputStream();
      System.setOut(new PrintStream(bytes));
      String[] arg1 = { "-showacls" };
      jobClient.run(arg1);
      assertTrue(bytes.toString().contains("Queue acls for user :"));
      assertTrue(bytes.toString().contains(
          "root  ADMINISTER_QUEUE,SUBMIT_APPLICATIONS"));
      assertTrue(bytes.toString().contains(
          "default  ADMINISTER_QUEUE,SUBMIT_APPLICATIONS"));

      // test for info and default queue

      bytes = new ByteArrayOutputStream();
      System.setOut(new PrintStream(bytes));
      String[] arg2 = { "-info", "default" };
      jobClient.run(arg2);
      assertTrue(bytes.toString().contains("Queue Name : default"));
      assertTrue(bytes.toString().contains("Queue State : running"));
      assertTrue(bytes.toString().contains("Scheduling Info"));

      // test for info , default queue and jobs
      bytes = new ByteArrayOutputStream();
      System.setOut(new PrintStream(bytes));
      String[] arg3 = { "-info", "default", "-showJobs" };
      jobClient.run(arg3);
      assertTrue(bytes.toString().contains("Queue Name : default"));
      assertTrue(bytes.toString().contains("Queue State : running"));
      assertTrue(bytes.toString().contains("Scheduling Info"));
      assertTrue(bytes.toString().contains("job_1"));

      String[] arg4 = {};
      jobClient.run(arg4);

      
    } finally {
      System.setOut(oldOut);
      if (fileSys != null) {
        fileSys.delete(testDir, true);
      }
      if (mr != null) {
        mr.stop();
      }
    }
  }
  
  private MiniMRClientCluster createMiniClusterWithCapacityScheduler()
      throws IOException {
    Configuration conf = new Configuration();
    // Expected queue names depending on Capacity Scheduler queue naming
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        CapacityScheduler.class);
    return MiniMRClientClusterFactory.create(this.getClass(), 2, conf);
  }
}
