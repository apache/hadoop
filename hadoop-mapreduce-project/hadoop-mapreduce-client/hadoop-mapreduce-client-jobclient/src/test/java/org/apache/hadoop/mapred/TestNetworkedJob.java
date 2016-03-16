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
      assertEquals(jobId, runningJob.getID());
      assertEquals(jobId.toString(), runningJob.getJobID());
      assertEquals("N/A", runningJob.getJobName());
      assertTrue(runningJob.getJobFile().endsWith(
          ".staging/" + runningJob.getJobID() + "/job.xml"));
      assertTrue(runningJob.getTrackingURL().length() > 0);
      assertTrue(runningJob.mapProgress() == 0.0f);
      assertTrue(runningJob.reduceProgress() == 0.0f);
      assertTrue(runningJob.cleanupProgress() == 0.0f);
      assertTrue(runningJob.setupProgress() == 0.0f);

      TaskCompletionEvent[] tce = runningJob.getTaskCompletionEvents(0);
      assertEquals(tce.length, 0);

      assertEquals("", runningJob.getHistoryUrl());
      assertFalse(runningJob.isRetired());
      assertEquals("", runningJob.getFailureInfo());
      assertEquals("N/A", runningJob.getJobStatus().getJobName());
      assertEquals(0, client.getMapTaskReports(jobId).length);
      
      try {
        client.getSetupTaskReports(jobId);
      } catch (YarnRuntimeException e) {
        assertEquals("Unrecognized task type: JOB_SETUP", e.getMessage());
      }
      try {
        client.getCleanupTaskReports(jobId);
      } catch (YarnRuntimeException e) {
        assertEquals("Unrecognized task type: JOB_CLEANUP", e.getMessage());
      }
      assertEquals(0, client.getReduceTaskReports(jobId).length);
      // test ClusterStatus
      ClusterStatus status = client.getClusterStatus(true);
      assertEquals(2, status.getActiveTrackerNames().size());
      // it method does not implemented and always return empty array or null;
      assertEquals(0, status.getBlacklistedTrackers());
      assertEquals(0, status.getBlacklistedTrackerNames().size());
      assertEquals(0, status.getBlackListedTrackersInfo().size());
      assertEquals(JobTrackerStatus.RUNNING, status.getJobTrackerStatus());
      assertEquals(1, status.getMapTasks());
      assertEquals(20, status.getMaxMapTasks());
      assertEquals(4, status.getMaxReduceTasks());
      assertEquals(0, status.getNumExcludedNodes());
      assertEquals(1, status.getReduceTasks());
      assertEquals(2, status.getTaskTrackers());
      assertEquals(0, status.getTTExpiryInterval());
      assertEquals(JobTrackerStatus.RUNNING, status.getJobTrackerStatus());
      assertEquals(0, status.getGraylistedTrackers());

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

      // test taskStatusfilter
      JobClient.setTaskOutputFilter(job, TaskStatusFilter.ALL);
      assertEquals(TaskStatusFilter.ALL, JobClient.getTaskOutputFilter(job));

      // runningJob.setJobPriority(JobPriority.HIGH.name());

      // test default map
      assertEquals(20, client.getDefaultMaps());
      assertEquals(4, client.getDefaultReduces());
      assertEquals("jobSubmitDir", client.getSystemDir().getName());
      // test queue information
      JobQueueInfo[] rootQueueInfo = client.getRootQueues();
      assertEquals(1, rootQueueInfo.length);
      assertEquals("default", rootQueueInfo[0].getQueueName());
      JobQueueInfo[] qinfo = client.getQueues();
      assertEquals(1, qinfo.length);
      assertEquals("default", qinfo[0].getQueueName());
      assertEquals(0, client.getChildQueues("default").length);
      assertEquals(1, client.getJobsFromQueue("default").length);
      assertTrue(client.getJobsFromQueue("default")[0].getJobFile().endsWith(
          "/job.xml"));

      JobQueueInfo qi = client.getQueueInfo("default");
      assertEquals("default", qi.getQueueName());
      assertEquals("running", qi.getQueueState());

      QueueAclsInfo[] aai = client.getQueueAclsForCurrentUser();
      assertEquals(2, aai.length);
      assertEquals("root", aai[0].getQueueName());
      assertEquals("default", aai[1].getQueueName());
      
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
    assertEquals(info, info2);
    assertEquals(info.toString(), info2.toString());
    assertEquals("trackerName", info2.getTrackerName());
    assertEquals("reasonForBlackListing", info2.getReasonForBlackListing());
    assertEquals("blackListInfo", info2.getBlackListReport());
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
