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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TaskReport;
import org.junit.Test;

public class JobClientUnitTest {
  
  public class TestJobClient extends JobClient {

    TestJobClient(JobConf jobConf) throws IOException {
      super(jobConf);
    }

    void setCluster(Cluster cluster) {
      this.cluster = cluster;
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testMapTaskReportsWithNullJob() throws Exception {
    TestJobClient client = new TestJobClient(new JobConf());
    Cluster mockCluster = mock(Cluster.class);
    client.setCluster(mockCluster);
    JobID id = new JobID("test",0);
    
    when(mockCluster.getJob(id)).thenReturn(null);
    
    TaskReport[] result = client.getMapTaskReports(id);
    assertEquals(0, result.length);
    
    verify(mockCluster).getJob(id);
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void testReduceTaskReportsWithNullJob() throws Exception {
    TestJobClient client = new TestJobClient(new JobConf());
    Cluster mockCluster = mock(Cluster.class);
    client.setCluster(mockCluster);
    JobID id = new JobID("test",0);
    
    when(mockCluster.getJob(id)).thenReturn(null);
    
    TaskReport[] result = client.getReduceTaskReports(id);
    assertEquals(0, result.length);
    
    verify(mockCluster).getJob(id);
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void testSetupTaskReportsWithNullJob() throws Exception {
    TestJobClient client = new TestJobClient(new JobConf());
    Cluster mockCluster = mock(Cluster.class);
    client.setCluster(mockCluster);
    JobID id = new JobID("test",0);
    
    when(mockCluster.getJob(id)).thenReturn(null);
    
    TaskReport[] result = client.getSetupTaskReports(id);
    assertEquals(0, result.length);
    
    verify(mockCluster).getJob(id);
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void testCleanupTaskReportsWithNullJob() throws Exception {
    TestJobClient client = new TestJobClient(new JobConf());
    Cluster mockCluster = mock(Cluster.class);
    client.setCluster(mockCluster);
    JobID id = new JobID("test",0);
    
    when(mockCluster.getJob(id)).thenReturn(null);
    
    TaskReport[] result = client.getCleanupTaskReports(id);
    assertEquals(0, result.length);
    
    verify(mockCluster).getJob(id);
  }

  @Test
  public void testShowJob() throws Exception {
    TestJobClient client = new TestJobClient(new JobConf());
    JobID jobID = new JobID("test", 0);

    JobStatus mockJobStatus = mock(JobStatus.class);
    when(mockJobStatus.getJobID()).thenReturn(jobID);
    when(mockJobStatus.getState()).thenReturn(JobStatus.State.RUNNING);
    when(mockJobStatus.getStartTime()).thenReturn(0L);
    when(mockJobStatus.getUsername()).thenReturn("mockuser");
    when(mockJobStatus.getQueue()).thenReturn("mockqueue");
    when(mockJobStatus.getPriority()).thenReturn(JobPriority.NORMAL);
    when(mockJobStatus.getNumUsedSlots()).thenReturn(1);
    when(mockJobStatus.getNumReservedSlots()).thenReturn(1);
    when(mockJobStatus.getUsedMem()).thenReturn(1024);
    when(mockJobStatus.getReservedMem()).thenReturn(512);
    when(mockJobStatus.getNeededMem()).thenReturn(2048);
    when(mockJobStatus.getSchedulingInfo()).thenReturn("NA");

    Job mockJob = mock(Job.class);
    when(mockJob.getTaskReports(isA(TaskType.class))).thenReturn(new TaskReport[0]);

    Cluster mockCluster = mock(Cluster.class);
    when(mockCluster.getJob(jobID)).thenReturn(mockJob);

    client.setCluster(mockCluster);
    
    
    client.displayJobList(new JobStatus[] {mockJobStatus});
    verify(mockJobStatus, atLeastOnce()).getJobID();
    verify(mockJob, atLeastOnce()).getTaskReports(isA(TaskType.class));
    verify(mockCluster, atLeastOnce()).getJob(jobID);
    verify(mockJobStatus).getState();
    verify(mockJobStatus).getStartTime();
    verify(mockJobStatus).getUsername();
    verify(mockJobStatus).getQueue();
    verify(mockJobStatus).getPriority();
    verify(mockJobStatus).getNumUsedSlots();
    verify(mockJobStatus).getNumReservedSlots();
    verify(mockJobStatus).getUsedMem();
    verify(mockJobStatus).getReservedMem();
    verify(mockJobStatus).getNeededMem();
    verify(mockJobStatus).getSchedulingInfo();
  }
}
