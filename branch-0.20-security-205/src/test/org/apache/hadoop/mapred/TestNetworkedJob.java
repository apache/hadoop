/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;
import static org.mockito.Mockito.*;

public class TestNetworkedJob {

  @Test(expected=IOException.class)
  public void testNullStatus() throws Exception {
    JobProfile mockProf = mock(JobProfile.class);
    JobSubmissionProtocol mockClient = mock(JobSubmissionProtocol.class);
    new JobClient.NetworkedJob(null, mockProf, mockClient);
  }
  
  @Test(expected=IOException.class)
  public void testNullProfile() throws Exception {
    JobStatus mockStatus = mock(JobStatus.class);
    JobSubmissionProtocol mockClient = mock(JobSubmissionProtocol.class);
    new JobClient.NetworkedJob(mockStatus, null, mockClient);
  }

  @Test(expected=IOException.class)
  public void testNullClient() throws Exception {
    JobStatus mockStatus = mock(JobStatus.class);
    JobProfile mockProf = mock(JobProfile.class);
    new JobClient.NetworkedJob(mockStatus, mockProf, null);
  }
  

  @SuppressWarnings("deprecation")
  @Test
  public void testBadUpdate() throws Exception {
    JobStatus mockStatus = mock(JobStatus.class);
    JobProfile mockProf = mock(JobProfile.class);
    JobSubmissionProtocol mockClient = mock(JobSubmissionProtocol.class);
    
    JobID id = new JobID("test",0);
    
    RunningJob rj = new JobClient.NetworkedJob(mockStatus, mockProf, mockClient);
    
    when(mockProf.getJobID()).thenReturn(id);
    when(mockClient.getJobStatus(id)).thenReturn(null);
    
    boolean caught = false;
    try {
      rj.isSuccessful();
    } catch(IOException e) {
      caught = true;
    }
    assertTrue("Expected updateStatus to throw an IOException bt it did not", caught);
    
    //verification
    verify(mockProf).getJobID();
    verify(mockClient).getJobStatus(id);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testGetNullCounters() throws Exception {
    JobStatus mockStatus = mock(JobStatus.class);
    JobProfile mockProf = mock(JobProfile.class);
    JobSubmissionProtocol mockClient = mock(JobSubmissionProtocol.class);
    RunningJob underTest =
      new JobClient.NetworkedJob(mockStatus, mockProf, mockClient); 

    JobID id = new JobID("test", 0);
    when(mockProf.getJobID()).thenReturn(id);
    when(mockClient.getJobCounters(id)).thenReturn(null);
    assertNull(underTest.getCounters());
    //verification
    verify(mockClient).getJobCounters(id);
  }

}
