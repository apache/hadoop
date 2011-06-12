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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.mapreduce.Cluster;
import org.junit.Test;

public class TestJobClient {
  
  @SuppressWarnings("deprecation")
  @Test
  public void testMapTaskReportsWithNullJob() throws Exception {
    JobClient client = new JobClient();
    Cluster mockCluster = mock(Cluster.class);
    client.cluster = mockCluster;
    JobID id = new JobID("test",0);
    
    when(mockCluster.getJob(id)).thenReturn(null);
    
    TaskReport[] result = client.getMapTaskReports(id);
    assertEquals(0, result.length);
    
    verify(mockCluster).getJob(id);
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void testReduceTaskReportsWithNullJob() throws Exception {
    JobClient client = new JobClient();
    Cluster mockCluster = mock(Cluster.class);
    client.cluster = mockCluster;
    JobID id = new JobID("test",0);
    
    when(mockCluster.getJob(id)).thenReturn(null);
    
    TaskReport[] result = client.getReduceTaskReports(id);
    assertEquals(0, result.length);
    
    verify(mockCluster).getJob(id);
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void testSetupTaskReportsWithNullJob() throws Exception {
    JobClient client = new JobClient();
    Cluster mockCluster = mock(Cluster.class);
    client.cluster = mockCluster;
    JobID id = new JobID("test",0);
    
    when(mockCluster.getJob(id)).thenReturn(null);
    
    TaskReport[] result = client.getSetupTaskReports(id);
    assertEquals(0, result.length);
    
    verify(mockCluster).getJob(id);
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void testCleanupTaskReportsWithNullJob() throws Exception {
    JobClient client = new JobClient();
    Cluster mockCluster = mock(Cluster.class);
    client.cluster = mockCluster;
    JobID id = new JobID("test",0);
    
    when(mockCluster.getJob(id)).thenReturn(null);
    
    TaskReport[] result = client.getCleanupTaskReports(id);
    assertEquals(0, result.length);
    
    verify(mockCluster).getJob(id);
  }
}
