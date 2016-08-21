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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
import org.junit.Test;

public class TestJob {
  @Test
  public void testJobToString() throws IOException, InterruptedException {
    Cluster cluster = mock(Cluster.class);
    ClientProtocol client = mock(ClientProtocol.class);
    when(cluster.getClient()).thenReturn(client);
    JobID jobid = new JobID("1014873536921", 6);
    JobStatus status = new JobStatus(jobid, 0.0f, 0.0f, 0.0f, 0.0f,
        State.FAILED, JobPriority.DEFAULT, "root", "TestJobToString",
        "job file", "tracking url");
    when(client.getJobStatus(jobid)).thenReturn(status);
    when(client.getTaskReports(jobid, TaskType.MAP)).thenReturn(
        new TaskReport[0]);
    when(client.getTaskReports(jobid, TaskType.REDUCE)).thenReturn(
        new TaskReport[0]);
    when(client.getTaskCompletionEvents(jobid, 0, 10)).thenReturn(
        new TaskCompletionEvent[0]);
    Job job = Job.getInstance(cluster, status, new JobConf());
    Assert.assertNotNull(job.toString());
  }

  @Test
  public void testUnexpectedJobStatus() throws Exception {
    Cluster cluster = mock(Cluster.class);
    JobID jobid = new JobID("1014873536921", 6);
    ClientProtocol clientProtocol = mock(ClientProtocol.class);
    when(cluster.getClient()).thenReturn(clientProtocol);
    JobStatus status = new JobStatus(jobid, 0f, 0f, 0f, 0f,
        State.RUNNING, JobPriority.DEFAULT, "root",
        "testUnexpectedJobStatus", "job file", "tracking URL");
    when(clientProtocol.getJobStatus(jobid)).thenReturn(status);
    Job job = Job.getInstance(cluster, status, new JobConf());

    // ensurer job status is RUNNING
    Assert.assertNotNull(job.getStatus());
    Assert.assertTrue(job.getStatus().getState() == State.RUNNING);

    // when updating job status, job client could not retrieve
    // job status, and status reset to null
    when(clientProtocol.getJobStatus(jobid)).thenReturn(null);

    try {
      job.updateStatus();
    } catch (IOException e) {
      Assert.assertTrue(e != null
          && e.getMessage().contains("Job status not available"));
    }

    try {
      ControlledJob cj = new ControlledJob(job, null);
      Assert.assertNotNull(cj.toString());
    } catch (NullPointerException e) {
      Assert.fail("job API fails with NPE");
    }
  }

  @Test
  public void testUGICredentialsPropogation() throws Exception {
    Credentials creds = new Credentials();
    Token<?> token = mock(Token.class);
    Text tokenService = new Text("service");
    Text secretName = new Text("secret");
    byte secret[] = new byte[]{};
        
    creds.addToken(tokenService,  token);
    creds.addSecretKey(secretName, secret);
    UserGroupInformation.getLoginUser().addCredentials(creds);
    
    JobConf jobConf = new JobConf();
    Job job = new Job(jobConf);

    assertSame(token, job.getCredentials().getToken(tokenService));
    assertSame(secret, job.getCredentials().getSecretKey(secretName));
  }
}
