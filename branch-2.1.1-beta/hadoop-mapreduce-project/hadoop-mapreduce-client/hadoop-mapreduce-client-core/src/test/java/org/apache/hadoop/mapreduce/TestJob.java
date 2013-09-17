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
        State.FAILED, JobPriority.NORMAL, "root", "TestJobToString",
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
