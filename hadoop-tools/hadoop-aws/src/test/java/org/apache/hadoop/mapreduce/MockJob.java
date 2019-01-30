/*
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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.authorize.AccessControlList;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This is a mock job which doesn't talk to YARN.
 * It's in this package as the JobSubmitter API is package-scoped.
 */
public class MockJob extends Job {

  private static final Logger LOG =
      LoggerFactory.getLogger(MockJob.class);

  public static final String NAME = "mock";

  private final ClientProtocol mockClient;
  private static int jobIdCounter;

  private static String trackerId = Long.toString(System.currentTimeMillis());

  private Credentials submittedCredentials;

  public MockJob(final JobConf conf, final String jobName)
      throws IOException, InterruptedException {
    super(conf);
    setJobName(jobName);
    mockClient = mock(ClientProtocol.class);
    init();
  }

  public void init() throws IOException, InterruptedException {
    when(mockClient.submitJob(any(JobID.class),
        any(String.class),
        any(Credentials.class)))
        .thenAnswer(invocation -> {

          final Object[] args = invocation.getArguments();
          String name = (String) args[1];
          LOG.info("Submitted Job {}", name);
          submittedCredentials = (Credentials) args[2];
          final JobStatus status = new JobStatus();
          status.setState(JobStatus.State.RUNNING);
          status.setSchedulingInfo(NAME);
          status.setTrackingUrl("http://localhost:8080/");
          return status;
        });

    when(mockClient.getNewJobID())
        .thenReturn(
            new JobID(trackerId, jobIdCounter++));

    when(mockClient.getQueueAdmins(any(String.class)))
        .thenReturn(
            new AccessControlList(AccessControlList.WILDCARD_ACL_VALUE));
  }

  @Override
  public boolean isSuccessful() throws IOException {
    return true;
  }

  /** Only for mocking via unit tests. */
  @InterfaceAudience.Private
  JobSubmitter getJobSubmitter(FileSystem fs,
      ClientProtocol submitClient) throws IOException {

    return new JobSubmitter(fs, mockClient);
  }

  @Override
  synchronized void connect()
      throws IOException, InterruptedException, ClassNotFoundException {
    super.connect();
  }

  public Credentials getSubmittedCredentials() {
    return submittedCredentials;
  }

  @Override
  synchronized void updateStatus() throws IOException {
    // no-op
  }
}
