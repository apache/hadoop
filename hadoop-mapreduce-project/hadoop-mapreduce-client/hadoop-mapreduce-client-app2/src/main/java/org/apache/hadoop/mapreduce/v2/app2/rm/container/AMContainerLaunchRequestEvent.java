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

package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ContainerId;

public class AMContainerLaunchRequestEvent extends AMContainerEvent {

  private final JobId jobId;
  private final TaskType taskTypeForContainer;
  private final Token<JobTokenIdentifier> jobToken;
  private final Credentials credentials;
  private final boolean shouldProfile;
  private final JobConf jobConf;

  public AMContainerLaunchRequestEvent(ContainerId containerId, JobId jobId,
      TaskType taskType, Token<JobTokenIdentifier> jobToken,
      Credentials credentials, boolean shouldProfile, JobConf jobConf) {
    super(containerId, AMContainerEventType.C_LAUNCH_REQUEST);
    this.jobId = jobId;
    this.taskTypeForContainer = taskType;
    this.jobToken = jobToken;
    this.credentials = credentials;
    this.shouldProfile = shouldProfile;
    this.jobConf = jobConf;
  }

  public JobId getJobId() {
    return this.jobId;
  }

  public TaskType getTaskTypeForContainer() {
    return this.taskTypeForContainer;
  }

  public Token<JobTokenIdentifier> getJobToken() {
    return this.jobToken;
  }

  public Credentials getCredentials() {
    return this.credentials;
  }

  public boolean shouldProfile() {
    return this.shouldProfile;
  }

  public JobConf getJobConf() {
    return this.jobConf;
  }
}
