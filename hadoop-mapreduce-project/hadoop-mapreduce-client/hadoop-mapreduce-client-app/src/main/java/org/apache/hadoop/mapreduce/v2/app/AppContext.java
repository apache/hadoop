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

package org.apache.hadoop.mapreduce.v2.app;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.util.Clock;


/**
 * Context interface for sharing information across components in YARN App.
 */
@InterfaceAudience.Private
public interface AppContext {

  ApplicationId getApplicationID();

  ApplicationAttemptId getApplicationAttemptId();

  String getApplicationName();

  long getStartTime();

  CharSequence getUser();

  Job getJob(JobId jobID);

  Map<JobId, Job> getAllJobs();

  EventHandler<Event> getEventHandler();

  Clock getClock();
  
  ClusterInfo getClusterInfo();
  
  Set<String> getBlacklistedNodes();
  
  ClientToAMTokenSecretManager getClientToAMTokenSecretManager();

  boolean isLastAMRetry();

  boolean hasSuccessfullyUnregistered();

  String getNMHostname();

  TaskAttemptFinishingMonitor getTaskAttemptFinishingMonitor();

  String getHistoryUrl();

  void setHistoryUrl(String historyUrl);
}
