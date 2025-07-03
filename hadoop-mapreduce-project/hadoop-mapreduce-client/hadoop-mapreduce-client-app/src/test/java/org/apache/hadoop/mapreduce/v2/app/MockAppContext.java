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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.util.Clock;

import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

public class MockAppContext implements AppContext {
  final ApplicationAttemptId appAttemptID;
  final ApplicationId appID;
  final String user = MockJobs.newUserName();
  final Map<JobId, Job> jobs;
  final long startTime = System.currentTimeMillis();
  Set<String> blacklistedNodes;
  String queue;
  
  public MockAppContext(int appid) {
    appID = MockJobs.newAppID(appid);
    appAttemptID = ApplicationAttemptId.newInstance(appID, 0);
    jobs = null;
  }
  
  public MockAppContext(int appid, int numTasks, int numAttempts, Path confPath) {
    appID = MockJobs.newAppID(appid);
    appAttemptID = ApplicationAttemptId.newInstance(appID, 0);
    Map<JobId, Job> map = Maps.newHashMap();
    Job job = MockJobs.newJob(appID, 0, numTasks, numAttempts, confPath);
    map.put(job.getID(), job);
    jobs = map;
  }
  
  public MockAppContext(int appid, int numJobs, int numTasks, int numAttempts) {
    this(appid, numJobs, numTasks, numAttempts, false);
  }
  
  public MockAppContext(int appid, int numJobs, int numTasks, int numAttempts,
      boolean hasFailedTasks) {
    appID = MockJobs.newAppID(appid);
    appAttemptID = ApplicationAttemptId.newInstance(appID, 0);
    jobs = MockJobs.newJobs(appID, numJobs, numTasks, numAttempts, hasFailedTasks);
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return appAttemptID;
  }

  @Override
  public ApplicationId getApplicationID() {
    return appID;
  }

  @Override
  public CharSequence getUser() {
    return user;
  }

  @Override
  public Job getJob(JobId jobID) {
    return jobs.get(jobID);
  }

  @Override
  public Map<JobId, Job> getAllJobs() {
    return jobs; // OK
  }

  @Override
  public EventHandler<Event> getEventHandler() {
    return new MockEventHandler();
  }

  @Override
  public Clock getClock() {
    return null;
  }

  @Override
  public String getApplicationName() {
    return "TestApp";
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public ClusterInfo getClusterInfo() {
    return null;
  }

  @Override
  public Set<String> getBlacklistedNodes() {
    return blacklistedNodes;
  }
  
  public void setBlacklistedNodes(Set<String> blacklistedNodes) {
    this.blacklistedNodes = blacklistedNodes;
  }

  public ClientToAMTokenSecretManager getClientToAMTokenSecretManager() {
    // Not implemented
    return null;
  }

  @Override
  public boolean isLastAMRetry() {
    return false;
  }

  @Override
  public boolean hasSuccessfullyUnregistered() {
    // bogus - Not Required
    return true;
  }

@Override
  public String getNMHostname() {
    // bogus - Not Required
    return null;
  }

  @Override
  public TaskAttemptFinishingMonitor getTaskAttemptFinishingMonitor() {
      return null;
  }

  @Override
  public String getHistoryUrl() {
    return null;
  }

  @Override
  public void setHistoryUrl(String historyUrl) {
    return;
  }

}
