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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.MockAppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.hs.MockHistoryJobs.JobsPair;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobsInfo;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

public class MockHistoryContext extends MockAppContext implements HistoryContext {

  private final Map<JobId, Job> partialJobs;
  private final Map<JobId, Job> fullJobs;
  
  public MockHistoryContext(int numJobs, int numTasks, int numAttempts) {
    super(0);
    JobsPair jobs;
    try {
      jobs = MockHistoryJobs.newHistoryJobs(numJobs, numTasks, numAttempts);
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
    partialJobs = jobs.partial;
    fullJobs = jobs.full;
  }

  public MockHistoryContext(int appid, int numJobs, int numTasks,
      int numAttempts) {
    super(appid);
    JobsPair jobs;
    try {
      jobs = MockHistoryJobs.newHistoryJobs(getApplicationID(), numJobs, numTasks,
          numAttempts);
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
    partialJobs = jobs.partial;
    fullJobs = jobs.full;
  }
  
  public MockHistoryContext(int appid, int numTasks, int numAttempts, Path confPath) {
    super(appid, numTasks, numAttempts, confPath);
    fullJobs = super.getAllJobs();
    partialJobs = null;
  }
  
  public MockHistoryContext(int appid, int numJobs, int numTasks, int numAttempts,
      boolean hasFailedTasks) {
    super(appid);
    JobsPair jobs;
    try {
      jobs = MockHistoryJobs.newHistoryJobs(getApplicationID(), numJobs, numTasks,
          numAttempts, hasFailedTasks);
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
    partialJobs = jobs.partial;
    fullJobs = jobs.full;
  }
  
  @Override
  public Job getJob(JobId jobID) {
    return fullJobs.get(jobID);
  }

  public Job getPartialJob(JobId jobID) {
    return partialJobs.get(jobID);
  }
  
  @Override
  public Map<JobId, Job> getAllJobs() {
    return fullJobs;
  }

  @Override
  public Map<JobId, Job> getAllJobs(ApplicationId appID) {
    return null;
  }

  @Override
  public JobsInfo getPartialJobs(Long offset, Long count, String user,
      String queue, Long sBegin, Long sEnd, Long fBegin, Long fEnd,
      JobState jobState) {
    return CachedHistoryStorage.getPartialJobs(this.partialJobs.values(), 
        offset, count, user, queue, sBegin, sEnd, fBegin, fEnd, jobState);
  }

}
