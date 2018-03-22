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

package org.apache.hadoop.mapreduce.v2.app.job;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.Priority;


/**
 * Main interface to interact with the job.
 */
public interface Job {

  JobId getID();
  String getName();
  JobState getState();
  JobReport getReport();

  /**
   * Get all the counters of this job. This includes job-counters aggregated
   * together with the counters of each task. This creates a clone of the
   * Counters, so use this judiciously.  
   * @return job-counters and aggregate task-counters
   */
  Counters getAllCounters();

  Map<TaskId,Task> getTasks();
  Map<TaskId,Task> getTasks(TaskType taskType);
  Task getTask(TaskId taskID);
  List<String> getDiagnostics();
  int getTotalMaps();
  int getTotalReduces();
  int getCompletedMaps();
  int getCompletedReduces();
  int getFailedMaps();
  int getFailedReduces();
  int getKilledMaps();
  int getKilledReduces();
  float getProgress();
  boolean isUber();
  String getUserName();
  String getQueueName();
  
  /**
   * @return a path to where the config file for this job is located.
   */
  Path getConfFile();
  
  /**
   * @return a parsed version of the config files pointed to by 
   * {@link #getConfFile()}.
   * @throws IOException on any error trying to load the conf file. 
   */
  Configuration loadConfFile() throws IOException;
  
  /**
   * @return the ACLs for this job for each type of JobACL given. 
   */
  Map<JobACL, AccessControlList> getJobACLs();

  TaskAttemptCompletionEvent[]
      getTaskAttemptCompletionEvents(int fromEventId, int maxEvents);

  TaskCompletionEvent[]
      getMapAttemptCompletionEvents(int startIndex, int maxEvents);

  /**
   * @return information for MR AppMasters (previously failed and current)
   */
  List<AMInfo> getAMInfos();
  
  boolean checkAccess(UserGroupInformation callerUGI, JobACL jobOperation);
  
  public void setQueueName(String queueName);
  public void setJobPriority(Priority priority);
}
