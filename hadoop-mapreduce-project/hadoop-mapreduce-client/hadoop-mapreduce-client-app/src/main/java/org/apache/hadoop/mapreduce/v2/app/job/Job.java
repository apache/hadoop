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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.security.UserGroupInformation;


/**
 * Main interface to interact with the job. Provides only getters. 
 */
public interface Job {

  JobId getID();
  String getName();
  JobState getState();
  JobReport getReport();
  Counters getCounters();
  Map<TaskId,Task> getTasks();
  Map<TaskId,Task> getTasks(TaskType taskType);
  Task getTask(TaskId taskID);
  List<String> getDiagnostics();
  int getTotalMaps();
  int getTotalReduces();
  int getCompletedMaps();
  int getCompletedReduces();
  boolean isUber();
  String getUserName();

  TaskAttemptCompletionEvent[]
      getTaskAttemptCompletionEvents(int fromEventId, int maxEvents);

  boolean checkAccess(UserGroupInformation callerUGI, JobACL jobOperation);
}
