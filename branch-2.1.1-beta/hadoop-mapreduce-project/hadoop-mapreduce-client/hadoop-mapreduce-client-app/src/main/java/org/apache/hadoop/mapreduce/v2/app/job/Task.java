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

import java.util.Map;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;

/**
 * Read only view of Task.
 */
public interface Task {
  TaskId getID();
  TaskReport getReport();
  TaskState getState();
  Counters getCounters();
  float getProgress();
  TaskType getType();
  Map<TaskAttemptId, TaskAttempt> getAttempts();
  TaskAttempt getAttempt(TaskAttemptId attemptID);

  /** Has Task reached the final state or not.
   */
  boolean isFinished();

  /**
   * Can the output of the taskAttempt be committed. Note that once the task
   * gives a go for a commit, further canCommit requests from any other attempts
   * should return false.
   * 
   * @param taskAttemptID
   * @return whether the attempt's output can be committed or not.
   */
  boolean canCommit(TaskAttemptId taskAttemptID);

  
}
