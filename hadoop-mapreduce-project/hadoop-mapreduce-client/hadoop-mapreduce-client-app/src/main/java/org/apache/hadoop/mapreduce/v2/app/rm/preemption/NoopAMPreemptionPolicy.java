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
package org.apache.hadoop.mapreduce.v2.app.rm.preemption;

import org.apache.hadoop.mapreduce.checkpoint.TaskCheckpointID;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;

/**
 * NoOp policy that ignores all the requests for preemption.
 */
public class NoopAMPreemptionPolicy implements AMPreemptionPolicy {

  @Override
  public void init(AppContext context){
   // do nothing
  }

  @Override
  public void preempt(Context ctxt, PreemptionMessage preemptionRequests) {
    // do nothing, ignore all requeusts
  }

  @Override
  public void handleFailedContainer(TaskAttemptId attemptID) {
    // do nothing
  }

  @Override
  public boolean isPreempted(TaskAttemptId yarnAttemptID) {
    return false;
  }

  @Override
  public void reportSuccessfulPreemption(TaskAttemptId taskAttemptID) {
    // ignore
  }

  @Override
  public TaskCheckpointID getCheckpointID(TaskId taskId) {
    return null;
  }

  @Override
  public void setCheckpointID(TaskId taskId, TaskCheckpointID cid) {
    // ignore
  }

  @Override
  public void handleCompletedContainer(TaskAttemptId attemptID) {
    // ignore
  }

}
