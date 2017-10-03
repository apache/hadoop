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

import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.checkpoint.TaskCheckpointID;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.StrictPreemptionContract;
import org.apache.hadoop.yarn.event.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample policy that aggressively kills tasks when requested.
 */
public class KillAMPreemptionPolicy implements AMPreemptionPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(KillAMPreemptionPolicy.class);

  @SuppressWarnings("rawtypes")
  private EventHandler dispatcher = null;

  @Override
  public void init(AppContext context) {
    dispatcher = context.getEventHandler();
  }

  @Override
  public void preempt(Context ctxt, PreemptionMessage preemptionRequests) {
    // for both strict and negotiable preemption requests kill the
    // container
    StrictPreemptionContract strictContract = preemptionRequests
        .getStrictContract();
    if (strictContract != null) {
      for (PreemptionContainer c : strictContract.getContainers()) {
        killContainer(ctxt, c);
      }
    }
    PreemptionContract contract = preemptionRequests.getContract();
    if (contract != null) {
      for (PreemptionContainer c : contract.getContainers()) {
        killContainer(ctxt, c);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void killContainer(Context ctxt, PreemptionContainer c){
    ContainerId reqCont = c.getId();
    TaskAttemptId reqTask = ctxt.getTaskAttempt(reqCont);
    LOG.info("Evicting " + reqTask);
    dispatcher.handle(new TaskAttemptEvent(reqTask,
        TaskAttemptEventType.TA_KILL));

    // add preemption to counters
    JobCounterUpdateEvent jce = new JobCounterUpdateEvent(reqTask
            .getTaskId().getJobId());
        jce.addCounterUpdate(JobCounter.TASKS_REQ_PREEMPT, 1);
        dispatcher.handle(jce);
  }

  @Override
  public void handleFailedContainer(TaskAttemptId attemptID) {
    // ignore
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
