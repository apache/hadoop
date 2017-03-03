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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.MRAppMaster.RunningAppContext;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.preemption.AMPreemptionPolicy;
import org.apache.hadoop.mapreduce.v2.app.rm.preemption.KillAMPreemptionPolicy;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.StrictPreemptionContract;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.junit.Test;

public class TestKillAMPreemptionPolicy {
  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  @SuppressWarnings("unchecked")
  @Test
  public void testKillAMPreemptPolicy() {

    ApplicationId appId = ApplicationId.newInstance(123456789, 1);
    ContainerId container = ContainerId.newContainerId(
        ApplicationAttemptId.newInstance(appId, 1), 1);
    AMPreemptionPolicy.Context mPctxt = mock(AMPreemptionPolicy.Context.class);
    when(mPctxt.getTaskAttempt(any(ContainerId.class))).thenReturn(
        MRBuilderUtils.newTaskAttemptId(MRBuilderUtils.newTaskId(
            MRBuilderUtils.newJobId(appId, 1), 1, TaskType.MAP), 0));
    List<Container> p = new ArrayList<Container>();
    p.add(Container.newInstance(container, null, null, null, null, null));
    when(mPctxt.getContainers(any(TaskType.class))).thenReturn(p);

    KillAMPreemptionPolicy policy = new KillAMPreemptionPolicy();

    // strictContract is null & contract is null
    RunningAppContext mActxt = getRunningAppContext();
    policy.init(mActxt);
    PreemptionMessage pM = getPreemptionMessage(false, false, container);
    policy.preempt(mPctxt, pM);
    verify(mActxt.getEventHandler(), times(0)).handle(
        any(TaskAttemptEvent.class));
    verify(mActxt.getEventHandler(), times(0)).handle(
        any(JobCounterUpdateEvent.class));

    // strictContract is not null & contract is null
    mActxt = getRunningAppContext();
    policy.init(mActxt);
    pM = getPreemptionMessage(true, false, container);
    policy.preempt(mPctxt, pM);
    verify(mActxt.getEventHandler(), times(2)).handle(
        any(TaskAttemptEvent.class));
    verify(mActxt.getEventHandler(), times(2)).handle(
        any(JobCounterUpdateEvent.class));

    // strictContract is null & contract is not null
    mActxt = getRunningAppContext();
    policy.init(mActxt);
    pM = getPreemptionMessage(false, true, container);
    policy.preempt(mPctxt, pM);
    verify(mActxt.getEventHandler(), times(2)).handle(
        any(TaskAttemptEvent.class));
    verify(mActxt.getEventHandler(), times(2)).handle(
        any(JobCounterUpdateEvent.class));

    // strictContract is not null & contract is not null
    mActxt = getRunningAppContext();
    policy.init(mActxt);
    pM = getPreemptionMessage(true, true, container);
    policy.preempt(mPctxt, pM);
    verify(mActxt.getEventHandler(), times(4)).handle(
        any(TaskAttemptEvent.class));
    verify(mActxt.getEventHandler(), times(4)).handle(
        any(JobCounterUpdateEvent.class));
  }

  private RunningAppContext getRunningAppContext() {
    RunningAppContext mActxt = mock(RunningAppContext.class);
    @SuppressWarnings("unchecked")
    EventHandler<Event> eventHandler = mock(EventHandler.class);
    when(mActxt.getEventHandler()).thenReturn(eventHandler);
    return mActxt;
  }

  private PreemptionMessage getPreemptionMessage(boolean strictContract,
      boolean contract, final ContainerId container) {
    PreemptionMessage preemptionMessage = recordFactory
        .newRecordInstance(PreemptionMessage.class);
    Set<PreemptionContainer> cntrs = new HashSet<PreemptionContainer>();
    PreemptionContainer preemptContainer = recordFactory
        .newRecordInstance(PreemptionContainer.class);
    preemptContainer.setId(container);
    cntrs.add(preemptContainer);
    if (strictContract) {
      StrictPreemptionContract set = recordFactory
          .newRecordInstance(StrictPreemptionContract.class);
      set.setContainers(cntrs);
      preemptionMessage.setStrictContract(set);
    }
    if (contract) {
      PreemptionContract preemptContract = recordFactory
          .newRecordInstance(PreemptionContract.class);
      preemptContract.setContainers(cntrs);
      preemptionMessage.setContract(preemptContract);
    }
    return preemptionMessage;
  }

}
