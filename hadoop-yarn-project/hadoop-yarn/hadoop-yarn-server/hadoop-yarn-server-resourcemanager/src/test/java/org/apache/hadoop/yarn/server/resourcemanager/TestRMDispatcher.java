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

package org.apache.hadoop.yarn.server.resourcemanager;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.junit.Assert;
import org.junit.Test;

public class TestRMDispatcher {

  @SuppressWarnings("unchecked")
  @Test(timeout=10000)
  public void testSchedulerEventDispatcherForPreemptionEvents() {
    AsyncDispatcher rmDispatcher = new AsyncDispatcher();
    CapacityScheduler sched = spy(new CapacityScheduler());
    YarnConfiguration conf = new YarnConfiguration();
    EventDispatcher schedulerDispatcher =
        new EventDispatcher(sched, sched.getClass().getName());
    schedulerDispatcher.disableExitOnError();
    rmDispatcher.register(SchedulerEventType.class, schedulerDispatcher);
    rmDispatcher.init(conf);
    rmDispatcher.start();
    schedulerDispatcher.init(conf);
    schedulerDispatcher.start();
    try {
      ApplicationAttemptId appAttemptId = mock(ApplicationAttemptId.class);
      RMContainer container = mock(RMContainer.class);
      ContainerPreemptEvent event1 = new ContainerPreemptEvent(
          appAttemptId, container, SchedulerEventType.KILL_RESERVED_CONTAINER);
      rmDispatcher.getEventHandler().handle(event1);
      ContainerPreemptEvent event2 =
          new ContainerPreemptEvent(appAttemptId, container,
            SchedulerEventType.MARK_CONTAINER_FOR_KILLABLE);
      rmDispatcher.getEventHandler().handle(event2);
      ContainerPreemptEvent event3 =
          new ContainerPreemptEvent(appAttemptId, container,
            SchedulerEventType.MARK_CONTAINER_FOR_PREEMPTION);
      rmDispatcher.getEventHandler().handle(event3);
      // Wait for events to be processed by scheduler dispatcher.
      Thread.sleep(1000);
      verify(sched, times(3)).handle(any(SchedulerEvent.class));
      verify(sched).killReservedContainer(container);
      verify(sched).markContainerForPreemption(appAttemptId, container);
      verify(sched).markContainerForKillable(container);
    } catch (InterruptedException e) {
      Assert.fail();
    } finally {
      schedulerDispatcher.stop();
      rmDispatcher.stop();
    }
  }
}