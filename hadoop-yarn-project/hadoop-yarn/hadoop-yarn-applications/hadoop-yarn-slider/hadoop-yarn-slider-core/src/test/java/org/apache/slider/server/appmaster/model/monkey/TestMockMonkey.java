/*
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

package org.apache.slider.server.appmaster.model.monkey;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.server.appmaster.actions.ActionHalt;
import org.apache.slider.server.appmaster.actions.ActionKillContainer;
import org.apache.slider.server.appmaster.actions.AsyncAction;
import org.apache.slider.server.appmaster.actions.QueueService;
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest;
import org.apache.slider.server.appmaster.model.mock.MockRMOperationHandler;
import org.apache.slider.server.appmaster.monkey.ChaosKillAM;
import org.apache.slider.server.appmaster.monkey.ChaosKillContainer;
import org.apache.slider.server.appmaster.monkey.ChaosMonkeyService;
import org.apache.slider.server.appmaster.monkey.ChaosTarget;
import org.apache.slider.server.appmaster.operations.ContainerReleaseOperation;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test chaos monkey.
 */
public class TestMockMonkey extends BaseMockAppStateTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMockMonkey.class);

  /**
   * This queue service is NOT started; tests need to poll the queue
   * rather than expect them to execute.
   */
  private QueueService queues;
  private ChaosMonkeyService monkey;

  @Before
  public void init() {
    YarnConfiguration configuration = new YarnConfiguration();
    queues = new QueueService();
    queues.init(configuration);
    monkey = new ChaosMonkeyService(METRICS.getMetrics(), queues);
    monkey.init(configuration);
  }

  @Test
  public void testMonkeyStart() throws Throwable {
    monkey.start();
    monkey.stop();
  }

  @Test
  public void testMonkeyPlay() throws Throwable {
    ChaosCounter counter = new ChaosCounter();
    monkey.addTarget("target", counter, InternalKeys.PROBABILITY_PERCENT_100);
    assertEquals(1, monkey.getTargetCount());
    monkey.play();
    assertEquals(1, counter.count);
  }

  @Test
  public void testMonkeySchedule() throws Throwable {
    ChaosCounter counter = new ChaosCounter();
    assertEquals(0, monkey.getTargetCount());
    monkey.addTarget("target", counter, InternalKeys.PROBABILITY_PERCENT_100);
    assertEquals(1, monkey.getTargetCount());
    assertTrue(monkey.schedule(0, 1, TimeUnit.SECONDS));
    assertEquals(1, queues.scheduledActions.size());
  }

  @Test
  public void testMonkeyDoesntAddProb0Actions() throws Throwable {
    ChaosCounter counter = new ChaosCounter();
    monkey.addTarget("target", counter, 0);
    assertEquals(0, monkey.getTargetCount());
    monkey.play();
    assertEquals(0, counter.count);
  }

  @Test
  public void testMonkeyScheduleProb0Actions() throws Throwable {
    ChaosCounter counter = new ChaosCounter();
    monkey.addTarget("target", counter, 0);
    assertFalse(monkey.schedule(0, 1, TimeUnit.SECONDS));
    assertEquals(0, queues.scheduledActions.size());
  }

  @Test
  public void testMonkeyPlaySometimes() throws Throwable {
    ChaosCounter counter = new ChaosCounter();
    ChaosCounter counter2 = new ChaosCounter();
    monkey.addTarget("target1", counter, InternalKeys.PROBABILITY_PERCENT_1
        * 50);
    monkey.addTarget("target2", counter2, InternalKeys
        .PROBABILITY_PERCENT_1 * 25);

    for (int i = 0; i < 100; i++) {
      monkey.play();
    }
    LOG.info("Counter1 = {} counter2 = {}", counter.count, counter2.count);
    /*
     * Relying on probability here to give approximate answers
     */
    assertTrue(counter.count > 25);
    assertTrue(counter.count < 75);
    assertTrue(counter2.count < counter.count);
  }

  @Test
  public void testAMKiller() throws Throwable {

    ChaosKillAM chaos = new ChaosKillAM(queues, -1);
    chaos.chaosAction();
    assertEquals(1, queues.scheduledActions.size());
    AsyncAction action = queues.scheduledActions.take();
    assertTrue(action instanceof ActionHalt);
  }

  @Test
  public void testContainerKillerEmptyApp() throws Throwable {


    ChaosKillContainer chaos = new ChaosKillContainer(appState,
        queues,
        new MockRMOperationHandler());
    chaos.chaosAction();
    assertEquals(0, queues.scheduledActions.size());
  }

  @Ignore
  @Test
  public void testContainerKillerIgnoresAM() throws Throwable {
    // TODO: AM needed in live container list?
    addAppMastertoAppState();
    assertEquals(1, appState.getLiveContainers().size());

    ChaosKillContainer chaos = new ChaosKillContainer(appState,
        queues,
        new MockRMOperationHandler());
    chaos.chaosAction();
    assertEquals(0, queues.scheduledActions.size());
  }

  @Test
  public void testContainerKiller() throws Throwable {
    MockRMOperationHandler ops = new MockRMOperationHandler();
    getRole0Status().setDesired(1);
    List<RoleInstance> instances = createAndStartNodes();
    assertEquals(1, instances.size());
    RoleInstance instance = instances.get(0);

    ChaosKillContainer chaos = new ChaosKillContainer(appState, queues, ops);
    chaos.chaosAction();
    assertEquals(1, queues.scheduledActions.size());
    AsyncAction action = queues.scheduledActions.take();
    ActionKillContainer killer = (ActionKillContainer) action;
    assertEquals(killer.getContainerId(), instance.getContainerId());
    killer.execute(null, queues, appState);
    assertEquals(1, ops.getNumReleases());

    ContainerReleaseOperation operation = (ContainerReleaseOperation) ops
        .getFirstOp();
    assertEquals(operation.getContainerId(), instance.getContainerId());
  }

  /**
   * Chaos target that just implements a counter.
   */
  private static class ChaosCounter implements ChaosTarget {
    private int count;

    @Override
    public void chaosAction() {
      count++;
    }


    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "ChaosCounter{");
      sb.append("count=").append(count);
      sb.append('}');
      return sb.toString();
    }
  }
}
