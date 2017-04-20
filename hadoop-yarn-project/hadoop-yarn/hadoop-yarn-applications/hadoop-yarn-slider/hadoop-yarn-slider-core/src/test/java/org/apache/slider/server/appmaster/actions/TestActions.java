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

package org.apache.slider.server.appmaster.actions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.slider.server.appmaster.SliderAppMaster;
import org.apache.slider.server.appmaster.state.AppState;
import org.apache.slider.server.services.workflow.ServiceThreadFactory;
import org.apache.slider.server.services.workflow.WorkflowExecutorService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test AM actions.
 */
public class TestActions {
  protected static final Logger LOG =
      LoggerFactory.getLogger(TestActions.class);

  private QueueService queues;
  private WorkflowExecutorService<ExecutorService> executorService;


  @Before
  public void createService() {
    queues = new QueueService();

    Configuration conf = new Configuration();
    queues.init(conf);

    queues.start();

    executorService = new WorkflowExecutorService<>("AmExecutor",
        Executors.newCachedThreadPool(
            new ServiceThreadFactory("AmExecutor", true)));

    executorService.init(conf);
    executorService.start();
  }

  @After
  public void destroyService() {
    ServiceOperations.stop(executorService);
    ServiceOperations.stop(queues);
  }

  @Test
  public void testBasicService() throws Throwable {
    queues.start();
  }

  @Test
  public void testDelayLogic() throws Throwable {
    ActionNoteExecuted action = new ActionNoteExecuted("", 1000);
    long now = System.currentTimeMillis();

    long delay = action.getDelay(TimeUnit.MILLISECONDS);
    assertTrue(delay >= 800);
    assertTrue(delay <= 1800);

    ActionNoteExecuted a2 = new ActionNoteExecuted("a2", 10000);
    assertTrue(action.compareTo(a2) < 0);
    assertTrue(a2.compareTo(action) > 0);
    assertEquals(0, action.compareTo(action));

  }

  @Test
  public void testActionDelayedExecutorTermination() throws Throwable {
    long start = System.currentTimeMillis();

    ActionStopQueue stopAction = new ActionStopQueue(1000);
    queues.scheduledActions.add(stopAction);
    queues.run();
    AsyncAction take = queues.actionQueue.take();
    assertEquals(take, stopAction);
    long stop = System.currentTimeMillis();
    assertTrue(stop - start > 500);
    assertTrue(stop - start < 1500);
  }

  @Test
  public void testImmediateQueue() throws Throwable {
    ActionNoteExecuted noteExecuted = new ActionNoteExecuted("executed", 0);
    queues.put(noteExecuted);
    queues.put(new ActionStopQueue(0));
    QueueExecutor ex = new QueueExecutor(queues);
    ex.run();
    assertTrue(queues.actionQueue.isEmpty());
    assertTrue(noteExecuted.executed.get());
  }

  @Test
  public void testActionOrdering() throws Throwable {

    ActionNoteExecuted note1 = new ActionNoteExecuted("note1", 500);
    ActionStopQueue stop = new ActionStopQueue(1500);
    ActionNoteExecuted note2 = new ActionNoteExecuted("note2", 800);

    List<AsyncAction> actions = Arrays.asList(note1, stop, note2);
    Collections.sort(actions);
    assertEquals(actions.get(0), note1);
    assertEquals(actions.get(1), note2);
    assertEquals(actions.get(2), stop);
  }

  @Test
  public void testDelayedQueueWithReschedule() throws Throwable {

    ActionNoteExecuted note1 = new ActionNoteExecuted("note1", 500);
    ActionStopQueue stop = new ActionStopQueue(1500);
    ActionNoteExecuted note2 = new ActionNoteExecuted("note2", 800);

    assertTrue(note2.compareTo(stop) < 0);
    assertTrue(note1.getNanos() < note2.getNanos());
    assertTrue(note2.getNanos() < stop.getNanos());
    queues.schedule(note1);
    queues.schedule(note2);
    queues.schedule(stop);
    // async to sync expected to run in order
    runQueuesToCompletion();
    assertTrue(note1.executed.get());
    assertTrue(note2.executed.get());
  }

  public void runQueuesToCompletion() {
    queues.run();
    assertTrue(queues.scheduledActions.isEmpty());
    assertFalse(queues.actionQueue.isEmpty());
    QueueExecutor ex = new QueueExecutor(queues);
    ex.run();
    // flush all stop commands from the queue
    queues.flushActionQueue(ActionStopQueue.class);

    assertTrue(queues.actionQueue.isEmpty());
  }

  @Test
  public void testRenewedActionFiresOnceAtLeast() throws Throwable {
    ActionNoteExecuted note1 = new ActionNoteExecuted("note1", 500);
    RenewingAction renewer = new RenewingAction(
        note1,
        500,
        100,
        TimeUnit.MILLISECONDS,
        3);
    queues.schedule(renewer);
    ActionStopQueue stop = new ActionStopQueue(4, TimeUnit.SECONDS);
    queues.schedule(stop);
    // this runs all the delayed actions FIRST, so can't be used
    // to play tricks of renewing actions ahead of the stop action
    runQueuesToCompletion();
    assertEquals(1, renewer.executionCount.intValue());
    assertEquals(1, note1.executionCount.intValue());
    // assert the renewed item is back in
    assertTrue(queues.scheduledActions.contains(renewer));
  }


  @Test
  public void testRenewingActionOperations() throws Throwable {
    ActionNoteExecuted note1 = new ActionNoteExecuted("note1", 500);
    RenewingAction renewer = new RenewingAction(
        note1,
        100,
        100,
        TimeUnit.MILLISECONDS,
        3);
    queues.renewing("note", renewer);
    assertTrue(queues.removeRenewingAction("note"));
    queues.stop();
    assertTrue(queues.waitForServiceToStop(10000));
  }

  /**
   * Test action.
   */
  public class ActionNoteExecuted extends AsyncAction {
    private final AtomicBoolean executed = new AtomicBoolean(false);
    private final AtomicLong executionTimeNanos = new AtomicLong();
    private final AtomicLong executionCount = new AtomicLong();

    public ActionNoteExecuted(String text, int delay) {
      super(text, delay);
    }

    @Override
    public void execute(
        SliderAppMaster appMaster,
        QueueAccess queueService,
        AppState appState) throws Exception {
      LOG.info("Executing {}", name);
      executed.set(true);
      executionTimeNanos.set(System.nanoTime());
      executionCount.incrementAndGet();
      LOG.info(this.toString());

      synchronized (this) {
        this.notify();
      }
    }

    @Override
    public String toString() {
      return super.toString() + " executed=" + executed.get() + "; count=" +
          executionCount.get() + ";";
    }

    public long getExecutionCount() {
      return executionCount.get();
    }
  }
}
