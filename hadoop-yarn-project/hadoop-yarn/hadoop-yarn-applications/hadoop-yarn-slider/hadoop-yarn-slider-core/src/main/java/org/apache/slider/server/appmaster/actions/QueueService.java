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


import org.apache.slider.server.services.workflow.ServiceThreadFactory;
import org.apache.slider.server.services.workflow.WorkflowExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The Queue service provides immediate and scheduled queues, as well
 * as an executor thread that moves queued actions from the scheduled
 * queue to the immediate one.
 * 
 * <p>
 * This code to be revisited to see if all that was needed is the single scheduled
 * queue, implicitly making actions immediate by giving them an execution
 * time of "now". It would force having a sequence number to all actions, one 
 * which the queue would have to set from its (monotonic, thread-safe) counter
 * on every submission, with a modified comparison operator. This would guarantee
 * that earlier submissions were picked before later ones.
 */
public class QueueService extends WorkflowExecutorService<ExecutorService>
implements Runnable, QueueAccess {
  private static final Logger log =
      LoggerFactory.getLogger(QueueService.class);
  public static final String NAME = "Action Queue";
  private final AtomicBoolean completed = new AtomicBoolean(false);

  /**
   * Immediate actions.
   */
  public final BlockingDeque<AsyncAction> actionQueue =
      new LinkedBlockingDeque<>();

  /**
   * Actions to be scheduled in the future
   */
  public final DelayQueue<AsyncAction> scheduledActions = new DelayQueue<>();

  /**
   * Map of renewing actions by name ... this is to allow them to 
   * be cancelled by name
   */
  private final Map<String, RenewingAction<? extends AsyncAction>> renewingActions
      = new ConcurrentHashMap<>();

  /**
   * Create a queue instance with a single thread executor
   */
  public QueueService() {
    super(NAME,
        ServiceThreadFactory.singleThreadExecutor(NAME, true));
  }

  @Override
  public void put(AsyncAction action) {
    log.debug("Queueing {}", action);
    actionQueue.add(action);
  }

  @Override
  public void schedule(AsyncAction action) {
    log.debug("Scheduling {}", action);
    scheduledActions.add(action);
  }

  @Override
  public boolean remove(AsyncAction action) {
    boolean removedFromDelayQueue = scheduledActions.remove(action);
    boolean removedFromActions = actionQueue.remove(action);
    return removedFromActions || removedFromDelayQueue;
  }
  
  @Override
  public void renewing(String name,
      RenewingAction<? extends AsyncAction> renewingAction) {
    log.debug("Adding renewing Action \"{}\": {}", name,
        renewingAction.getAction());
    if (removeRenewingAction(name)) {
      log.debug("Removed predecessor action");
    }
    renewingActions.put(name, renewingAction);
    schedule(renewingAction);
  } 

  @Override
  public RenewingAction<? extends AsyncAction> lookupRenewingAction(String name) {
    return renewingActions.get(name);
  }

  @Override
  public boolean removeRenewingAction(String name) {
    RenewingAction<? extends AsyncAction> action = renewingActions.remove(name);
     return action != null && remove(action);
  }
  
  /**
   * Stop the service by scheduling an {@link ActionStopQueue} action
   * ..if the processor thread is working this will propagate through
   * and stop the queue handling after all other actions complete.
   * @throws Exception
   */
  @Override
  protected void serviceStop() throws Exception {
    ActionStopQueue stopQueue = new ActionStopQueue("serviceStop: "+ this,
        0, TimeUnit.MILLISECONDS);
    schedule(stopQueue);
    super.serviceStop();
  }

  /**
   * Flush an action queue of all types of a specific action
   * @param clazz 
   */
  protected void flushActionQueue(Class<? extends AsyncAction> clazz) {
    Iterator<AsyncAction> iterator =
        actionQueue.descendingIterator();
    while (iterator.hasNext()) {
      AsyncAction next = iterator.next();
      if (next.getClass().equals(clazz)) {
        iterator.remove();
      }
    }
  }

  @Override
  public boolean hasQueuedActionWithAttribute(int attr) {
    for (AsyncAction action : actionQueue) {
      if (action.hasAttr(attr)) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Run until the queue has been told to stop
   */
  @Override
  public void run() {
    try {

      log.info("QueueService processor started");

      AsyncAction take;
      do {
        take = scheduledActions.take();
        log.debug("Propagating {}", take);
        actionQueue.put(take);
      } while (!(take instanceof ActionStopQueue));
      log.info("QueueService processor terminated");
    } catch (InterruptedException e) {
      // interrupted during actions
    }
    // the thread exits, but does not tag the service as complete. That's expected
    // to be done by the stop queue
  }


  /**
   * Check to see if the queue executor has completed
   * @return the status
   */
  public boolean isCompleted() {
    return completed.get();
  }

  /**
   * Package scoped method to mark the queue service as finished
   */
  void complete() {
    completed.set(true);
  }
}
