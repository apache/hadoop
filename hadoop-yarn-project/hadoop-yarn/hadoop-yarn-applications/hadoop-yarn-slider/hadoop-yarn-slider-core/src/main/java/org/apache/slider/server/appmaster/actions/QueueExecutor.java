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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.slider.server.appmaster.SliderAppMaster;
import org.apache.slider.server.appmaster.state.AppState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Executor for async actions - hands them off to the AM as 
 * appropriate
 */
public class QueueExecutor implements Runnable {
  private static final Logger log =
      LoggerFactory.getLogger(QueueExecutor.class);

  private final SliderAppMaster appMaster;
  private final QueueService actionQueues;
  private final AppState appState;


  public QueueExecutor(SliderAppMaster appMaster,
      QueueService actionQueues) {
    Preconditions.checkNotNull(appMaster);
    Preconditions.checkNotNull(actionQueues);

    this.appMaster = appMaster;
    this.actionQueues = actionQueues;
    this.appState = appMaster.getAppState();
  }

  @VisibleForTesting
  public QueueExecutor(QueueService actionQueues) {
    Preconditions.checkNotNull(actionQueues);
    this.appMaster = null;
    this.appState = null;
    this.actionQueues = actionQueues;
  }

  /**
   * Run until the queue has been told to stop
   */
  @Override
  public void run() {
    AsyncAction take = null;
    try {
      log.info("Queue Executor run() started");
      do {
        take = actionQueues.actionQueue.take();
        log.debug("Executing {}", take);
        
        take.execute(appMaster, actionQueues, appState);
        log.debug("Completed {}", take);

      } while (!(take instanceof ActionStopQueue));
      log.info("Queue Executor run() stopped");
    } catch (InterruptedException e) {
      // interrupted: exit
    } catch (Throwable e) {
      log.error("Exception processing {}: {}", take, e, e);
      if (appMaster != null) {
        appMaster.onExceptionInThread(Thread.currentThread(), e);
      }
    }
    // tag completed
    actionQueues.complete();
  }

}
