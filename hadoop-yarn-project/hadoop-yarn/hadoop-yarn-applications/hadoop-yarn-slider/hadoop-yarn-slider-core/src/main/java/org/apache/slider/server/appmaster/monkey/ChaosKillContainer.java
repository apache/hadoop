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

package org.apache.slider.server.appmaster.monkey;

import com.google.common.base.Preconditions;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.server.appmaster.actions.ActionKillContainer;
import org.apache.slider.server.appmaster.actions.QueueAccess;
import org.apache.slider.server.appmaster.operations.RMOperationHandler;
import org.apache.slider.server.appmaster.state.AppState;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ListIterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Kill a container
 */
public class ChaosKillContainer implements ChaosTarget {
  protected static final Logger log =
      LoggerFactory.getLogger(ChaosKillContainer.class);
  public static final int DELAY = 100;
  private final AppState appState;
  private final QueueAccess queues;
  private final Random random = new Random();
  private final RMOperationHandler operationHandler;

  public ChaosKillContainer(AppState appState,
      QueueAccess queues,
      RMOperationHandler operationHandler) {
    Preconditions.checkNotNull(appState);
    Preconditions.checkNotNull(queues);
    this.appState = appState;
    this.queues = queues;
    this.operationHandler = operationHandler;
  }

  /**
   * Trigger a container kill 
   */
  @Override
  public void chaosAction() {
    List<RoleInstance> liveContainers =
        appState.cloneLiveContainerInfoList();
    // purge any and all components which declare that they are an AM
    ListIterator<RoleInstance> containers =
        liveContainers.listIterator();
    while (containers.hasNext()) {
      RoleInstance instance = containers.next();
      if (SliderKeys.COMPONENT_AM.equals(instance.role)) {
        containers.remove();
      }
    }
    int size = liveContainers.size();
    if (size > 0) {
      int target = random.nextInt(size);
      RoleInstance roleInstance = liveContainers.get(target);
      log.info("Killing {}", roleInstance);

      queues.schedule(new ActionKillContainer(roleInstance.getId(),
          DELAY, TimeUnit.MILLISECONDS, operationHandler));
    }
  }
}
