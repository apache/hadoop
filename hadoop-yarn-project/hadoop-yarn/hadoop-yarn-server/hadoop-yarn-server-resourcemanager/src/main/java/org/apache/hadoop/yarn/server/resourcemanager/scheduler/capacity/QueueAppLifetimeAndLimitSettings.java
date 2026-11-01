/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ResolvedQueueConfig;

/**
 * This class holds the application lifetime and max parallel apps settings of a queue, taken
 * from its resolved configuration, in which a queue inherits the lifetimes of its parent.
 **/
public class QueueAppLifetimeAndLimitSettings {
  // -1 indicates lifetime is disabled
  private final long maxApplicationLifetime;
  private final long defaultApplicationLifetime;

  // Indicates if this queue's default lifetime was set by a config property,
  // either at this level or anywhere in the queue's hierarchy.
  private final boolean defaultAppLifetimeWasSpecifiedInConfig;

  private int maxParallelApps;

  public QueueAppLifetimeAndLimitSettings(ResolvedQueueConfig resolved, QueuePath queuePath) {
    // Store max parallel apps property
    this.maxParallelApps = resolved.get(QueueProperties.MAX_PARALLEL_APPS).getValue();
    this.maxApplicationLifetime =
        resolved.get(QueueProperties.MAXIMUM_APPLICATION_LIFETIME).getValue();
    // The resolved default lifetime is already replaced by the maximum lifetime when it is not
    // positive, which does not change the outcome of the check below
    long defaultAppLifetime =
        resolved.get(QueueProperties.DEFAULT_APPLICATION_LIFETIME).getValue();
    this.defaultAppLifetimeWasSpecifiedInConfig = resolved.isDefaultLifetimeSpecified();
    if (!queuePath.isRoot()) {
      String lifetimeError = QueueLimitChecks.checkDefaultAppLifetime(
          maxApplicationLifetime, defaultAppLifetime);
      if (lifetimeError != null) {
        throw new YarnRuntimeException(lifetimeError);
      }
    }
    this.defaultApplicationLifetime = defaultAppLifetime;
  }

  public int getMaxParallelApps() {
    return maxParallelApps;
  }

  public void setMaxParallelApps(int maxParallelApps) {
    this.maxParallelApps = maxParallelApps;
  }

  public long getMaxApplicationLifetime() {
    return maxApplicationLifetime;
  }

  public long getDefaultApplicationLifetime() {
    return defaultApplicationLifetime;
  }

  public boolean isDefaultAppLifetimeWasSpecifiedInConfig() {
    return defaultAppLifetimeWasSpecifiedInConfig;
  }
}
