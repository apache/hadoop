/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.service.component;

import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;

/**
 * Policy for components with instances that do not require/support a restart.
 */
public final class NeverRestartPolicy implements ComponentRestartPolicy {

  private static NeverRestartPolicy INSTANCE = new NeverRestartPolicy();

  private NeverRestartPolicy() {
  }

  public static NeverRestartPolicy getInstance() {
    return INSTANCE;
  }

  @Override public boolean isLongLived() {
    return false;
  }

  @Override public boolean hasCompleted(Component component) {
    if (component.getNumSucceededInstances() + component.getNumFailedInstances()
        < component.getNumDesiredInstances()) {
      return false;
    }
    return true;
  }

  @Override public boolean hasCompletedSuccessfully(Component component) {
    if (component.getNumSucceededInstances() == component
        .getNumDesiredInstances()) {
      return true;
    }
    return false;
  }

  @Override public boolean shouldRelaunchInstance(
      ComponentInstance componentInstance, ContainerStatus containerStatus) {
    return false;
  }

  @Override public boolean isReadyForDownStream(Component component) {
    if (hasCompleted(component)) {
      return true;
    }
    return false;
  }

  @Override public boolean allowUpgrades() {
    return false;
  }

  @Override public boolean shouldTerminate(Component component) {
    long nSucceeded = component.getNumSucceededInstances();
    long nFailed = component.getNumFailedInstances();
    if (nSucceeded + nFailed < component.getComponentSpec()
        .getNumberOfContainers()) {
      return false;
    }
    return true;
  }

  @Override public boolean allowContainerRetriesForInstance(
      ComponentInstance componentInstance) {
    return false;
  }
}
