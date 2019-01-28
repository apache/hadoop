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
 * Policy for components that require restarts for instances on failure.
 */
public final class OnFailureRestartPolicy implements ComponentRestartPolicy {

  private static OnFailureRestartPolicy INSTANCE = new OnFailureRestartPolicy();

  private OnFailureRestartPolicy() {
  }

  public static OnFailureRestartPolicy getInstance() {
    return INSTANCE;
  }

  @Override public boolean isLongLived() {
    return false;
  }

  @Override public boolean hasCompleted(Component component) {
    if (hasCompletedSuccessfully(component)) {
      return true;
    }

    return false;
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

    if (ComponentInstance.hasContainerFailed(containerStatus)) {
      return true;
    }

    return false;
  }

  @Override public boolean isReadyForDownStream(Component dependentComponent) {
    if (dependentComponent.getNumReadyInstances()
        + dependentComponent.getNumSucceededInstances()
        + dependentComponent.getNumFailedInstances()
        < dependentComponent.getNumDesiredInstances()) {
      return false;
    }
    return true;
  }

  @Override public boolean allowUpgrades() {
    return false;
  }

  @Override public boolean shouldTerminate(Component component) {
    long nSucceeded = component.getNumSucceededInstances();
    if (nSucceeded < component.getComponentSpec().getNumberOfContainers()) {
      return false;
    }
    return true;
  }

  @Override public boolean allowContainerRetriesForInstance(
      ComponentInstance componentInstance) {
    return true;
  }
}
