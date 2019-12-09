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
 * Always restart policy allows for restarts for long live components which
 * never terminate.
 */
public final class AlwaysRestartPolicy implements ComponentRestartPolicy {

  private static AlwaysRestartPolicy INSTANCE = new AlwaysRestartPolicy();

  private AlwaysRestartPolicy() {
  }

  public static AlwaysRestartPolicy getInstance() {
    return INSTANCE;
  }

  @Override public boolean isLongLived() {
    return true;
  }

  /**
   * This is always false since these components never terminate
   *
   * @param component
   * @return
   */
  @Override public boolean hasCompleted(Component component) {
    return false;
  }

  /**
   * This is always false since these components never terminate
   *
   * @param component
   * @return
   */
  @Override public boolean hasCompletedSuccessfully(Component component) {
    return false;
  }

  @Override public boolean shouldRelaunchInstance(
      ComponentInstance componentInstance, ContainerStatus containerStatus) {
    return true;
  }

  @Override public boolean isReadyForDownStream(Component dependentComponent) {
    if (dependentComponent.getNumReadyInstances() < dependentComponent
        .getNumDesiredInstances()) {
      return false;
    }
    return true;
  }

  @Override public boolean allowUpgrades() {
    return true;
  }

  @Override public boolean shouldTerminate(Component component) {
    return false;
  }

  @Override public boolean allowContainerRetriesForInstance(
      ComponentInstance componentInstance) {
    return true;
  }
}
