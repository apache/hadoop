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
 * Interface for Component Restart policies.
 * Which is used to make decisions on termination/restart of components and
 * their instances.
 */
public interface ComponentRestartPolicy {

  boolean isLongLived();

  boolean hasCompleted(Component component);

  boolean hasCompletedSuccessfully(Component component);

  boolean shouldRelaunchInstance(ComponentInstance componentInstance,
      ContainerStatus containerStatus);

  boolean isReadyForDownStream(Component component);

  boolean allowUpgrades();

  boolean shouldTerminate(Component component);

  boolean allowContainerRetriesForInstance(ComponentInstance componentInstance);

}