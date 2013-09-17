/**
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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.application;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerInitEvent;

/**
 * Event sent from {@link ContainerManagerImpl} to {@link ApplicationImpl} to
 * request the initialization of a container. This is funneled through
 * the Application so that the application life-cycle can be checked, and container
 * launches can be delayed until the application is fully initialized.
 * 
 * Once the application is initialized,
 * {@link ApplicationImpl.InitContainerTransition} simply passes this event on as a
 * {@link ContainerInitEvent}.
 *  
 */
public class ApplicationContainerInitEvent extends ApplicationEvent {
  final Container container;
  
  public ApplicationContainerInitEvent(Container container) {
    super(container.getContainerId().getApplicationAttemptId()
        .getApplicationId(), ApplicationEventType.INIT_CONTAINER);
    this.container = container;
  }

  Container getContainer() {
    return container;
  }
}
