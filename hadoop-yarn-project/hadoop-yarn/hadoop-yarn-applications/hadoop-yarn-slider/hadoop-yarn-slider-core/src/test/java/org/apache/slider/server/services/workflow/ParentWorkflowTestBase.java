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

package org.apache.slider.server.services.workflow;

import org.apache.hadoop.service.Service;

/**
 * Extends {@link WorkflowServiceTestBase} with parent-specific operations
 * and logic to build up and run the parent service
 */
public abstract class ParentWorkflowTestBase extends WorkflowServiceTestBase {

  /**
   * Wait a second for the service parent to stop
   * @param parent the service to wait for
   */
  protected void waitForParentToStop(ServiceParent parent) {
    waitForParentToStop(parent, 1000);
  }

  /**
   * Wait for the service parent to stop
   * @param parent the service to wait for
   * @param timeout time in milliseconds
   */
  protected void waitForParentToStop(ServiceParent parent, int timeout) {
    boolean stop = parent.waitForServiceToStop(timeout);
    if (!stop) {
      logState(parent);
      fail("Service failed to stop : after " + timeout + " millis " + parent);
    }
  }

  /**
   * Subclasses are require to implement this and return an instance of a
   * ServiceParent
   * @param services a possibly empty list of services
   * @return an inited -but -not-started- service parent instance
   */
  protected abstract ServiceParent buildService(Service... services);

  /**
   * Use {@link #buildService(Service...)} to create service and then start it
   * @param services
   * @return
   */
  protected ServiceParent startService(Service... services) {
    ServiceParent parent = buildService(services);
    //expect service to start and stay started
    parent.start();
    return parent;
  }

}
