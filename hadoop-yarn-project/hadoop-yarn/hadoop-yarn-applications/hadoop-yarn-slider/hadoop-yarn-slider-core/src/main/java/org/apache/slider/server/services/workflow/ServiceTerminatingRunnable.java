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

import com.google.common.base.Preconditions;
import org.apache.hadoop.service.Service;

/**
 * A runnable which terminates its after running; it also catches any
 * exception raised and can serve it back. 
 */
public class ServiceTerminatingRunnable implements Runnable {

  private final Service owner;
  private final Runnable action;
  private Exception exception;

  /**
   * Create an instance.
   * @param owner owning service
   * @param action action to execute before terminating the service
   */
  public ServiceTerminatingRunnable(Service owner, Runnable action) {
    Preconditions.checkArgument(owner != null, "null owner");
    Preconditions.checkArgument(action != null, "null action");
    this.owner = owner;
    this.action = action;
  }

  /**
   * Get the owning service.
   * @return the service to receive notification when
   * the runnable completes.
   */
  public Service getOwner() {
    return owner;
  }

  /**
   * Any exception raised by inner <code>action's</code> run.
   * @return an exception or null.
   */
  public Exception getException() {
    return exception;
  }

  @Override
  public void run() {
    try {
      action.run();
    } catch (Exception e) {
      exception = e;
    }
    owner.stop();
  }
}
