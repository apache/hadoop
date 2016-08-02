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

import java.util.concurrent.Callable;

/**
 * A runnable which terminates its owner; it also catches any
 * exception raised and can serve it back.  
 * 
 */
public class ServiceTerminatingCallable<V> implements Callable<V> {

  private final Service owner;
  private Exception exception;
  /**
   * This is the callback
   */
  private final Callable<V> callable;


  /**
   * Create an instance. If the owner is null, the owning service
   * is not terminated.
   * @param owner owning service -can be null
   * @param callable callback.
   */
  public ServiceTerminatingCallable(Service owner,
      Callable<V> callable) {
    Preconditions.checkArgument(callable != null, "null callable");
    this.owner = owner;
    this.callable = callable;
  }


  /**
   * Get the owning service
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

  /**
   * Delegates the call to the callable supplied in the constructor,
   * then calls the stop() operation on its owner. Any exception
   * is caught, noted and rethrown
   * @return the outcome of the delegated call operation
   * @throws Exception if one was raised.
   */
  @Override
  public V call() throws Exception {
    try {
      return callable.call();
    } catch (Exception e) {
      exception = e;
      throw e;
    } finally {
      if (owner != null) {
        owner.stop();
      }
    }
  }
}
