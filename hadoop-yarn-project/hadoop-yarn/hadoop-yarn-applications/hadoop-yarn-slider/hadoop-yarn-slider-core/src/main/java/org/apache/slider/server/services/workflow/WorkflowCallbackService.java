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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A service that calls the supplied callback when it is started -after the 
 * given delay.
 *
 * It can be configured to stop itself after the callback has
 * completed, marking any exception raised as the exception of this service.
 * The notifications come in on a callback thread -a thread that is only
 * started in this service's <code>start()</code> operation.
 */
public class WorkflowCallbackService<V> extends
    WorkflowScheduledExecutorService<ScheduledExecutorService> {
  protected static final Logger LOG =
      LoggerFactory.getLogger(WorkflowCallbackService.class);

  /**
   * This is the callback.
   */
  private final Callable<V> callback;
  private final int delay;
  private final ServiceTerminatingCallable<V> command;

  private ScheduledFuture<V> scheduledFuture;

  /**
   * Create an instance of the service
   * @param name service name
   * @param callback callback to invoke
   * @param delay delay -or 0 for no delay
   * @param terminate terminate this service after the callback?
   */
  public WorkflowCallbackService(String name,
      Callable<V> callback,
      int delay,
      boolean terminate) {
    super(name);
    Preconditions.checkNotNull(callback, "Null callback argument");
    this.callback = callback;
    this.delay = delay;
    command = new ServiceTerminatingCallable<V>(
        terminate ? this : null,
        callback);
  }

  public ScheduledFuture<V> getScheduledFuture() {
    return scheduledFuture;
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.debug("Notifying {} after a delay of {} millis", callback, delay);
    ScheduledExecutorService executorService =
        Executors.newSingleThreadScheduledExecutor(
            new ServiceThreadFactory(getName(), true));
    setExecutor(executorService);
    scheduledFuture =
        executorService.schedule(command, delay, TimeUnit.MILLISECONDS);
  }

  /**
   * Stop the service.
   * If there is any exception noted from any executed notification,
   * note the exception in this class
   * @throws Exception exception.
   */
  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    // propagate any failure
    if (getCallbackException() != null) {
      throw getCallbackException();
    }
  }

  /**
   * Get the exception raised by a callback. Will always be null if the 
   * callback has not been executed; will only be non-null after any success.
   * @return a callback
   */
  public Exception getCallbackException() {
    return command.getException();
  }

}
