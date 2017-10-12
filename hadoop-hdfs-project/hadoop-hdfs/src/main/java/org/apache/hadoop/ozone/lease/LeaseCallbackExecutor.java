/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.lease;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * This class is responsible for executing the callbacks of a lease in case of
 * timeout.
 */
public class LeaseCallbackExecutor<T> implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(Lease.class);

  private final T resource;
  private final List<Callable<Void>> callbacks;

  /**
   * Constructs LeaseCallbackExecutor instance with list of callbacks.
   *
   * @param resource
   *        The resource for which the callbacks are executed
   * @param callbacks
   *        Callbacks to be executed by this executor
   */
  public LeaseCallbackExecutor(T resource, List<Callable<Void>> callbacks) {
    this.resource = resource;
    this.callbacks = callbacks;
  }

  @Override
  public void run() {
    if(LOG.isDebugEnabled()) {
      LOG.debug("Executing callbacks for lease on {}", resource);
    }
    for(Callable<Void> callback : callbacks) {
      try {
        callback.call();
      } catch (Exception e) {
        LOG.warn("Exception while executing callback for lease on {}",
            resource, e);
      }
    }
  }

}
