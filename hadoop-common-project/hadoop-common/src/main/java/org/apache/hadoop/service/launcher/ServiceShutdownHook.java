/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.service.launcher;

import java.lang.ref.WeakReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ShutdownHookManager;

/**
 * JVM Shutdown hook for Service which will stop the
 * Service gracefully in case of JVM shutdown.
 * This hook uses a weak reference to the service,
 * and when shut down, calls {@link Service#stop()} if the reference is valid.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ServiceShutdownHook implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(
      ServiceShutdownHook.class);

  /**
   * A weak reference to the service.
   */
  private final WeakReference<Service> serviceRef;

  /**
   * Create an instance.
   * @param service the service
   */
  public ServiceShutdownHook(Service service) {
    serviceRef = new WeakReference<>(service);
  }

  /**
   * Register the service for shutdown with Hadoop's
   * {@link ShutdownHookManager}.
   * @param priority shutdown hook priority
   */
  public synchronized void register(int priority) {
    unregister();
    ShutdownHookManager.get().addShutdownHook(this, priority);
  }

  /**
   * Unregister the hook.
   */
  public synchronized void unregister() {
    try {
      ShutdownHookManager.get().removeShutdownHook(this);
    } catch (IllegalStateException e) {
      LOG.info("Failed to unregister shutdown hook: {}", e, e);
    }
  }

  /**
   * Shutdown handler.
   * Query the service hook reference -if it is still valid the 
   * {@link Service#stop()} operation is invoked.
   */
  @Override
  public void run() {
    shutdown();
  }

  /**
   * Shutdown operation.
   * <p>
   * Subclasses may extend it, but it is primarily
   * made available for testing.
   * @return true if the service was stopped and no exception was raised.
   */
  protected boolean shutdown() {
    Service service;
    boolean result = false;
    synchronized (this) {
      service = serviceRef.get();
      serviceRef.clear();
    }
    if (service != null) {
      try {
        // Stop the  Service
        service.stop();
        result = true;
      } catch (Throwable t) {
        LOG.info("Error stopping {}", service.getName(), t);
      }
    }
    return result;
  }
}
