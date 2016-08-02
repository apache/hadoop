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

package org.apache.slider.server.services.utility;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceStateChangeListener;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wait for a service to stop.
 * 
 * WARNING: the notification may come in as soon as the service enters
 * the stopped state: it may take some time for the actual stop operation
 * to complete.
 */
public class EndOfServiceWaiter implements ServiceStateChangeListener {

  private final AtomicBoolean finished = new AtomicBoolean(false);
  private final String name;
  private Service service;

  /**
   * Wait for a service; use the service name as this instance's name
   * @param service service
   */
  public EndOfServiceWaiter(Service service) {
    this(service.getName(), service);
  }


  /**
   * Wait for a service
   * @param name name for messages
   * @param service service
   */
  public EndOfServiceWaiter(String name, Service service) {
    this.name = name;
    this.service = service;
    service.registerServiceListener(this);
  }

  public synchronized void waitForServiceToStop(long timeout) throws
      InterruptedException, TimeoutException {
    service.waitForServiceToStop(timeout);
    if (!finished.get()) {
      wait(timeout);
      if (!finished.get()) {
        throw new TimeoutException(name
                                   + " did not finish after " + timeout +
                                   " milliseconds");
      }
    }
  }

  /**
   * Wait for service state change callbacks; notify self if the service has
   * now stopped
   * @param service service
   */
  @Override
  public synchronized void stateChanged(Service service) {
    if (service.isInState(Service.STATE.STOPPED)) {
      finished.set(true);
      notify();
    }
  }


}
