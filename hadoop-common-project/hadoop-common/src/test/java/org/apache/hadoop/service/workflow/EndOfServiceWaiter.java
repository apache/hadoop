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

package org.apache.hadoop.service.workflow;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.junit.Assert;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a {@link ServiceStateChangeListener} that waits for a service to stop;
 */
public class EndOfServiceWaiter implements ServiceStateChangeListener {

  private final AtomicBoolean finished = new AtomicBoolean(false);

  public EndOfServiceWaiter(Service svc) {
    svc.registerServiceListener(this);
  }

  /**
   * Wait for a service to stop. Raises an assertion if the
   * service does not stop in the time period.
   * @param timeout time to wait in millis before failing.
   * @throws InterruptedException
   */
  public synchronized void waitForServiceToStop(long timeout) throws
      InterruptedException {
    if (!finished.get()) {
      wait(timeout);
    }
    Assert.assertTrue("Service did not finish in time period",
        finished.get());
  }

  @Override
  public synchronized void stateChanged(Service service) {
    if (service.isInState(Service.STATE.STOPPED)) {
      finished.set(true);
      notify();
    }
  }


}
