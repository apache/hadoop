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


package org.apache.hadoop.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.Service;

/**
 * This is a service that can be configured to break on any of the lifecycle
 * events, so test the failure handling of other parts of the service
 * infrastructure.
 *
 * It retains a counter to the number of times each entry point is called -
 * these counters are incremented before the exceptions are raised and
 * before the superclass state methods are invoked.
 *
 */


public class BreakableService extends AbstractService {

  private boolean failOnInit;
  private boolean failOnStart;
  private boolean failOnStop;
  private int[] counts = new int[4];

  public BreakableService() {
    this(false, false, false);
  }

  public BreakableService(boolean failOnInit,
                          boolean failOnStart,
                          boolean failOnStop) {
    super("BreakableService");
    this.failOnInit = failOnInit;
    this.failOnStart = failOnStart;
    this.failOnStop = failOnStop;
    inc(STATE.NOTINITED);
  }

  private int convert(STATE state) {
    return state.getValue();
  }

  private void inc(STATE state) {
    int index = convert(state);
    counts[index] ++;
  }

  public int getCount(STATE state) {
    return counts[convert(state)];
  }

  private void maybeFail(boolean fail, String action) {
    if (fail) {
      throw new BrokenLifecycleEvent(this, action);
    }
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    inc(STATE.INITED);
    maybeFail(failOnInit, "init");
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() {
    inc(STATE.STARTED);
    maybeFail(failOnStart, "start");
  }

  @Override
  protected void serviceStop() {
    inc(STATE.STOPPED);
    maybeFail(failOnStop, "stop");
  }

  public void setFailOnInit(boolean failOnInit) {
    this.failOnInit = failOnInit;
  }

  public void setFailOnStart(boolean failOnStart) {
    this.failOnStart = failOnStart;
  }

  public void setFailOnStop(boolean failOnStop) {
    this.failOnStop = failOnStop;
  }

  /**
   * The exception explicitly raised on a failure
   */
  public static class BrokenLifecycleEvent extends RuntimeException {

    final STATE state;

    public BrokenLifecycleEvent(Service service, String action) {
      super("Lifecycle Failure during " + action + " state is "
            + service.getServiceState());
      state = service.getServiceState();
    }
  }
}
