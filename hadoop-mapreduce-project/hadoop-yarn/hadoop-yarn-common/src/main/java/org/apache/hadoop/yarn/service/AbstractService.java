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

package org.apache.hadoop.yarn.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public abstract class AbstractService implements Service {

  private static final Log LOG = LogFactory.getLog(AbstractService.class);
  
  private STATE state = STATE.NOTINITED;
  private final String name;
  private long startTime;
  private Configuration config;
  private List<ServiceStateChangeListener> listeners =
    new ArrayList<ServiceStateChangeListener>();

  public AbstractService(String name) {
    this.name = name;
  }

  @Override
  public synchronized STATE getServiceState() {
    return state;
  }

  @Override
  public synchronized void init(Configuration conf) {
    ensureCurrentState(STATE.NOTINITED);
    this.config = conf;
    changeState(STATE.INITED);
    LOG.info("Service:" + getName() + " is inited.");
  }

  @Override
  public synchronized void start() {
    startTime = System.currentTimeMillis();
    ensureCurrentState(STATE.INITED);
    changeState(STATE.STARTED);
    LOG.info("Service:" + getName() + " is started.");
  }

  @Override
  public synchronized void stop() {
    if (state == STATE.STOPPED ||
        state == STATE.INITED ||
        state == STATE.NOTINITED) {
      // already stopped, or else it was never
      // started (eg another service failing canceled startup)
      return;
    }
    ensureCurrentState(STATE.STARTED);
    changeState(STATE.STOPPED);
    LOG.info("Service:" + getName() + " is stopped.");
  }

  @Override
  public synchronized void register(ServiceStateChangeListener l) {
    listeners.add(l);
  }

  @Override
  public synchronized void unregister(ServiceStateChangeListener l) {
    listeners.remove(l);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public synchronized Configuration getConfig() {
    return config;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  private void ensureCurrentState(STATE currentState) {
    if (state != currentState) {
      throw new IllegalStateException("For this operation, current State must " +
        "be " + currentState + " instead of " + state);
    }
  }

  private void changeState(STATE newState) {
    state = newState;
    //notify listeners
    for (ServiceStateChangeListener l : listeners) {
      l.stateChanged(this);
    }
  }
}
