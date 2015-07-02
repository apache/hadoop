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
package org.apache.hadoop.yarn.server.resourcemanager.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;

import com.google.common.annotations.VisibleForTesting;

public class SchedulingMonitor extends AbstractService {

  private final SchedulingEditPolicy scheduleEditPolicy;
  private static final Log LOG = LogFactory.getLog(SchedulingMonitor.class);

  //thread which runs periodically to see the last time since a heartbeat is
  //received.
  private Thread checkerThread;
  private volatile boolean stopped;
  private long monitorInterval;
  private RMContext rmContext;

  public SchedulingMonitor(RMContext rmContext,
      SchedulingEditPolicy scheduleEditPolicy) {
    super("SchedulingMonitor (" + scheduleEditPolicy.getPolicyName() + ")");
    this.scheduleEditPolicy = scheduleEditPolicy;
    this.rmContext = rmContext;
  }

  public long getMonitorInterval() {
    return monitorInterval;
  }
  
  @VisibleForTesting
  public synchronized SchedulingEditPolicy getSchedulingEditPolicy() {
    return scheduleEditPolicy;
  }

  public void serviceInit(Configuration conf) throws Exception {
    scheduleEditPolicy.init(conf, rmContext,
        (PreemptableResourceScheduler) rmContext.getScheduler());
    this.monitorInterval = scheduleEditPolicy.getMonitoringInterval();
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    assert !stopped : "starting when already stopped";
    checkerThread = new Thread(new PreemptionChecker());
    checkerThread.setName(getName());
    checkerThread.start();
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    stopped = true;
    if (checkerThread != null) {
      checkerThread.interrupt();
    }
    super.serviceStop();
  }

  @VisibleForTesting
  public void invokePolicy(){
    scheduleEditPolicy.editSchedule();
  }

  private class PreemptionChecker implements Runnable {
    @Override
    public void run() {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        //invoke the preemption policy at a regular pace
        //the policy will generate preemption or kill events
        //managed by the dispatcher
        invokePolicy();
        try {
          Thread.sleep(monitorInterval);
        } catch (InterruptedException e) {
          LOG.info(getName() + " thread interrupted");
          break;
        }
      }
    }
  }
}
