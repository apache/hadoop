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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

import com.google.common.annotations.VisibleForTesting;

public class SchedulingMonitor extends AbstractService {

  private final SchedulingEditPolicy scheduleEditPolicy;
  private static final Log LOG = LogFactory.getLog(SchedulingMonitor.class);

  // ScheduledExecutorService which schedules the PreemptionChecker to run
  // periodically.
  private ScheduledExecutorService ses;
  private ScheduledFuture<?> handler;
  private volatile boolean stopped;
  private long monitorInterval;
  private RMContext rmContext;

  public SchedulingMonitor(RMContext rmContext,
      SchedulingEditPolicy scheduleEditPolicy) {
    super("SchedulingMonitor (" + scheduleEditPolicy.getPolicyName() + ")");
    this.scheduleEditPolicy = scheduleEditPolicy;
    this.rmContext = rmContext;
  }

  @VisibleForTesting
  public synchronized SchedulingEditPolicy getSchedulingEditPolicy() {
    return scheduleEditPolicy;
  }

  public void serviceInit(Configuration conf) throws Exception {
    scheduleEditPolicy.init(conf, rmContext, rmContext.getScheduler());
    this.monitorInterval = scheduleEditPolicy.getMonitoringInterval();
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    assert !stopped : "starting when already stopped";
    ses = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName(getName());
        return t;
      }
    });
    handler = ses.scheduleAtFixedRate(new PreemptionChecker(),
        0, monitorInterval, TimeUnit.MILLISECONDS);
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    stopped = true;
    if (handler != null) {
      LOG.info("Stop " + getName());
      handler.cancel(true);
      ses.shutdown();
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
      try {
        //invoke the preemption policy
        invokePolicy();
      } catch (Throwable t) {
        // The preemption monitor does not alter structures nor do structures
        // persist across invocations. Therefore, log, skip, and retry.
        LOG.error("Exception raised while executing preemption"
            + " checker, skip this run..., exception=", t);
      }
    }
  }
}
