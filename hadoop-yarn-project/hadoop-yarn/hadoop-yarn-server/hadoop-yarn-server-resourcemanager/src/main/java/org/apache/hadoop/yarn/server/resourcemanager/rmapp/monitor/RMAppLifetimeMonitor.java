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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.monitor;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;
import org.apache.hadoop.yarn.util.SystemClock;

/**
 * This service will monitor the applications against the lifetime value given.
 * The applications will be killed if it running beyond the given time.
 */
public class RMAppLifetimeMonitor
    extends AbstractLivelinessMonitor<RMAppToMonitor> {

  private static final Log LOG = LogFactory.getLog(RMAppLifetimeMonitor.class);

  private RMContext rmContext;
  private Map<RMAppToMonitor, Long> monitoredApps =
      new HashMap<RMAppToMonitor, Long>();

  private static final EnumSet<RMAppState> COMPLETED_APP_STATES =
      EnumSet.of(RMAppState.FINISHED, RMAppState.FINISHING, RMAppState.FAILED,
          RMAppState.KILLED, RMAppState.FINAL_SAVING, RMAppState.KILLING);

  public RMAppLifetimeMonitor(RMContext rmContext) {
    super(RMAppLifetimeMonitor.class.getName(), SystemClock.getInstance());
    this.rmContext = rmContext;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    long monitorInterval = conf.getLong(
        YarnConfiguration.RM_APPLICATION_LIFETIME_MONITOR_INTERVAL_MS,
        YarnConfiguration.DEFAULT_RM_APPLICATION_LIFETIME_MONITOR_INTERVAL_MS);
    if (monitorInterval <= 0) {
      monitorInterval =
          YarnConfiguration.DEFAULT_RM_APPLICATION_LIFETIME_MONITOR_INTERVAL_MS;
    }
    setMonitorInterval(monitorInterval);
    LOG.info("Application lifelime monitor interval set to " + monitorInterval
        + " ms.");
    super.serviceInit(conf);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected synchronized void expire(RMAppToMonitor monitoredAppKey) {
    Long remove = monitoredApps.remove(monitoredAppKey);
    ApplicationId appId = monitoredAppKey.getApplicationId();
    RMApp app = rmContext.getRMApps().get(appId);
    if (app == null) {
      return;
    }
    // Don't trigger a KILL event if application is in completed states
    if (!COMPLETED_APP_STATES.contains(app.getState())) {
      String diagnostics =
          "Application killed due to exceeding its lifetime period " + remove
              + " milliseconds";
      rmContext.getDispatcher().getEventHandler()
          .handle(new RMAppEvent(appId, RMAppEventType.KILL, diagnostics));
    } else {
      LOG.info("Application " + appId
          + " is about to complete. So not killing the application.");
    }
  }

  public synchronized void registerApp(ApplicationId appId,
      ApplicationTimeoutType timeoutType, long monitorStartTime, long timeout) {
    RMAppToMonitor appToMonitor = new RMAppToMonitor(appId, timeoutType);
    register(appToMonitor, monitorStartTime);
    monitoredApps.putIfAbsent(appToMonitor, timeout);
  }

  @Override
  protected synchronized long getExpireInterval(
      RMAppToMonitor monitoredAppKey) {
    return monitoredApps.get(monitoredAppKey);
  }

  public synchronized void unregisterApp(ApplicationId appId,
      ApplicationTimeoutType timeoutType) {
    RMAppToMonitor appToRemove = new RMAppToMonitor(appId, timeoutType);
    unregister(appToRemove);
    monitoredApps.remove(appToRemove);
  }

  public synchronized void unregisterApp(ApplicationId appId,
      Set<ApplicationTimeoutType> types) {
    for (ApplicationTimeoutType type : types) {
      unregisterApp(appId, type);
    }
  }

  public synchronized void updateApplicationTimeouts(ApplicationId appId,
      Map<ApplicationTimeoutType, Long> timeouts) {
    // TODO in YARN-5611
  }
}