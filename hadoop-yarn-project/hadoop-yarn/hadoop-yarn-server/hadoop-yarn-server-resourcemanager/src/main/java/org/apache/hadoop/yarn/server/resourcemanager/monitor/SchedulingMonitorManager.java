/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.monitor;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Manages scheduling monitors.
 */
public class SchedulingMonitorManager {
  private static final Log LOG = LogFactory.getLog(
      SchedulingMonitorManager.class);

  private Map<String, SchedulingMonitor> runningSchedulingMonitors =
      new HashMap<>();
  private RMContext rmContext;

  private void updateSchedulingMonitors(Configuration conf,
      boolean startImmediately) throws YarnException {
    boolean monitorsEnabled = conf.getBoolean(
        YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ENABLE_MONITORS);

    if (!monitorsEnabled) {
      if (!runningSchedulingMonitors.isEmpty()) {
        // If monitors disabled while we have some running monitors, we should
        // stop them.
        LOG.info("Scheduling Monitor disabled, stopping all services");
        stopAndRemoveAll();
      }

      return;
    }

    // When monitor is enabled, loading policies
    String[] configuredPolicies = conf.getStrings(
        YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES);
    if (configuredPolicies == null || configuredPolicies.length == 0) {
      return;
    }

    Set<String> configurePoliciesSet = new HashSet<>();
    for (String s : configuredPolicies) {
      configurePoliciesSet.add(s);
    }

    // Add new monitor when needed
    for (String s : configurePoliciesSet) {
      if (!runningSchedulingMonitors.containsKey(s)) {
        Class<?> policyClass;
        try {
          policyClass = Class.forName(s);
        } catch (ClassNotFoundException e) {
          String message = "Failed to find class of specified policy=" + s;
          LOG.warn(message);
          throw new YarnException(message);
        }

        if (SchedulingEditPolicy.class.isAssignableFrom(policyClass)) {
          SchedulingEditPolicy policyInstance =
              (SchedulingEditPolicy) ReflectionUtils.newInstance(policyClass,
                  null);
          SchedulingMonitor mon = new SchedulingMonitor(rmContext,
              policyInstance);
          mon.init(conf);
          if (startImmediately) {
            mon.start();
          }
          runningSchedulingMonitors.put(s, mon);
        } else {
          String message =
              "Specified policy=" + s + " is not a SchedulingEditPolicy class.";
          LOG.warn(message);
          throw new YarnException(message);
        }
      }
    }

    // Stop monitor when needed.
    Set<String> disabledPolicies = Sets.difference(
        runningSchedulingMonitors.keySet(), configurePoliciesSet);
    for (String disabledPolicy : disabledPolicies) {
      LOG.info("SchedulingEditPolicy=" + disabledPolicy
          + " removed, stopping it now ...");
      silentlyStopSchedulingMonitor(disabledPolicy);
      runningSchedulingMonitors.remove(disabledPolicy);
    }
  }

  public synchronized void initialize(RMContext rmContext,
      Configuration configuration) throws YarnException {
    this.rmContext = rmContext;
    stopAndRemoveAll();

    updateSchedulingMonitors(configuration, false);
  }

  public synchronized void reinitialize(RMContext rmContext,
      Configuration configuration) throws YarnException {
    this.rmContext = rmContext;

    updateSchedulingMonitors(configuration, true);
  }

  public synchronized void startAll() {
    for (SchedulingMonitor schedulingMonitor : runningSchedulingMonitors
        .values()) {
      schedulingMonitor.start();
    }
  }

  private void silentlyStopSchedulingMonitor(String name) {
    SchedulingMonitor mon = runningSchedulingMonitors.get(name);
    try {
      mon.stop();
      LOG.info("Sucessfully stopped monitor=" + mon.getName());
    } catch (Exception e) {
      LOG.warn("Exception while stopping monitor=" + mon.getName(), e);
    }
  }

  private void stopAndRemoveAll() {
    if (!runningSchedulingMonitors.isEmpty()) {
      for (String schedulingMonitorName : runningSchedulingMonitors
          .keySet()) {
        silentlyStopSchedulingMonitor(schedulingMonitorName);
      }
      runningSchedulingMonitors.clear();
    }
  }

  public boolean isRSMEmpty() {
    return runningSchedulingMonitors.isEmpty();
  }

  public boolean isSameConfiguredPolicies(Set<String> configurePoliciesSet) {
    return configurePoliciesSet.equals(runningSchedulingMonitors.keySet());
  }

  public SchedulingMonitor getAvailableSchedulingMonitor() {
    if (isRSMEmpty()) {
      return null;
    }
    for (SchedulingMonitor smon : runningSchedulingMonitors.values()) {
      if (smon.getSchedulingEditPolicy()
          instanceof ProportionalCapacityPreemptionPolicy) {
        return smon;
      }
    }
    return null;
  }

  public synchronized void stop() throws YarnException {
    stopAndRemoveAll();
  }
}
