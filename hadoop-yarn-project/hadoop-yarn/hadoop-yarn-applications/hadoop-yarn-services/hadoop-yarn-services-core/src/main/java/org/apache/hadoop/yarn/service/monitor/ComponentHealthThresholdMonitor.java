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

package org.apache.hadoop.yarn.service.monitor;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.service.component.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitors the health of containers of a specific component at a regular
 * interval. It takes necessary actions when the health of a component drops
 * below a desired threshold.
 */
public class ComponentHealthThresholdMonitor implements Runnable {
  private static final Logger LOG = LoggerFactory
      .getLogger(ComponentHealthThresholdMonitor.class);
  private final Component component;
  private final int healthThresholdPercent;
  private final long healthThresholdWindowSecs;
  private final long healthThresholdWindowNanos;
  private long firstOccurrenceTimestamp = 0;
  // Sufficient logging happens when component health is below threshold.
  // However, there has to be some logging when it is above threshold, otherwise
  // service owners have no idea how the health is fluctuating. So let's log
  // whenever there is a change in component health, thereby preventing
  // excessive logging on every poll.
  private float prevReadyContainerFraction = 0;

  public ComponentHealthThresholdMonitor(Component component,
      int healthThresholdPercent, long healthThresholdWindowSecs) {
    this.component = component;
    this.healthThresholdPercent = healthThresholdPercent;
    this.healthThresholdWindowSecs = healthThresholdWindowSecs;
    this.healthThresholdWindowNanos = TimeUnit.NANOSECONDS
        .convert(healthThresholdWindowSecs, TimeUnit.SECONDS);
  }

  @Override
  public void run() {
    LOG.debug("ComponentHealthThresholdMonitor run method");
    // Perform container health checks against desired threshold
    long desiredContainerCount = component.getNumDesiredInstances();
    // If desired container count for this component is 0 then nothing to do
    if (desiredContainerCount == 0) {
      return;
    }
    long readyContainerCount = component.getNumReadyInstances();
    float thresholdFraction = (float) healthThresholdPercent / 100;
    // No possibility of div by 0 since desiredContainerCount won't be 0 here
    float readyContainerFraction = (float) readyContainerCount
        / desiredContainerCount;
    boolean healthChanged = false;
    if (Math.abs(
        readyContainerFraction - prevReadyContainerFraction) > .0000001) {
      prevReadyContainerFraction = readyContainerFraction;
      healthChanged = true;
    }
    String readyContainerPercentStr = String.format("%.2f",
        readyContainerFraction * 100);
    // Check if the current ready container percent is less than the
    // threshold percent
    if (readyContainerFraction < thresholdFraction) {
      // Check if it is the first occurrence and if yes set the timestamp
      long currentTimestamp = System.nanoTime();
      if (firstOccurrenceTimestamp == 0) {
        firstOccurrenceTimestamp = currentTimestamp;
        Date date = new Date();
        LOG.info(
            "[COMPONENT {}] Health has gone below threshold. Starting health "
                + "threshold timer at ts = {} ({})",
            component.getName(), date.getTime(), date);
      }
      long elapsedTime = currentTimestamp - firstOccurrenceTimestamp;
      long elapsedTimeSecs = TimeUnit.SECONDS.convert(elapsedTime,
          TimeUnit.NANOSECONDS);
      LOG.warn(
          "[COMPONENT {}] Current health {}% is below health threshold of "
              + "{}% for {} secs (threshold window = {} secs)",
          component.getName(), readyContainerPercentStr,
          healthThresholdPercent, elapsedTimeSecs, healthThresholdWindowSecs);
      if (elapsedTime > healthThresholdWindowNanos) {
        LOG.warn(
            "[COMPONENT {}] Current health {}% has been below health "
                + "threshold of {}% for {} secs (threshold window = {} secs)",
            component.getName(), readyContainerPercentStr,
            healthThresholdPercent, elapsedTimeSecs, healthThresholdWindowSecs);
        // Trigger service stop
        String exitDiag = String.format(
            "Service is being killed because container health for component "
                + "%s was %s%% (health threshold = %d%%) for %d secs "
                + "(threshold window = %d secs)",
            component.getName(), readyContainerPercentStr,
            healthThresholdPercent, elapsedTimeSecs, healthThresholdWindowSecs);
        // Append to global diagnostics that will be reported to RM.
        component.getScheduler().getDiagnostics().append(exitDiag);
        LOG.warn(exitDiag);
        // Sleep for 5 seconds in hope that the state can be recorded in ATS.
        // In case there's a client polling the component state, it can be
        // notified.
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          LOG.error("Interrupted on sleep while exiting.", e);
        }
        ExitUtil.terminate(-1);
      }
    } else {
      String logMsg = "[COMPONENT {}] Health threshold = {}%, Current health "
          + "= {}% (Current Ready count = {}, Desired count = {})";
      if (healthChanged) {
        LOG.info(logMsg, component.getName(), healthThresholdPercent,
            readyContainerPercentStr, readyContainerCount,
            desiredContainerCount);
      } else {
        LOG.debug(logMsg, component.getName(), healthThresholdPercent,
            readyContainerPercentStr, readyContainerCount,
            desiredContainerCount);
      }
      // The container health might have recovered above threshold after being
      // below for less than the threshold window amount of time. So we need
      // to reset firstOccurrenceTimestamp to 0.
      if (firstOccurrenceTimestamp != 0) {
        Date date = new Date();
        LOG.info(
            "[COMPONENT {}] Health recovered above threshold at ts = {} ({})",
            component.getName(), date.getTime(), date);
        firstOccurrenceTimestamp = 0;
      }
    }
  }
}
