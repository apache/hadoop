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

package org.apache.hadoop.yarn.server.nodemanager.health;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

/**
 * A {@link HealthReporter} skeleton for regularly checking a specific
 * {@link TimerTask} and obtaining information about it.
 *
 * @see NodeHealthScriptRunner
 */
public abstract class TimedHealthReporterService extends AbstractService
    implements HealthReporter {

  private static final Logger LOG =
      LoggerFactory.getLogger(TimedHealthReporterService.class);

  private boolean isHealthy;
  private String healthReport;
  private long lastReportedTime;

  private Timer timer;
  private TimerTask task;
  private long intervalMs;
  private boolean runBeforeStartup;

  TimedHealthReporterService(String name, long intervalMs) {
    super(name);
    this.isHealthy = true;
    this.healthReport = "";
    this.lastReportedTime = System.currentTimeMillis();
    this.intervalMs = intervalMs;
    this.runBeforeStartup = false;
  }

  TimedHealthReporterService(String name, long intervalMs,
      boolean runBeforeStartup) {
    super(name);
    this.isHealthy = true;
    this.healthReport = "";
    this.lastReportedTime = System.currentTimeMillis();
    this.intervalMs = intervalMs;
    this.runBeforeStartup = runBeforeStartup;
  }

  @VisibleForTesting
  void setTimerTask(TimerTask timerTask) {
    task = timerTask;
  }

  @VisibleForTesting
  TimerTask getTimerTask() {
    return task;
  }

  /**
   * Method used to start the health monitoring.
   */
  @Override
  public void serviceStart() throws Exception {
    if (task == null) {
      throw new Exception("Health reporting task hasn't been set!");
    }
    timer = new Timer("HealthReporterService-Timer", true);
    long delay = 0;
    if (runBeforeStartup) {
      delay = intervalMs;
      task.run();
    }

    timer.scheduleAtFixedRate(task, delay, intervalMs);
    super.serviceStart();
  }

  /**
   * Method used to terminate the health monitoring service.
   */
  @Override
  protected void serviceStop() throws Exception {
    if (timer != null) {
      timer.cancel();
    }
    super.serviceStop();
  }

  @Override
  public boolean isHealthy() {
    return isHealthy;
  }

  /**
   * Sets if the node is healthy or not.
   *
   * @param healthy whether the node is healthy
   */
  protected synchronized void setHealthy(boolean healthy) {
    this.isHealthy = healthy;
  }

  @Override
  public String getHealthReport() {
    return healthReport;
  }

  /**
   * Sets the health report from the node health check. Also set the disks'
   * health info obtained from DiskHealthCheckerService.
   *
   * @param report report String
   */
  private synchronized void setHealthReport(String report) {
    this.healthReport = report;
  }

  @Override
  public long getLastHealthReportTime() {
    return lastReportedTime;
  }

  /**
   * Sets the last run time of the node health check.
   *
   * @param lastReportedTime last reported time in long
   */
  private synchronized void setLastReportedTime(long lastReportedTime) {
    this.lastReportedTime = lastReportedTime;
  }

  synchronized void setHealthyWithoutReport() {
    this.setHealthy(true);
    this.setHealthReport("");
    this.setLastReportedTime(System.currentTimeMillis());
  }

  synchronized void setUnhealthyWithReport(String output) {
    LOG.info("Health status being set as: \"" + output + "\".");
    this.setHealthy(false);
    this.setHealthReport(output);
    this.setLastReportedTime(System.currentTimeMillis());
  }
}
