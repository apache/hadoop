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

package org.apache.hadoop.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * The class which provides functionality of checking the health of the node
 * using the configured node health script and reporting back to the service
 * for which the health checker has been asked to report.
 */
public class NodeHealthScriptRunner extends AbstractService {

  private static final Logger LOG =
      LoggerFactory.getLogger(NodeHealthScriptRunner.class);

  /** Absolute path to the health script. */
  private String nodeHealthScript;
  /** Delay after which node health script to be executed */
  private long intervalTime;
  /** Time after which the script should be timedout */
  private long scriptTimeout;
  /** Timer used to schedule node health monitoring script execution */
  private Timer nodeHealthScriptScheduler;

  /** ShellCommandExecutor used to execute monitoring script */
  ShellCommandExecutor shexec = null;

  /** Pattern used for searching in the output of the node health script */
  static private final String ERROR_PATTERN = "ERROR";

  /** Time out error message */
  public static final String NODE_HEALTH_SCRIPT_TIMED_OUT_MSG = "Node health script timed out";

  private boolean isHealthy;

  private String healthReport;

  private long lastReportedTime;

  private TimerTask timer;
  
  private enum HealthCheckerExitStatus {
    SUCCESS,
    TIMED_OUT,
    FAILED_WITH_EXIT_CODE,
    FAILED_WITH_EXCEPTION,
    FAILED
  }


  /**
   * Class which is used by the {@link Timer} class to periodically execute the
   * node health script.
   * 
   */
  private class NodeHealthMonitorExecutor extends TimerTask {

    String exceptionStackTrace = "";

    public NodeHealthMonitorExecutor(String[] args) {
      ArrayList<String> execScript = new ArrayList<String>();
      execScript.add(nodeHealthScript);
      if (args != null) {
        execScript.addAll(Arrays.asList(args));
      }
      shexec = new ShellCommandExecutor(execScript
          .toArray(new String[execScript.size()]), null, null, scriptTimeout);
    }

    @Override
    public void run() {
      HealthCheckerExitStatus status = HealthCheckerExitStatus.SUCCESS;
      try {
        shexec.execute();
      } catch (ExitCodeException e) {
        // ignore the exit code of the script
        status = HealthCheckerExitStatus.FAILED_WITH_EXIT_CODE;
        // On Windows, we will not hit the Stream closed IOException
        // thrown by stdout buffered reader for timeout event.
        if (Shell.WINDOWS && shexec.isTimedOut()) {
          status = HealthCheckerExitStatus.TIMED_OUT;
        }
      } catch (Exception e) {
        LOG.warn("Caught exception : " + e.getMessage());
        if (!shexec.isTimedOut()) {
          status = HealthCheckerExitStatus.FAILED_WITH_EXCEPTION;
        } else {
          status = HealthCheckerExitStatus.TIMED_OUT;
        }
        exceptionStackTrace = StringUtils.stringifyException(e);
      } finally {
        if (status == HealthCheckerExitStatus.SUCCESS) {
          if (hasErrors(shexec.getOutput())) {
            status = HealthCheckerExitStatus.FAILED;
          }
        }
        reportHealthStatus(status);
      }
    }

    /**
     * Method which is used to parse output from the node health monitor and
     * send to the report address.
     * 
     * The timed out script or script which causes IOException output is
     * ignored.
     * 
     * The node is marked unhealthy if
     * <ol>
     * <li>The node health script times out</li>
     * <li>The node health scripts output has a line which begins with ERROR</li>
     * <li>An exception is thrown while executing the script</li>
     * </ol>
     * If the script throws {@link IOException} or {@link ExitCodeException} the
     * output is ignored and node is left remaining healthy, as script might
     * have syntax error.
     * 
     * @param status
     */
    void reportHealthStatus(HealthCheckerExitStatus status) {
      long now = System.currentTimeMillis();
      switch (status) {
      case SUCCESS:
        setHealthStatus(true, "", now);
        break;
      case TIMED_OUT:
        setHealthStatus(false, NODE_HEALTH_SCRIPT_TIMED_OUT_MSG);
        break;
      case FAILED_WITH_EXCEPTION:
        setHealthStatus(false, exceptionStackTrace);
        break;
      case FAILED_WITH_EXIT_CODE:
        setHealthStatus(true, "", now);
        break;
      case FAILED:
        setHealthStatus(false, shexec.getOutput());
        break;
      }
    }

    /**
     * Method to check if the output string has line which begins with ERROR.
     * 
     * @param output
     *          string
     * @return true if output string has error pattern in it.
     */
    private boolean hasErrors(String output) {
      String[] splits = output.split("\n");
      for (String split : splits) {
        if (split.startsWith(ERROR_PATTERN)) {
          return true;
        }
      }
      return false;
    }
  }

  public NodeHealthScriptRunner(String scriptName, long chkInterval, long timeout,
      String[] scriptArgs) {
    super(NodeHealthScriptRunner.class.getName());
    this.lastReportedTime = System.currentTimeMillis();
    this.isHealthy = true;
    this.healthReport = "";
    this.nodeHealthScript = scriptName;
    this.intervalTime = chkInterval;
    this.scriptTimeout = timeout;
    this.timer = new NodeHealthMonitorExecutor(scriptArgs);
  }

  /*
   * Method which initializes the values for the script path and interval time.
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  /**
   * Method used to start the Node health monitoring.
   * 
   */
  @Override
  protected void serviceStart() throws Exception {
    nodeHealthScriptScheduler = new Timer("NodeHealthMonitor-Timer", true);
    // Start the timer task immediately and
    // then periodically at interval time.
    nodeHealthScriptScheduler.scheduleAtFixedRate(timer, 0, intervalTime);
    super.serviceStart();
  }

  /**
   * Method used to terminate the node health monitoring service.
   * 
   */
  @Override
  protected void serviceStop() {
    if (nodeHealthScriptScheduler != null) {
      nodeHealthScriptScheduler.cancel();
    }
    if (shexec != null) {
      Process p = shexec.getProcess();
      if (p != null) {
        p.destroy();
      }
    }
  }

  /**
   * Gets the if the node is healthy or not
   * 
   * @return true if node is healthy
   */
  public boolean isHealthy() {
    return isHealthy;
  }

  /**
   * Sets if the node is healthy or not considering disks' health also.
   * 
   * @param isHealthy
   *          if or not node is healthy
   */
  private synchronized void setHealthy(boolean isHealthy) {
    this.isHealthy = isHealthy;
  }

  /**
   * Returns output from health script. if node is healthy then an empty string
   * is returned.
   * 
   * @return output from health script
   */
  public String getHealthReport() {
    return healthReport;
  }

  /**
   * Sets the health report from the node health script. Also set the disks'
   * health info obtained from DiskHealthCheckerService.
   *
   * @param healthReport
   */
  private synchronized void setHealthReport(String healthReport) {
    this.healthReport = healthReport;
  }
  
  /**
   * Returns time stamp when node health script was last run.
   * 
   * @return timestamp when node health script was last run
   */
  public long getLastReportedTime() {
    return lastReportedTime;
  }

  /**
   * Sets the last run time of the node health script.
   * 
   * @param lastReportedTime
   */
  private synchronized void setLastReportedTime(long lastReportedTime) {
    this.lastReportedTime = lastReportedTime;
  }

  /**
   * Method used to determine if or not node health monitoring service should be
   * started or not. Returns true if following conditions are met:
   * 
   * <ol>
   * <li>Path to Node health check script is not empty</li>
   * <li>Node health check script file exists</li>
   * </ol>
   * 
   * @return true if node health monitoring service can be started.
   */
  public static boolean shouldRun(String healthScript) {
    if (healthScript == null || healthScript.trim().isEmpty()) {
      return false;
    }
    File f = new File(healthScript);
    return f.exists() && FileUtil.canExecute(f);
  }

  private synchronized void setHealthStatus(boolean isHealthy, String output) {
		LOG.info("health status being set as " + output);
    this.setHealthy(isHealthy);
    this.setHealthReport(output);
  }
  
  private synchronized void setHealthStatus(boolean isHealthy, String output,
      long time) {
	LOG.info("health status being set as " + output);
    this.setHealthStatus(isHealthy, output);
    this.setLastReportedTime(time);
  }

  /**
   * Used only by tests to access the timer task directly
   * @return the timer task
   */
  public TimerTask getTimerTask() {
    return timer;
  }
}
