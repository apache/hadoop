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

package org.apache.slider.server.appmaster.actions;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.slider.core.exceptions.ExceptionConverter;
import org.apache.slider.core.exceptions.TriggerClusterTeardownException;
import org.apache.slider.core.main.ExitCodeProvider;
import org.apache.slider.core.main.LauncherExitCodes;
import org.apache.slider.server.appmaster.SliderAppMaster;
import org.apache.slider.server.appmaster.state.AppState;

import java.util.concurrent.TimeUnit;

/**
 * Trigger an AM exit. This is used to build the exit status message for YARN
 */
public class ActionStopSlider extends AsyncAction {

  private int exitCode;
  private FinalApplicationStatus finalApplicationStatus;
  private String message;
  private final Exception ex;

  /**
   * Simple constructor
   * @param name action name
   */
  public ActionStopSlider(String name) {
    super(name);
    this.ex = null;
  }

  /**
   * Stop slider
   * @param name action name
   * @param delay execution delay
   * @param timeUnit delay time unit
   * @param exitCode process exit code
   * @param finalApplicationStatus yarn status
   * @param message message for AM
   */
  public ActionStopSlider(String name,
      long delay,
      TimeUnit timeUnit,
      int exitCode,
      FinalApplicationStatus finalApplicationStatus,
      String message) {
    super(name, delay, timeUnit, ATTR_HALTS_APP);
    this.exitCode = exitCode;
    this.finalApplicationStatus = finalApplicationStatus;
    this.message = message;
    this.ex = null;
  }

  /**
   * Stop slider
   * @param name action name
   * @param exitCode process exit code
   * @param finalApplicationStatus yarn status
   * @param message message for AM
   */
  public ActionStopSlider(String name,
      int exitCode,
      FinalApplicationStatus finalApplicationStatus,
    String message) {
    super(name);
    this.exitCode = exitCode;
    this.finalApplicationStatus = finalApplicationStatus;
    this.message = message;
    this.ex = null;
  }

  /**
   * Simple constructor
   * @param ex teardown exception
   */
  public ActionStopSlider(TriggerClusterTeardownException ex) {
    this("stop",
        ex.getExitCode(),
        ex.getFinalApplicationStatus(),
        ex.getMessage());
  }
  
  /**
   * Build from an exception.
   * <p>
   * If the exception implements
   * {@link ExitCodeProvider} then the exit code is extracted from that
   * @param ex exception.
   */
  public ActionStopSlider(Exception ex) {
    super("stop");
    if (ex instanceof ExitCodeProvider) {
      setExitCode(((ExitCodeProvider)ex).getExitCode());
    } else {
      setExitCode(LauncherExitCodes.EXIT_EXCEPTION_THROWN);
    }
    setFinalApplicationStatus(FinalApplicationStatus.FAILED);
    setMessage(ex.getMessage());
    this.ex = ex;
  }
  
  @Override
  public void execute(SliderAppMaster appMaster,
      QueueAccess queueService,
      AppState appState) throws Exception {
    SliderAppMaster.getLog().info("SliderAppMasterApi.stopCluster: {}",
        message);
    appMaster.onAMStop(this);
  }

  @Override
  public String toString() {
    return String.format("%s:  exit code = %d, %s: %s;",
        name, exitCode, finalApplicationStatus, message) ;
  }

  public int getExitCode() {
    return exitCode;
  }

  public void setExitCode(int exitCode) {
    this.exitCode = exitCode;
  }

  public FinalApplicationStatus getFinalApplicationStatus() {
    return finalApplicationStatus;
  }

  public void setFinalApplicationStatus(FinalApplicationStatus finalApplicationStatus) {
    this.finalApplicationStatus = finalApplicationStatus;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public Exception getEx() {
    return ex;
  }
}
