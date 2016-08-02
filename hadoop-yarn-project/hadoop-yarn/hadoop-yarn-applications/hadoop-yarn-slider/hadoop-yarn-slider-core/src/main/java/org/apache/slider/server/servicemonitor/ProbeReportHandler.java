/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.slider.server.servicemonitor;

/**
 * This interface is for use by the Poll Workers to send events to the reporters.
 *
 * It is up the reporters what to do with the specific events.
 */
public interface ProbeReportHandler {

  /**
   * The probe process has changed state. 
   * @param probePhase the new process phrase
   */
  void probeProcessStateChange(ProbePhase probePhase);

  /**
   * Report a probe outcome
   * @param phase the current phase of probing
   * @param status the probe status
   */
  void probeResult(ProbePhase phase, ProbeStatus status);

  /**
   * A probe has failed
   */
  void probeFailure(ProbeFailedException exception);

  /**
   * A probe has just booted
   * @param status probe status
   */
  void probeBooted(ProbeStatus status);

  boolean commence(String name, String description);

  void unregister();

  /**
   * A heartbeat event should be raised
   * @param status the probe status
   */
  void heartbeat(ProbeStatus status);

  /**
   * A probe has timed out
   * @param currentPhase the current execution phase
   * @param probe the probe that timed out
   * @param lastStatus the last status that was successfully received -which is implicitly 
   * not the status of the timed out probe
   * @param currentTime the current time
   */
  void probeTimedOut(ProbePhase currentPhase,
                     Probe probe,
                     ProbeStatus lastStatus,
                     long currentTime);

  /**
   * Event to say that the live probe cycle completed so the entire
   * system can be considered functional.
   */
  void liveProbeCycleCompleted();
}
