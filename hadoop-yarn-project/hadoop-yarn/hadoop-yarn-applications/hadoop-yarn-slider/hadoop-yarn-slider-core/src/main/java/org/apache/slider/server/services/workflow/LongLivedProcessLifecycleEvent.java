/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.server.services.workflow;

/**
 * Callback when a long-lived application exits
 */
public interface LongLivedProcessLifecycleEvent {

  /**
   * Callback when a process is started
   * @param process the process invoking the callback
   */
  void onProcessStarted(LongLivedProcess process);

  /**
   * Callback when a process has finished
   * @param process the process invoking the callback
   * @param exitCode exit code from the process
   * @param signCorrectedCode the code- as sign corrected
   */
  void onProcessExited(LongLivedProcess process,
      int exitCode,
      int signCorrectedCode);
}
