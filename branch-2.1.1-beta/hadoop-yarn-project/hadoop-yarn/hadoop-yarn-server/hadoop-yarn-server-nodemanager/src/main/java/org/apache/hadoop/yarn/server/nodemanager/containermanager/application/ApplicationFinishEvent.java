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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.application;

import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * Finish/abort event
 */
public class ApplicationFinishEvent extends ApplicationEvent {
  private final String diagnostic;

  /**
   * Application event to abort all containers associated with the app
   * @param appId to abort containers
   * @param diagnostic reason for the abort
   */
  public ApplicationFinishEvent(ApplicationId appId, String diagnostic) {
    super(appId, ApplicationEventType.FINISH_APPLICATION);
    this.diagnostic = diagnostic;
  }

  /**
   * Why the app was aborted
   * @return diagnostic message
   */
  public String getDiagnostic() {
    return diagnostic;
  }
}
