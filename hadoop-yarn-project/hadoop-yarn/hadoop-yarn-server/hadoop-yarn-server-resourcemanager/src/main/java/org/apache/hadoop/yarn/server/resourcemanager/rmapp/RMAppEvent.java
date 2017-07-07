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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.AbstractEvent;

public class RMAppEvent extends AbstractEvent<RMAppEventType>{

  private final ApplicationId appId;
  private final String diagnosticMsg;
  private boolean storeAppInfo;

  public RMAppEvent(ApplicationId appId, RMAppEventType type) {
    this(appId, type, "");
  }

  public RMAppEvent(ApplicationId appId, RMAppEventType type,
      String diagnostic) {
    super(type);
    this.appId = appId;
    this.diagnosticMsg = diagnostic;
    this.storeAppInfo = true;
  }

  /**
   * Constructor to create RM Application Event type.
   *
   * @param appId application Id
   * @param type RM Event type
   * @param diagnostic Diagnostic message for event
   * @param storeApp Application should be saved or not
   */
  public RMAppEvent(ApplicationId appId, RMAppEventType type, String diagnostic,
      boolean storeApp) {
    this(appId, type, diagnostic);
    this.storeAppInfo = storeApp;
  }

  public ApplicationId getApplicationId() {
    return this.appId;
  }

  public String getDiagnosticMsg() {
    return this.diagnosticMsg;
  }

  /**
   * Store application to state store or not.
   *
   * @return boolean application should be saved to store.
   */
  public boolean doStoreAppInfo() {
    return storeAppInfo;
  }
}
