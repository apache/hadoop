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
package org.apache.hadoop.yarn.server.applicationhistoryservice.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.pajoin;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.server.api.ApplicationContext;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryManager;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.apache.hadoop.yarn.server.timeline.security.TimelineDelegationTokenSecretManagerService;
import org.apache.hadoop.yarn.server.timeline.webapp.TimelineWebServices;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

import com.google.common.annotations.VisibleForTesting;

public class AHSWebApp extends WebApp implements YarnWebParams {

  private ApplicationHistoryManager applicationHistoryManager;
  private TimelineStore timelineStore;
  private TimelineDelegationTokenSecretManagerService secretManagerService;
  private TimelineACLsManager timelineACLsManager;

  private static AHSWebApp instance = null;

  public static AHSWebApp getInstance() {
    if (instance == null) {
      instance = new AHSWebApp();
    }
    return instance;
  }

  @Private
  @VisibleForTesting
  public static void resetInstance() {
    instance = null;
  }

  private AHSWebApp() {

  }

  public ApplicationHistoryManager getApplicationHistoryManager() {
    return applicationHistoryManager;
  }

  public void setApplicationHistoryManager(
      ApplicationHistoryManager applicationHistoryManager) {
    this.applicationHistoryManager = applicationHistoryManager;
  }

  public TimelineStore getTimelineStore() {
    return timelineStore;
  }

  public void setTimelineStore(TimelineStore timelineStore) {
    this.timelineStore = timelineStore;
  }

  public TimelineDelegationTokenSecretManagerService
      getTimelineDelegationTokenSecretManagerService() {
    return secretManagerService;
  }

  public void setTimelineDelegationTokenSecretManagerService(
      TimelineDelegationTokenSecretManagerService secretManagerService) {
    this.secretManagerService = secretManagerService;
  }

  public TimelineACLsManager getTimelineACLsManager() {
    return timelineACLsManager;
  }

  public void setTimelineACLsManager(TimelineACLsManager timelineACLsManager) {
    this.timelineACLsManager = timelineACLsManager;
  }

  @Override
  public void setup() {
    bind(YarnJacksonJaxbJsonProvider.class);
    bind(AHSWebServices.class);
    bind(TimelineWebServices.class);
    bind(GenericExceptionHandler.class);
    bind(ApplicationContext.class).toInstance(applicationHistoryManager);
    bind(TimelineStore.class).toInstance(timelineStore);
    bind(TimelineDelegationTokenSecretManagerService.class).toInstance(
        secretManagerService);
    bind(TimelineACLsManager.class).toInstance(timelineACLsManager);
    route("/", AHSController.class);
    route(pajoin("/apps", APP_STATE), AHSController.class);
    route(pajoin("/app", APPLICATION_ID), AHSController.class, "app");
    route(pajoin("/appattempt", APPLICATION_ATTEMPT_ID), AHSController.class,
      "appattempt");
    route(pajoin("/container", CONTAINER_ID), AHSController.class, "container");
    route(
      pajoin("/logs", NM_NODENAME, CONTAINER_ID, ENTITY_STRING, APP_OWNER,
        CONTAINER_LOG_TYPE), AHSController.class, "logs");
  }
}