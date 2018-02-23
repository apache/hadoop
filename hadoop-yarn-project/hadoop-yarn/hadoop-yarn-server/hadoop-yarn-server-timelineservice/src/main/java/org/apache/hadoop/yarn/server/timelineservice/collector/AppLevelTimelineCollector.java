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

package org.apache.hadoop.yarn.server.timelineservice.collector;

import java.util.concurrent.Future;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service that handles writes to the timeline service and writes them to the
 * backing storage for a given YARN application.
 *
 * App-related lifecycle management is handled by this service.
 */
@Private
@Unstable
public class AppLevelTimelineCollector extends TimelineCollector {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineCollector.class);

  private final ApplicationId appId;
  private final String appUser;
  private final TimelineCollectorContext context;
  private UserGroupInformation currentUser;
  private Token<TimelineDelegationTokenIdentifier> delegationTokenForApp;
  private long tokenMaxDate = 0;
  private String tokenRenewer;
  private Future<?> renewalOrRegenerationFuture;

  public AppLevelTimelineCollector(ApplicationId appId) {
    this(appId, null);
  }

  public AppLevelTimelineCollector(ApplicationId appId, String user) {
    super(AppLevelTimelineCollector.class.getName() + " - " + appId.toString());
    Preconditions.checkNotNull(appId, "AppId shouldn't be null");
    this.appId = appId;
    this.appUser = user;
    context = new TimelineCollectorContext();
  }

  public UserGroupInformation getCurrentUser() {
    return currentUser;
  }

  public String getAppUser() {
    return appUser;
  }

  void setDelegationTokenAndFutureForApp(
      Token<TimelineDelegationTokenIdentifier> token,
      Future<?> appRenewalOrRegenerationFuture, long tknMaxDate,
      String renewer) {
    this.delegationTokenForApp = token;
    this.tokenMaxDate = tknMaxDate;
    this.tokenRenewer = renewer;
    this.renewalOrRegenerationFuture = appRenewalOrRegenerationFuture;
  }

  void setRenewalOrRegenerationFutureForApp(
      Future<?> appRenewalOrRegenerationFuture) {
    this.renewalOrRegenerationFuture = appRenewalOrRegenerationFuture;
  }

  void cancelRenewalOrRegenerationFutureForApp() {
    if (renewalOrRegenerationFuture != null &&
        !renewalOrRegenerationFuture.isDone()) {
      renewalOrRegenerationFuture.cancel(true);
    }
  }

  long getAppDelegationTokenMaxDate() {
    return tokenMaxDate;
  }

  String getAppDelegationTokenRenewer() {
    return tokenRenewer;
  }

  @VisibleForTesting
  public Token<TimelineDelegationTokenIdentifier> getDelegationTokenForApp() {
    return this.delegationTokenForApp;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    context.setClusterId(conf.get(YarnConfiguration.RM_CLUSTER_ID,
        YarnConfiguration.DEFAULT_RM_CLUSTER_ID));
    // Set the default values, which will be updated with an RPC call to get the
    // context info from NM.
    // Current user usually is not the app user, but keep this field non-null
    currentUser = UserGroupInformation.getCurrentUser();
    context.setUserId(currentUser.getShortUserName());
    context.setAppId(appId.toString());
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    cancelRenewalOrRegenerationFutureForApp();
    super.serviceStop();
  }

  @Override
  public TimelineCollectorContext getTimelineEntityContext() {
    return context;
  }
}
