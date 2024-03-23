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

package org.apache.hadoop.yarn.server.globalpolicygenerator.applicationcleaner;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.utils.FederationRegistryClient;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGContext;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGUtils;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.DeSelectFields;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ApplicationCleaner is a runnable that cleans up old applications from
 * table applicationsHomeSubCluster in FederationStateStore.
 */
public abstract class ApplicationCleaner implements Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(ApplicationCleaner.class);

  private Configuration conf;
  private GPGContext gpgContext;
  private FederationRegistryClient registryClient;

  private int minRouterSuccessCount;
  private int maxRouterRetry;
  private long routerQueryIntevalMillis;

  public void init(Configuration config, GPGContext context)
      throws YarnException {

    this.gpgContext = context;
    this.conf = config;
    this.registryClient = context.getRegistryClient();

    String routerSpecString =
        this.conf.get(YarnConfiguration.GPG_APPCLEANER_CONTACT_ROUTER_SPEC,
            YarnConfiguration.DEFAULT_GPG_APPCLEANER_CONTACT_ROUTER_SPEC);
    String[] specs = routerSpecString.split(",");
    if (specs.length != 3) {
      throw new YarnException("Expect three comma separated values in "
          + YarnConfiguration.GPG_APPCLEANER_CONTACT_ROUTER_SPEC + " but get "
          + routerSpecString);
    }
    this.minRouterSuccessCount = Integer.parseInt(specs[0]);
    this.maxRouterRetry = Integer.parseInt(specs[1]);
    this.routerQueryIntevalMillis = Long.parseLong(specs[2]);

    if (this.minRouterSuccessCount > this.maxRouterRetry) {
      throw new YarnException("minRouterSuccessCount "
          + this.minRouterSuccessCount
          + " should not be larger than maxRouterRetry" + this.maxRouterRetry);
    }
    if (this.minRouterSuccessCount <= 0) {
      throw new YarnException("minRouterSuccessCount "
          + this.minRouterSuccessCount + " should be positive");
    }

    LOG.info("Initialized AppCleaner with Router query with min success {}, " +
        "max retry {}, retry interval {}.", this.minRouterSuccessCount,
        this.maxRouterRetry,
        DurationFormatUtils.formatDurationISO(this.routerQueryIntevalMillis));
  }

  public GPGContext getGPGContext() {
    return this.gpgContext;
  }

  public FederationRegistryClient getRegistryClient() {
    return this.registryClient;
  }

  /**
   * To determine if the application comes from RM.
   *
   * @param webAppAddress RM webAppAddress.
   * @param applicationId applicationId.
   * @return If it is true, the application can be found in the subCluster.
   * If it is false, the application cannot be found.
   * @throws YarnRuntimeException if get application error.
   */
  public Boolean isApplicationExistsSubCluster(String webAppAddress, ApplicationId applicationId)
      throws YarnRuntimeException {

    try {
      AppInfo appInfo = GPGUtils.invokeRMWebService(webAppAddress,
          RMWSConsts.APPS + "/" + applicationId, AppInfo.class, conf,
           DeSelectFields.DeSelectType.RESOURCE_REQUESTS.toString());

      if(appInfo != null && isApplicationIdEqual(appInfo, applicationId)) {
        return true;
      }
    } catch (Exception e) {
      LOG.error("We cannot get application = {} from webAppAddress = {}.",
          applicationId, webAppAddress, e);
    }

    return false;
  }

  /**
   * Check if the applicationId is equal.
   *
   * @param appInfo appInfo comes from the specified SubCluster.
   * @param applicationId ApplicationId that needs to be checked.
   * @return true, they are equal, can be deleted; false, they are not equal, cannot be deleted.
   */
  private boolean isApplicationIdEqual(AppInfo appInfo, ApplicationId applicationId) {
    String appId = appInfo.getAppId();
    if (appId != null && StringUtils.equals(appId, applicationId.toString())) {
      return true;
    }
    return false;
  }

  @Override
  public abstract void run();
}
