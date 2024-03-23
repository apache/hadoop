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

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The default ApplicationCleaner that cleans up old applications from table
 * applicationsHomeSubCluster in FederationStateStore.
 */
public class DefaultApplicationCleaner extends ApplicationCleaner {
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultApplicationCleaner.class);

  @Override
  public void run() {
    Date now = new Date();
    LOG.info("Application cleaner run at time {}", now);

    FederationStateStoreFacade facade = getGPGContext().getStateStoreFacade();

    try {
      // Step1. Get the candidate list from StateStore before calling router
      List<ApplicationHomeSubCluster> applicationHomeSubClusters =
          facade.getApplicationsHomeSubCluster();
      LOG.info("FederationStateStore has {} applications.", applicationHomeSubClusters.size());

      // Step2. Get the list of active subClusters
      Map<SubClusterId, SubClusterInfo> subClusters = facade.getSubClusters(true);

      // Step3. We iterate through all application lists.
      for (ApplicationHomeSubCluster app : applicationHomeSubClusters) {
        ApplicationId applicationId = app.getApplicationId();
        SubClusterId homeSubCluster = app.getHomeSubCluster();
        if (subClusters.containsKey(homeSubCluster)) {
          SubClusterInfo subClusterInfo = subClusters.get(homeSubCluster);
          String rmWebAddress = subClusterInfo.getRMWebServiceAddress();
          // If the application no longer exists in the subCluster, it can be deleted.
          if (!isApplicationExistsSubCluster(rmWebAddress, applicationId)) {
            try {
              // Delete application from homeSubCluster
              facade.deleteApplicationHomeSubCluster(app.getApplicationId());
              // Delete application from registry
              getRegistryClient().removeAppFromRegistry(applicationId, true);
            } catch (Exception e) {
              LOG.error("deleteApplicationHomeSubCluster failed at application {}.",
                  applicationId, e);
            }
          } else {
            LOG.debug("Find application [{}] exists In SubCluster [] and skip deleting.",
                applicationId, homeSubCluster);
          }
        }
      }
    } catch (Throwable e) {
      LOG.error("Application cleaner started at time {} fails. ", now, e);
    }
  }
}
