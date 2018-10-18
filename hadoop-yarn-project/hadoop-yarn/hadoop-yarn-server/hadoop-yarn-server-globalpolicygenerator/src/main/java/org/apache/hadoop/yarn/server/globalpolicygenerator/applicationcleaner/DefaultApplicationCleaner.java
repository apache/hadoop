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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

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
    Set<ApplicationId> candidates = new HashSet<ApplicationId>();
    try {
      List<ApplicationHomeSubCluster> response =
          facade.getApplicationsHomeSubCluster();
      for (ApplicationHomeSubCluster app : response) {
        candidates.add(app.getApplicationId());
      }
      LOG.info("{} app entries in FederationStateStore", candidates.size());

      Set<ApplicationId> routerApps = getRouterKnownApplications();
      LOG.info("{} known applications from Router", routerApps.size());

      candidates = Sets.difference(candidates, routerApps);
      LOG.info("Deleting {} applications from statestore", candidates.size());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Apps to delete: ", candidates.stream().map(Object::toString)
            .collect(Collectors.joining(",")));
      }
      for (ApplicationId appId : candidates) {
        try {
          facade.deleteApplicationHomeSubCluster(appId);
        } catch (Exception e) {
          LOG.error(
              "deleteApplicationHomeSubCluster failed at application " + appId,
              e);
        }
      }

      // Clean up registry entries
      cleanupAppRecordInRegistry(routerApps);
    } catch (Throwable e) {
      LOG.error("Application cleaner started at time " + now + " fails: ", e);
    }
  }

}
