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
package org.apache.hadoop.yarn.server.router.cleaner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The SubClusterCleaner thread is used to check whether the SubCluster
 * has exceeded the heartbeat time.
 * If the SubCluster heartbeat time exceeds 30 mins, set the SubCluster to LOST.
 * Check the thread every 1 mins, check once.
 */
public class SubClusterCleaner implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(SubClusterCleaner.class);
  private FederationStateStoreFacade federationFacade;
  private long heartbeatExpirationMillis;

  public SubClusterCleaner(Configuration conf) {
    federationFacade = FederationStateStoreFacade.getInstance();
    this.heartbeatExpirationMillis =
        conf.getTimeDuration(YarnConfiguration.ROUTER_SUBCLUSTER_EXPIRATION_TIME,
        YarnConfiguration.DEFAULT_ROUTER_SUBCLUSTER_EXPIRATION_TIME, TimeUnit.MILLISECONDS);
  }

  @Override
  public void run() {
    try {
      // Step1. Get Current Time.
      Date now = new Date();
      LOG.info("SubClusterCleaner at {}.", now);

      Map<SubClusterId, SubClusterInfo> subClusters = federationFacade.getSubClusters(true);

      for (Map.Entry<SubClusterId, SubClusterInfo> subCluster : subClusters.entrySet()) {
        // Step2. Get information about subClusters.
        SubClusterId subClusterId = subCluster.getKey();
        SubClusterInfo subClusterInfo = subCluster.getValue();
        SubClusterState subClusterState = subClusterInfo.getState();
        long lastHeartBeatTime = subClusterInfo.getLastHeartBeat();

        // We Only Check SubClusters in NEW and RUNNING states
        if (subClusterState.isUsable()) {
          long heartBeatInterval = now.getTime() - lastHeartBeatTime;
          try {
            // HeartBeat Interval Exceeds Expiration Time
            if (heartBeatInterval > heartbeatExpirationMillis) {
              LOG.info("Deregister SubCluster {} in state {} last heartbeat at {}.",
                  subClusterId, subClusterState, new Date(lastHeartBeatTime));
              federationFacade.deregisterSubCluster(subClusterId, SubClusterState.SC_LOST);
            }
          } catch (YarnException e) {
            LOG.error("deregisterSubCluster failed on SubCluster {}.", subClusterId, e);
          }
        } else {
          LOG.debug("SubCluster {} in state {} last heartbeat at {}, " +
              "heartbeat interval < 30mins, no need for Deregister.",
              subClusterId, subClusterState, new Date(lastHeartBeatTime));
        }
      }
    } catch (Throwable e) {
      LOG.error("SubClusterCleaner Fails.", e);
    }
  }
}
