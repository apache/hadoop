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

package org.apache.hadoop.yarn.server.globalpolicygenerator.subclustercleaner;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The sub-cluster cleaner is one of the GPG's services that periodically checks
 * the membership table in FederationStateStore and mark sub-clusters that have
 * not sent a heartbeat in certain amount of time as LOST.
 */
public class SubClusterCleaner implements Runnable {

  private static final Logger LOG =
      LoggerFactory.getLogger(SubClusterCleaner.class);

  private GPGContext gpgContext;
  private long heartbeatExpirationMillis;

  /**
   * The sub-cluster cleaner runnable is invoked by the sub cluster cleaner
   * service to check the membership table and remove sub clusters that have not
   * sent a heart beat in some amount of time.
   *
   * @param conf configuration.
   * @param gpgContext GPGContext.
   */
  public SubClusterCleaner(Configuration conf, GPGContext gpgContext) {
    this.heartbeatExpirationMillis = conf.getTimeDuration(
        YarnConfiguration.GPG_SUBCLUSTER_EXPIRATION_MS,
        YarnConfiguration.DEFAULT_GPG_SUBCLUSTER_EXPIRATION_MS, TimeUnit.MILLISECONDS);
    this.gpgContext = gpgContext;
    LOG.info("Initialized SubClusterCleaner with heartbeat expiration of {}",
        DurationFormatUtils.formatDurationISO(this.heartbeatExpirationMillis));
  }

  @Override
  public void run() {
    try {
      Date now = new Date();
      LOG.info("SubClusterCleaner at {}", now);

      Map<SubClusterId, SubClusterInfo> infoMap =
          this.gpgContext.getStateStoreFacade().getSubClusters(false, true);

      // Iterate over each sub cluster and check last heartbeat
      for (Map.Entry<SubClusterId, SubClusterInfo> entry : infoMap.entrySet()) {
        SubClusterInfo subClusterInfo = entry.getValue();

        Date lastHeartBeat = new Date(subClusterInfo.getLastHeartBeat());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Checking subcluster {} in state {}, last heartbeat at {}",
              subClusterInfo.getSubClusterId(), subClusterInfo.getState(),
              lastHeartBeat);
        }

        if (subClusterInfo.getState().isUsable()) {
          long timeUntilDeregister = this.heartbeatExpirationMillis
              - (now.getTime() - lastHeartBeat.getTime());
          // Deregister sub-cluster as SC_LOST if last heartbeat too old
          if (timeUntilDeregister < 0) {
            LOG.warn(
                "Deregistering subcluster {} in state {} last heartbeat at {}",
                subClusterInfo.getSubClusterId(), subClusterInfo.getState(),
                new Date(subClusterInfo.getLastHeartBeat()));
            try {
              this.gpgContext.getStateStoreFacade().deregisterSubCluster(
                  subClusterInfo.getSubClusterId(), SubClusterState.SC_LOST);
            } catch (Exception e) {
              LOG.error("deregisterSubCluster failed on subcluster "
                  + subClusterInfo.getSubClusterId(), e);
            }
          } else if (LOG.isDebugEnabled()) {
            LOG.debug("Time until deregister for subcluster {}: {}",
                entry.getKey(),
                DurationFormatUtils.formatDurationISO(timeUntilDeregister));
          }
        }
      }
    } catch (Throwable e) {
      LOG.error("Subcluster cleaner fails: ", e);
    }
  }

}
