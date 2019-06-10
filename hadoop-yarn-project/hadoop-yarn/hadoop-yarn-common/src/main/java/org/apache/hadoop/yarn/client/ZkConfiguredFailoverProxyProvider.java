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

package org.apache.hadoop.yarn.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ActiveStandbyElector;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ActiveRMInfoProto;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.HashSet;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ZkConfiguredFailoverProxyProvider<T>
    extends ConfiguredRMFailoverProxyProvider<T> implements Watcher {

  private static final Log LOG = LogFactory
      .getLog(ZkConfiguredFailoverProxyProvider.class);
  private int retryIndex = 1;

  @Override
  public void init(Configuration configuration, RMProxy<T> rmProxy,
                   Class<T> protocol) {
    super.init(configuration, rmProxy, protocol);
    if (!HAUtil.isHAEnabled(conf)) {
      throw new YarnRuntimeException("ZkConfiguredFailoverProxyProvider " +
          "is only supported in Yarn RM HA mode");
    }
  }

  @Override
  public synchronized ProxyInfo<T> getProxy() {
    if (retryIndex == 0) {
      String rmId = HAUtil.getRMHAId(conf);
      T current = proxies.get(rmId);
      if (current != null) {
        RPC.stopProxy(current);
      }
      current = getProxyInternal();
      proxies.put(rmId, current);
      return new ProxyInfo<T>(current, rmId);
    } else {
      return super.getProxy();
    }
  }

  @Override
  public synchronized void performFailover(T currentProxy) {
    retryIndex = (retryIndex + 1) % (rmServiceIds.length + 1);
    if (retryIndex == 0) {
      ActiveRMInfoProto info = getActiveRMInfoProto();
      LOG.info("Get active rm info from zookeeper: " + info);
      if (info != null && info.hasRmId()) {
        String rmId = info.getRmId();
        HashSet<String> rmIdSet = new HashSet<String>(HAUtil.getRMHAIds(conf));
        if (!rmIdSet.contains(rmId)) {
          rmIdSet.add(rmId);
          String updateRmIds = StringUtils.arrayToString(rmIdSet.toArray(new String[0]));
          conf.set(YarnConfiguration.RM_HA_IDS, updateRmIds);
        }
        LOG.info("Update RM config for id: " + rmId);
        conf.set(YarnConfiguration.RM_HA_ID, rmId);
        // set RM_ADDRESS/RM_ADMIN_ADDRESS/RM_SCHEDULER_ADDRESS/RM_RESOURCE_TRACKER_ADDRESS
        conf.set(HAUtil.addSuffix(YarnConfiguration.RM_ADDRESS, rmId),
            info.getRmAddr());
        conf.set(HAUtil.addSuffix(YarnConfiguration.RM_ADMIN_ADDRESS, rmId),
            info.getRmAdminAddr());
        conf.set(HAUtil.addSuffix(YarnConfiguration.RM_SCHEDULER_ADDRESS, rmId),
            info.getRmSchedulerAddr());
        conf.set(HAUtil.addSuffix(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, rmId),
            info.getRmResourceTrackerAddr());
        return;
      }
    }
    super.performFailover(currentProxy);
  }

  protected ActiveRMInfoProto getActiveRMInfoProto() {
    ZooKeeper zk = null;
    try {
      String zkQuorum = conf.get(YarnConfiguration.RM_ZK_ADDRESS);
      if (zkQuorum == null) {
        throw new YarnRuntimeException("Embedded automatic failover " +
            "is enabled, but " + YarnConfiguration.RM_ZK_ADDRESS +
            " is not set");
      }
      long zkSessionTimeout = conf.getLong(YarnConfiguration.RM_ZK_TIMEOUT_MS,
          YarnConfiguration.DEFAULT_RM_ZK_TIMEOUT_MS);

      String zkBasePath = conf.get(YarnConfiguration.AUTO_FAILOVER_ZK_BASE_PATH,
          YarnConfiguration.DEFAULT_AUTO_FAILOVER_ZK_BASE_PATH);
      String clusterId = YarnConfiguration.getClusterId(conf);
      String breadCrumbZnode = zkBasePath + "/" + clusterId
          + "/" + ActiveStandbyElector.BREADCRUMB_FILENAME;
      zk = new ZooKeeper(zkQuorum, (int) zkSessionTimeout, this, true);
      byte[] data = zk.getData(breadCrumbZnode, false, new Stat());
      return ActiveRMInfoProto.parseFrom(data);
    } catch (Exception e) {
      LOG.info("Failed to get the active rm info from zookeeper", e);
      return null;
    } finally {
      if (zk != null) {
        try {
          zk.close();
        } catch (Throwable e) {
          LOG.error("Failed to close zookeeper", e);
        }
      }
    }
  }

  public void process(WatchedEvent event) {
    LOG.debug("Zookeeper event: " + event);
  }
}
