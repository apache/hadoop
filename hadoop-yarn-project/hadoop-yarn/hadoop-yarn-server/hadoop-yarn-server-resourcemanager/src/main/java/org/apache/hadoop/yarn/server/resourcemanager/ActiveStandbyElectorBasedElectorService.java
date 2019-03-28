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
package org.apache.hadoop.yarn.server.resourcemanager;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.ActiveStandbyElector;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ZKUtil;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Leader election implementation that uses {@link ActiveStandbyElector}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ActiveStandbyElectorBasedElectorService extends AbstractService
    implements EmbeddedElector,
    ActiveStandbyElector.ActiveStandbyElectorCallback {
  private static final Logger LOG = LoggerFactory.
      getLogger(ActiveStandbyElectorBasedElectorService.class.getName());
  private static final HAServiceProtocol.StateChangeRequestInfo req =
      new HAServiceProtocol.StateChangeRequestInfo(
          HAServiceProtocol.RequestSource.REQUEST_BY_ZKFC);

  private ResourceManager rm;

  private byte[] localActiveNodeInfo;
  private ActiveStandbyElector elector;
  private long zkSessionTimeout;
  private Timer zkDisconnectTimer;
  @VisibleForTesting
  final Object zkDisconnectLock = new Object();

  ActiveStandbyElectorBasedElectorService(ResourceManager rm) {
    super(ActiveStandbyElectorBasedElectorService.class.getName());
    this.rm = rm;
  }

  @Override
  protected void serviceInit(Configuration conf)
      throws Exception {
    conf = conf instanceof YarnConfiguration
        ? conf
        : new YarnConfiguration(conf);

    String zkQuorum = conf.get(YarnConfiguration.RM_ZK_ADDRESS);
    if (zkQuorum == null) {
      throw new YarnRuntimeException("Embedded automatic failover " +
          "is enabled, but " + YarnConfiguration.RM_ZK_ADDRESS +
          " is not set");
    }

    String rmId = HAUtil.getRMHAId(conf);
    String clusterId = YarnConfiguration.getClusterId(conf);
    localActiveNodeInfo = createActiveNodeInfo(clusterId, rmId);

    String zkBasePath = conf.get(YarnConfiguration.AUTO_FAILOVER_ZK_BASE_PATH,
        YarnConfiguration.DEFAULT_AUTO_FAILOVER_ZK_BASE_PATH);
    String electionZNode = zkBasePath + "/" + clusterId;

    zkSessionTimeout = conf.getLong(YarnConfiguration.RM_ZK_TIMEOUT_MS,
        YarnConfiguration.DEFAULT_RM_ZK_TIMEOUT_MS);

    List<ACL> zkAcls = ZKCuratorManager.getZKAcls(conf);
    List<ZKUtil.ZKAuthInfo> zkAuths = ZKCuratorManager.getZKAuths(conf);

    int maxRetryNum =
        conf.getInt(YarnConfiguration.RM_HA_FC_ELECTOR_ZK_RETRIES_KEY, conf
          .getInt(CommonConfigurationKeys.HA_FC_ELECTOR_ZK_OP_RETRIES_KEY,
            CommonConfigurationKeys.HA_FC_ELECTOR_ZK_OP_RETRIES_DEFAULT));
    elector = new ActiveStandbyElector(zkQuorum, (int) zkSessionTimeout,
        electionZNode, zkAcls, zkAuths, this, maxRetryNum, false);

    elector.ensureParentZNode();
    if (!isParentZnodeSafe(clusterId)) {
      notifyFatalError(String.format("invalid data in znode, %s, " +
          "which may require the state store to be reformatted",
          electionZNode));
    }

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    elector.joinElection(localActiveNodeInfo);
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    /**
     * When error occurs in serviceInit(), serviceStop() can be called.
     * We need null check for the case.
     */
    if (elector != null) {
      elector.quitElection(false);
      elector.terminateConnection();
    }
    super.serviceStop();
  }

  @Override
  public void becomeActive() throws ServiceFailedException {
    cancelDisconnectTimer();

    try {
      rm.getRMContext().getRMAdminService().transitionToActive(req);
    } catch (Exception e) {
      throw new ServiceFailedException("RM could not transition to Active", e);
    }
  }

  @Override
  public void becomeStandby() {
    cancelDisconnectTimer();

    try {
      rm.getRMContext().getRMAdminService().transitionToStandby(req);
    } catch (Exception e) {
      LOG.error("RM could not transition to Standby", e);
    }
  }

  /**
   * Stop the disconnect timer.  Any running tasks will be allowed to complete.
   */
  private void cancelDisconnectTimer() {
    synchronized (zkDisconnectLock) {
      if (zkDisconnectTimer != null) {
        zkDisconnectTimer.cancel();
        zkDisconnectTimer = null;
      }
    }
  }

  /**
   * When the ZK client loses contact with ZK, this method will be called to
   * allow the RM to react. Because the loss of connection can be noticed
   * before the session timeout happens, it is undesirable to transition
   * immediately. Instead the method starts a timer that will wait
   * {@link YarnConfiguration#RM_ZK_TIMEOUT_MS} milliseconds before
   * initiating the transition into standby state.
   */
  @Override
  public void enterNeutralMode() {
    LOG.warn("Lost contact with Zookeeper. Transitioning to standby in "
        + zkSessionTimeout + " ms if connection is not reestablished.");

    // If we've just become disconnected, start a timer.  When the time's up,
    // we'll transition to standby.
    synchronized (zkDisconnectLock) {
      if (zkDisconnectTimer == null) {
        zkDisconnectTimer = new Timer("Zookeeper disconnect timer");
        zkDisconnectTimer.schedule(new TimerTask() {
          @Override
          public void run() {
            synchronized (zkDisconnectLock) {
              // Only run if the timer hasn't been cancelled
              if (zkDisconnectTimer != null) {
                becomeStandby();
              }
            }
          }
        }, zkSessionTimeout);
      }
    }
  }

  @SuppressWarnings(value = "unchecked")
  @Override
  public void notifyFatalError(String errorMessage) {
    rm.getRMContext().getDispatcher().getEventHandler().handle(
        new RMFatalEvent(RMFatalEventType.EMBEDDED_ELECTOR_FAILED,
            errorMessage));
  }

  @Override
  public void fenceOldActive(byte[] oldActiveData) {
    LOG.debug("Request to fence old active being ignored, " +
        "as embedded leader election doesn't support fencing");
  }

  private static byte[] createActiveNodeInfo(String clusterId, String rmId)
      throws IOException {
    return YarnServerResourceManagerServiceProtos.ActiveRMInfoProto
        .newBuilder()
        .setClusterId(clusterId)
        .setRmId(rmId)
        .build()
        .toByteArray();
  }

  private boolean isParentZnodeSafe(String clusterId)
      throws InterruptedException, IOException, KeeperException {
    byte[] data;
    try {
      data = elector.getActiveData();
    } catch (ActiveStandbyElector.ActiveNotFoundException e) {
      // no active found, parent znode is safe
      return true;
    }

    YarnServerResourceManagerServiceProtos.ActiveRMInfoProto proto;
    try {
      proto = YarnServerResourceManagerServiceProtos.ActiveRMInfoProto
          .parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      LOG.error("Invalid data in ZK: " + StringUtils.byteToHexString(data));
      return false;
    }

    // Check if the passed proto corresponds to an RM in the same cluster
    if (!proto.getClusterId().equals(clusterId)) {
      LOG.error("Mismatched cluster! The other RM seems " +
          "to be from a different cluster. Current cluster = " + clusterId +
          "Other RM's cluster = " + proto.getClusterId());
      return false;
    }
    return true;
  }

  // EmbeddedElector methods

  @Override
  public void rejoinElection() {
    elector.quitElection(false);
    elector.joinElection(localActiveNodeInfo);
  }

  @Override
  public String getZookeeperConnectionState() {
    return elector.getHAZookeeperConnectionState();
  }
}
