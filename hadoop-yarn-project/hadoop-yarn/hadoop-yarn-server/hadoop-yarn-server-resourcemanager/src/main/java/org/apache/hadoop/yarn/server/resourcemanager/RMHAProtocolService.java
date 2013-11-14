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

import com.google.protobuf.BlockingService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolServerSideTranslatorPB;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.WritableRpcEngine;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Internal class to handle HA related aspects of the {@link ResourceManager}.
 *
 * TODO (YARN-1318): Some/ all of this functionality should be merged with
 * {@link AdminService}. Currently, marking this as Private and Unstable for
 * those reasons.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RMHAProtocolService extends AbstractService implements
    HAServiceProtocol {
  private static final Log LOG = LogFactory.getLog(RMHAProtocolService.class);

  private Configuration conf;
  private ResourceManager rm;
  @VisibleForTesting
  protected HAServiceState haState = HAServiceState.INITIALIZING;
  private AccessControlList adminAcl;
  private Server haAdminServer;

  @InterfaceAudience.Private
  boolean haEnabled;

  public RMHAProtocolService(ResourceManager resourceManager)  {
    super("RMHAProtocolService");
    this.rm = resourceManager;
  }

  @Override
  protected synchronized void serviceInit(Configuration conf) throws
      Exception {
    this.conf = conf;
    haEnabled = HAUtil.isHAEnabled(this.conf);
    if (haEnabled) {
      HAUtil.verifyAndSetConfiguration(conf);
      rm.setConf(this.conf);
      adminAcl = new AccessControlList(conf.get(
          YarnConfiguration.YARN_ADMIN_ACL,
          YarnConfiguration.DEFAULT_YARN_ADMIN_ACL));
    }
    rm.createAndInitActiveServices();
    super.serviceInit(this.conf);
  }

  @Override
  protected synchronized void serviceStart() throws Exception {
    if (haEnabled) {
      transitionToStandby(true);
      startHAAdminServer();
    } else {
      transitionToActive();
    }

    super.serviceStart();
  }

  @Override
  protected synchronized void serviceStop() throws Exception {
    if (haEnabled) {
      stopHAAdminServer();
    }
    transitionToStandby(false);
    haState = HAServiceState.STOPPING;
    super.serviceStop();
  }


  protected void startHAAdminServer() throws Exception {
    InetSocketAddress haAdminServiceAddress = conf.getSocketAddr(
        YarnConfiguration.RM_HA_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_HA_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_HA_ADMIN_PORT);

    RPC.setProtocolEngine(conf, HAServiceProtocolPB.class,
        ProtobufRpcEngine.class);

    HAServiceProtocolServerSideTranslatorPB haServiceProtocolXlator =
        new HAServiceProtocolServerSideTranslatorPB(this);
    BlockingService haPbService =
        HAServiceProtocolProtos.HAServiceProtocolService
            .newReflectiveBlockingService(haServiceProtocolXlator);

    WritableRpcEngine.ensureInitialized();

    String bindHost = haAdminServiceAddress.getHostName();

    int serviceHandlerCount = conf.getInt(
        YarnConfiguration.RM_HA_ADMIN_CLIENT_THREAD_COUNT,
        YarnConfiguration.DEFAULT_RM_HA_ADMIN_CLIENT_THREAD_COUNT);

    haAdminServer = new RPC.Builder(conf)
        .setProtocol(HAServiceProtocolPB.class)
        .setInstance(haPbService)
        .setBindAddress(bindHost)
        .setPort(haAdminServiceAddress.getPort())
        .setNumHandlers(serviceHandlerCount)
        .setVerbose(false)
        .build();

    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false)) {
      haAdminServer.refreshServiceAcl(conf, new RMPolicyProvider());
    }

    haAdminServer.start();
    conf.updateConnectAddr(YarnConfiguration.RM_HA_ADMIN_ADDRESS,
        haAdminServer.getListenerAddress());
  }

  private void stopHAAdminServer() throws Exception {
    if (haAdminServer != null) {
      haAdminServer.stop();
      haAdminServer.join();
      haAdminServer = null;
    }
  }

  @Override
  public synchronized void monitorHealth()
      throws IOException {
    checkAccess("monitorHealth");
    if (haState == HAServiceState.ACTIVE && !rm.areActiveServicesRunning()) {
      throw new HealthCheckFailedException(
          "Active ResourceManager services are not running!");
    }
  }

  @InterfaceAudience.Private
  synchronized void transitionToActive() throws Exception {
    if (haState == HAServiceState.ACTIVE) {
      LOG.info("Already in active state");
      return;
    }

    LOG.info("Transitioning to active");
    rm.startActiveServices();
    haState = HAServiceState.ACTIVE;
    LOG.info("Transitioned to active");
  }

  @Override
  public synchronized void transitionToActive(StateChangeRequestInfo reqInfo)
      throws IOException {
    UserGroupInformation user = checkAccess("transitionToActive");
    // TODO (YARN-1177): When automatic failover is enabled,
    // check if transition should be allowed for this request
    try {
      transitionToActive();
      RMAuditLogger.logSuccess(user.getShortUserName(),
          "transitionToActive", "RMHAProtocolService");
    } catch (Exception e) {
      RMAuditLogger.logFailure(user.getShortUserName(), "transitionToActive",
          adminAcl.toString(), "RMHAProtocolService",
          "Exception transitioning to active");
      throw new ServiceFailedException(
          "Error when transitioning to Active mode", e);
    }
  }

  @InterfaceAudience.Private
  synchronized void transitionToStandby(boolean initialize)
      throws Exception {
    if (haState == HAServiceState.STANDBY) {
      LOG.info("Already in standby state");
      return;
    }

    LOG.info("Transitioning to standby");
    if (haState == HAServiceState.ACTIVE) {
      rm.stopActiveServices();
      if (initialize) {
        rm.createAndInitActiveServices();
      }
    }
    haState = HAServiceState.STANDBY;
    LOG.info("Transitioned to standby");
  }

  @Override
  public synchronized void transitionToStandby(StateChangeRequestInfo reqInfo)
      throws IOException {
    UserGroupInformation user = checkAccess("transitionToStandby");
    // TODO (YARN-1177): When automatic failover is enabled,
    // check if transition should be allowed for this request
    try {
      transitionToStandby(true);
      RMAuditLogger.logSuccess(user.getShortUserName(),
          "transitionToStandby", "RMHAProtocolService");
    } catch (Exception e) {
      RMAuditLogger.logFailure(user.getShortUserName(), "transitionToStandby",
          adminAcl.toString(), "RMHAProtocolService",
          "Exception transitioning to standby");
      throw new ServiceFailedException(
          "Error when transitioning to Standby mode", e);
    }
  }

  @Override
  public synchronized HAServiceStatus getServiceStatus() throws IOException {
    checkAccess("getServiceState");
    HAServiceStatus ret = new HAServiceStatus(haState);
    if (haState == HAServiceState.ACTIVE || haState == HAServiceState.STANDBY) {
      ret.setReadyToBecomeActive();
    } else {
      ret.setNotReadyToBecomeActive("State is " + haState);
    }
    return ret;
  }

  private UserGroupInformation checkAccess(String method) throws IOException {
    return RMServerUtils.verifyAccess(adminAcl, method, LOG);
  }
}
