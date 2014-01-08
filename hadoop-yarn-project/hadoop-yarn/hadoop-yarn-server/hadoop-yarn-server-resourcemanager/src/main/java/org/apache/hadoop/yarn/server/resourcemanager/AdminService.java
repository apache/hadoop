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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;

import com.google.protobuf.BlockingService;

public class AdminService extends CompositeService implements
    HAServiceProtocol, ResourceManagerAdministrationProtocol {

  private static final Log LOG = LogFactory.getLog(AdminService.class);

  private final RMContext rmContext;
  private final ResourceManager rm;
  private String rmId;

  private boolean autoFailoverEnabled;

  private Server server;
  private InetSocketAddress masterServiceAddress;
  private AccessControlList adminAcl;
  
  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  public AdminService(ResourceManager rm, RMContext rmContext) {
    super(AdminService.class.getName());
    this.rm = rm;
    this.rmContext = rmContext;
  }

  @Override
  public synchronized void serviceInit(Configuration conf) throws Exception {
    if (rmContext.isHAEnabled()) {
      autoFailoverEnabled = HAUtil.isAutomaticFailoverEnabled(conf);
      if (autoFailoverEnabled) {
        if (HAUtil.isAutomaticFailoverEmbedded(conf)) {
          addIfService(createEmbeddedElectorService());
        }
      }
    }

    masterServiceAddress = conf.getSocketAddr(
        YarnConfiguration.RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_PORT);
    adminAcl = new AccessControlList(conf.get(
        YarnConfiguration.YARN_ADMIN_ACL,
        YarnConfiguration.DEFAULT_YARN_ADMIN_ACL));
    rmId = conf.get(YarnConfiguration.RM_HA_ID);
    super.serviceInit(conf);
  }

  @Override
  protected synchronized void serviceStart() throws Exception {
    startServer();
    super.serviceStart();
  }

  @Override
  protected synchronized void serviceStop() throws Exception {
    stopServer();
    super.serviceStop();
  }

  protected void startServer() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    this.server = (Server) rpc.getServer(
        ResourceManagerAdministrationProtocol.class, this, masterServiceAddress,
        conf, null,
        conf.getInt(YarnConfiguration.RM_ADMIN_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_ADMIN_CLIENT_THREAD_COUNT));

    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      refreshServiceAcls(conf, new RMPolicyProvider());
    }

    if (rmContext.isHAEnabled()) {
      RPC.setProtocolEngine(conf, HAServiceProtocolPB.class,
          ProtobufRpcEngine.class);

      HAServiceProtocolServerSideTranslatorPB haServiceProtocolXlator =
          new HAServiceProtocolServerSideTranslatorPB(this);
      BlockingService haPbService =
          HAServiceProtocolProtos.HAServiceProtocolService
              .newReflectiveBlockingService(haServiceProtocolXlator);
      server.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
          HAServiceProtocol.class, haPbService);
    }

    this.server.start();
    conf.updateConnectAddr(YarnConfiguration.RM_ADMIN_ADDRESS,
        server.getListenerAddress());
  }

  protected void stopServer() throws Exception {
    if (this.server != null) {
      this.server.stop();
    }
  }

  protected EmbeddedElectorService createEmbeddedElectorService() {
    return new EmbeddedElectorService(rmContext);
  }

  private UserGroupInformation checkAccess(String method) throws IOException {
    return RMServerUtils.verifyAccess(adminAcl, method, LOG);
  }

  private UserGroupInformation checkAcls(String method) throws YarnException {
    try {
      return checkAccess(method);
    } catch (IOException ioe) {
      throw RPCUtil.getRemoteException(ioe);
    }
  }

  /**
   * Check that a request to change this node's HA state is valid.
   * In particular, verifies that, if auto failover is enabled, non-forced
   * requests from the HAAdmin CLI are rejected, and vice versa.
   *
   * @param req the request to check
   * @throws AccessControlException if the request is disallowed
   */
  private void checkHaStateChange(StateChangeRequestInfo req)
      throws AccessControlException {
    switch (req.getSource()) {
      case REQUEST_BY_USER:
        if (autoFailoverEnabled) {
          throw new AccessControlException(
              "Manual failover for this ResourceManager is disallowed, " +
                  "because automatic failover is enabled.");
        }
        break;
      case REQUEST_BY_USER_FORCED:
        if (autoFailoverEnabled) {
          LOG.warn("Allowing manual failover from " +
              org.apache.hadoop.ipc.Server.getRemoteAddress() +
              " even though automatic failover is enabled, because the user " +
              "specified the force flag");
        }
        break;
      case REQUEST_BY_ZKFC:
        if (!autoFailoverEnabled) {
          throw new AccessControlException(
              "Request from ZK failover controller at " +
                  org.apache.hadoop.ipc.Server.getRemoteAddress() + " denied " +
                  "since automatic failover is not enabled");
        }
        break;
    }
  }

  private synchronized boolean isRMActive() {
    return HAServiceState.ACTIVE == rmContext.getHAServiceState();
  }

  private void throwStandbyException() throws StandbyException {
    throw new StandbyException("ResourceManager " + rmId + " is not Active!");
  }

  @Override
  public synchronized void monitorHealth()
      throws IOException {
    checkAccess("monitorHealth");
    if (isRMActive() && !rm.areActiveServicesRunning()) {
      throw new HealthCheckFailedException(
          "Active ResourceManager services are not running!");
    }
  }

  @Override
  public synchronized void transitionToActive(
      HAServiceProtocol.StateChangeRequestInfo reqInfo) throws IOException {
    UserGroupInformation user = checkAccess("transitionToActive");
    checkHaStateChange(reqInfo);
    try {
      rm.transitionToActive();
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

  @Override
  public synchronized void transitionToStandby(
      HAServiceProtocol.StateChangeRequestInfo reqInfo) throws IOException {
    UserGroupInformation user = checkAccess("transitionToStandby");
    checkHaStateChange(reqInfo);
    try {
      rm.transitionToStandby(true);
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
    HAServiceState haState = rmContext.getHAServiceState();
    HAServiceStatus ret = new HAServiceStatus(haState);
    if (isRMActive() || haState == HAServiceProtocol.HAServiceState.STANDBY) {
      ret.setReadyToBecomeActive();
    } else {
      ret.setNotReadyToBecomeActive("State is " + haState);
    }
    return ret;
  } 

  @Override
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request)
      throws YarnException, StandbyException {
    UserGroupInformation user = checkAcls("refreshQueues");

    if (!isRMActive()) {
      RMAuditLogger.logFailure(user.getShortUserName(), "refreshQueues",
          adminAcl.toString(), "AdminService",
          "ResourceManager is not active. Can not refresh queues.");
      throwStandbyException();
    }

    try {
      rmContext.getScheduler().reinitialize(getConfig(), this.rmContext);
      RMAuditLogger.logSuccess(user.getShortUserName(), "refreshQueues", 
          "AdminService");
      return recordFactory.newRecordInstance(RefreshQueuesResponse.class);
    } catch (IOException ioe) {
      LOG.info("Exception refreshing queues ", ioe);
      RMAuditLogger.logFailure(user.getShortUserName(), "refreshQueues",
          adminAcl.toString(), "AdminService",
          "Exception refreshing queues");
      throw RPCUtil.getRemoteException(ioe);
    }
  }

  @Override
  public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
      throws YarnException, StandbyException {
    UserGroupInformation user = checkAcls("refreshNodes");

    if (!isRMActive()) {
      RMAuditLogger.logFailure(user.getShortUserName(), "refreshNodes",
          adminAcl.toString(), "AdminService",
          "ResourceManager is not active. Can not refresh nodes.");
      throwStandbyException();
    }

    try {
      rmContext.getNodesListManager().refreshNodes(new YarnConfiguration());
      RMAuditLogger.logSuccess(user.getShortUserName(), "refreshNodes",
          "AdminService");
      return recordFactory.newRecordInstance(RefreshNodesResponse.class);
    } catch (IOException ioe) {
      LOG.info("Exception refreshing nodes ", ioe);
      RMAuditLogger.logFailure(user.getShortUserName(), "refreshNodes",
          adminAcl.toString(), "AdminService", "Exception refreshing nodes");
      throw RPCUtil.getRemoteException(ioe);
    }
  }

  @Override
  public RefreshSuperUserGroupsConfigurationResponse refreshSuperUserGroupsConfiguration(
      RefreshSuperUserGroupsConfigurationRequest request)
      throws YarnException, StandbyException {
    UserGroupInformation user = checkAcls("refreshSuperUserGroupsConfiguration");

    // TODO (YARN-1459): Revisit handling super-user-groups on Standby RM
    if (!isRMActive()) {
      RMAuditLogger.logFailure(user.getShortUserName(),
          "refreshSuperUserGroupsConfiguration",
          adminAcl.toString(), "AdminService",
          "ResourceManager is not active. Can not refresh super-user-groups.");
      throwStandbyException();
    }

    ProxyUsers.refreshSuperUserGroupsConfiguration(new Configuration());
    RMAuditLogger.logSuccess(user.getShortUserName(),
        "refreshSuperUserGroupsConfiguration", "AdminService");
    
    return recordFactory.newRecordInstance(
        RefreshSuperUserGroupsConfigurationResponse.class);
  }

  @Override
  public RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(
      RefreshUserToGroupsMappingsRequest request)
      throws YarnException, StandbyException {
    UserGroupInformation user = checkAcls("refreshUserToGroupsMappings");

    // TODO (YARN-1459): Revisit handling user-groups on Standby RM
    if (!isRMActive()) {
      RMAuditLogger.logFailure(user.getShortUserName(),
          "refreshUserToGroupsMapping",
          adminAcl.toString(), "AdminService",
          "ResourceManager is not active. Can not refresh user-groups.");
      throwStandbyException();
    }

    Groups.getUserToGroupsMappingService().refresh();
    RMAuditLogger.logSuccess(user.getShortUserName(), 
        "refreshUserToGroupsMappings", "AdminService");

    return recordFactory.newRecordInstance(
        RefreshUserToGroupsMappingsResponse.class);
  }

  @Override
  public RefreshAdminAclsResponse refreshAdminAcls(
      RefreshAdminAclsRequest request) throws YarnException {
    UserGroupInformation user = checkAcls("refreshAdminAcls");
    
    Configuration conf = new Configuration();
    adminAcl = new AccessControlList(conf.get(
        YarnConfiguration.YARN_ADMIN_ACL,
        YarnConfiguration.DEFAULT_YARN_ADMIN_ACL));
    RMAuditLogger.logSuccess(user.getShortUserName(), "refreshAdminAcls", 
        "AdminService");

    return recordFactory.newRecordInstance(RefreshAdminAclsResponse.class);
  }

  @Override
  public RefreshServiceAclsResponse refreshServiceAcls(
      RefreshServiceAclsRequest request) throws YarnException {
    Configuration conf = new Configuration();
    if (!conf.getBoolean(
             CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
             false)) {
      throw RPCUtil.getRemoteException(
          new IOException("Service Authorization (" + 
              CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION + 
              ") not enabled."));
    }
    
    PolicyProvider policyProvider = new RMPolicyProvider(); 
    
    refreshServiceAcls(conf, policyProvider);
    if (isRMActive()) {
      rmContext.getClientRMService().refreshServiceAcls(conf, policyProvider);
      rmContext.getApplicationMasterService().refreshServiceAcls(
          conf, policyProvider);
      rmContext.getResourceTrackerService().refreshServiceAcls(
          conf, policyProvider);
    } else {
      LOG.warn("ResourceManager is not active. Not refreshing ACLs for " +
          "Clients, ApplicationMasters and NodeManagers");
    }
    
    return recordFactory.newRecordInstance(RefreshServiceAclsResponse.class);
  }

  void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  @Override
  public String[] getGroupsForUser(String user) throws IOException {
    return UserGroupInformation.createRemoteUser(user).getGroupNames();
  }

  @Override
  public UpdateNodeResourceResponse updateNodeResource(
      UpdateNodeResourceRequest request) throws YarnException, IOException {
    Map<NodeId, ResourceOption> nodeResourceMap = request.getNodeResourceMap();
    Set<NodeId> nodeIds = nodeResourceMap.keySet();
    // verify nodes are all valid first. 
    // if any invalid nodes, throw exception instead of partially updating
    // valid nodes.
    for (NodeId nodeId : nodeIds) {
      RMNode node = this.rmContext.getRMNodes().get(nodeId);
      if (node == null) {
        LOG.error("Resource update get failed on all nodes due to change "
            + "resource on an unrecognized node: " + nodeId);
        throw RPCUtil.getRemoteException(
            "Resource update get failed on all nodes due to change resource "
                + "on an unrecognized node: " + nodeId);
      }
    }
    
    // do resource update on each node.
    // Notice: it is still possible to have invalid NodeIDs as nodes decommission
    // may happen just at the same time. This time, only log and skip absent
    // nodes without throwing any exceptions.
    for (Map.Entry<NodeId, ResourceOption> entry : nodeResourceMap.entrySet()) {
      ResourceOption newResourceOption = entry.getValue();
      NodeId nodeId = entry.getKey();
      RMNode node = this.rmContext.getRMNodes().get(nodeId);
      if (node == null) {
        LOG.warn("Resource update get failed on an unrecognized node: " + nodeId);
      } else {
        node.setResourceOption(newResourceOption);
        LOG.info("Update resource successfully on node(" + node.getNodeID()
            +") with resource(" + newResourceOption.toString() + ")");
      }
    }
    UpdateNodeResourceResponse response = recordFactory.newRecordInstance(
          UpdateNodeResourceResponse.class);
      return response;
  }
  
}
