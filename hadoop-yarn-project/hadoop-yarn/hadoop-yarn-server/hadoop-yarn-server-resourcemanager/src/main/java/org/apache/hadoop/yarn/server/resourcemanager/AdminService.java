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
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
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
import org.apache.hadoop.yarn.api.records.DecommissionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ConfiguredYarnAuthorizer;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NodeLabelsUtils;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.resource.DynamicResourceConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeResourceUpdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;

public class AdminService extends CompositeService implements
    HAServiceProtocol, ResourceManagerAdministrationProtocol {

  private static final Log LOG = LogFactory.getLog(AdminService.class);

  private final RMContext rmContext;
  private final ResourceManager rm;
  private String rmId;

  private boolean autoFailoverEnabled;
  private boolean curatorEnabled;
  private EmbeddedElectorService embeddedElector;

  private Server server;

  // Address to use for binding. May be a wildcard address.
  private InetSocketAddress masterServiceBindAddress;

  private YarnAuthorizationProvider authorizer;

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  private UserGroupInformation daemonUser;

  @VisibleForTesting
  boolean isCentralizedNodeLabelConfiguration = true;

  public AdminService(ResourceManager rm, RMContext rmContext) {
    super(AdminService.class.getName());
    this.rm = rm;
    this.rmContext = rmContext;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    if (rmContext.isHAEnabled()) {
      curatorEnabled = conf.getBoolean(YarnConfiguration.CURATOR_LEADER_ELECTOR,
          YarnConfiguration.DEFAULT_CURATOR_LEADER_ELECTOR_ENABLED);
      autoFailoverEnabled = HAUtil.isAutomaticFailoverEnabled(conf);
      if (autoFailoverEnabled && !curatorEnabled) {
        if (HAUtil.isAutomaticFailoverEmbedded(conf)) {
          embeddedElector = createEmbeddedElectorService();
          addIfService(embeddedElector);
        }
      }

    }

    masterServiceBindAddress = conf.getSocketAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_PORT);
    daemonUser = UserGroupInformation.getCurrentUser();
    authorizer = YarnAuthorizationProvider.getInstance(conf);
    authorizer.setAdmins(getAdminAclList(conf), UserGroupInformation
        .getCurrentUser());
    rmId = conf.get(YarnConfiguration.RM_HA_ID);

    isCentralizedNodeLabelConfiguration =
        YarnConfiguration.isCentralizedNodeLabelConfiguration(conf);

    super.serviceInit(conf);
  }

  private AccessControlList getAdminAclList(Configuration conf) {
    AccessControlList aclList = new AccessControlList(conf.get(
        YarnConfiguration.YARN_ADMIN_ACL,
        YarnConfiguration.DEFAULT_YARN_ADMIN_ACL));
    aclList.addUser(daemonUser.getShortUserName());
    return aclList;
  }

  @Override
  protected void serviceStart() throws Exception {
    startServer();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopServer();
    super.serviceStop();
  }

  protected void startServer() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    this.server = (Server) rpc.getServer(
        ResourceManagerAdministrationProtocol.class, this, masterServiceBindAddress,
        conf, null,
        conf.getInt(YarnConfiguration.RM_ADMIN_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_ADMIN_CLIENT_THREAD_COUNT));

    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      refreshServiceAcls(
          getConfiguration(conf,
              YarnConfiguration.HADOOP_POLICY_CONFIGURATION_FILE),
          RMPolicyProvider.getInstance());
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
    conf.updateConnectAddr(YarnConfiguration.RM_BIND_HOST,
                           YarnConfiguration.RM_ADMIN_ADDRESS,
                           YarnConfiguration.DEFAULT_RM_ADMIN_ADDRESS,
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

  @InterfaceAudience.Private
  void resetLeaderElection() {
    if (embeddedElector != null) {
      embeddedElector.resetLeaderElection();
    }
  }

  private UserGroupInformation checkAccess(String method) throws IOException {
    return RMServerUtils.verifyAdminAccess(authorizer, method, LOG);
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

  @SuppressWarnings("unchecked")
  @Override
  public synchronized void transitionToActive(
      HAServiceProtocol.StateChangeRequestInfo reqInfo) throws IOException {
    // call refreshAdminAcls before HA state transition
    // for the case that adminAcls have been updated in previous active RM
    try {
      refreshAdminAcls(false);
    } catch (YarnException ex) {
      throw new ServiceFailedException("Can not execute refreshAdminAcls", ex);
    }

    UserGroupInformation user = checkAccess("transitionToActive");
    checkHaStateChange(reqInfo);
    try {
      rm.transitionToActive();
    } catch (Exception e) {
      RMAuditLogger.logFailure(user.getShortUserName(), "transitionToActive",
          "", "RM",
          "Exception transitioning to active");
      throw new ServiceFailedException(
          "Error when transitioning to Active mode", e);
    }
    try {
      // call all refresh*s for active RM to get the updated configurations.
      refreshAll();
    } catch (Exception e) {
      LOG.error("RefreshAll failed so firing fatal event", e);
      rmContext
          .getDispatcher()
          .getEventHandler()
          .handle(
          new RMFatalEvent(RMFatalEventType.TRANSITION_TO_ACTIVE_FAILED, e));
      throw new ServiceFailedException(
          "Error on refreshAll during transistion to Active", e);
    }
    RMAuditLogger.logSuccess(user.getShortUserName(), "transitionToActive",
        "RM");
  }

  @Override
  public synchronized void transitionToStandby(
      HAServiceProtocol.StateChangeRequestInfo reqInfo) throws IOException {
    // call refreshAdminAcls before HA state transition
    // for the case that adminAcls have been updated in previous active RM
    try {
      refreshAdminAcls(false);
    } catch (YarnException ex) {
      throw new ServiceFailedException("Can not execute refreshAdminAcls", ex);
    }
    UserGroupInformation user = checkAccess("transitionToStandby");
    checkHaStateChange(reqInfo);
    try {
      rm.transitionToStandby(true);
      RMAuditLogger.logSuccess(user.getShortUserName(),
          "transitionToStandby", "RM");
    } catch (Exception e) {
      RMAuditLogger.logFailure(user.getShortUserName(), "transitionToStandby",
          "", "RM",
          "Exception transitioning to standby");
      throw new ServiceFailedException(
          "Error when transitioning to Standby mode", e);
    }
  }

  @Override
  public synchronized HAServiceStatus getServiceStatus() throws IOException {
    checkAccess("getServiceState");
    if (curatorEnabled) {
      HAServiceStatus state;
      if (rmContext.getLeaderElectorService().hasLeaderShip()) {
        state = new HAServiceStatus(HAServiceState.ACTIVE);
      } else {
        state = new HAServiceStatus(HAServiceState.STANDBY);
      }
      // set empty string to avoid NPE at
      // HAServiceProtocolServerSideTranslatorPB#getServiceStatus
      state.setNotReadyToBecomeActive("");
      return state;
    } else {
      HAServiceState haState = rmContext.getHAServiceState();
      HAServiceStatus ret = new HAServiceStatus(haState);
      if (isRMActive() || haState == HAServiceProtocol.HAServiceState.STANDBY) {
        ret.setReadyToBecomeActive();
      } else {
        ret.setNotReadyToBecomeActive("State is " + haState);
      }
      return ret;
    }
  }

  @Override
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request)
      throws YarnException, StandbyException {
    String argName = "refreshQueues";
    final String msg = "refresh queues.";
    UserGroupInformation user = checkAcls(argName);

    checkRMStatus(user.getShortUserName(), argName, msg);

    RefreshQueuesResponse response =
        recordFactory.newRecordInstance(RefreshQueuesResponse.class);
    try {
      rmContext.getScheduler().reinitialize(getConfig(), this.rmContext);
      // refresh the reservation system
      ReservationSystem rSystem = rmContext.getReservationSystem();
      if (rSystem != null) {
        rSystem.reinitialize(getConfig(), rmContext);
      }
      RMAuditLogger.logSuccess(user.getShortUserName(), argName,
          "AdminService");
      return response;
    } catch (IOException ioe) {
      throw logAndWrapException(ioe, user.getShortUserName(), argName, msg);
    }
  }

  @Override
  public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
      throws YarnException, StandbyException {
    String argName = "refreshNodes";
    final String msg = "refresh nodes.";
    UserGroupInformation user = checkAcls("refreshNodes");

    checkRMStatus(user.getShortUserName(), argName, msg);

    try {
      Configuration conf =
          getConfiguration(new Configuration(false),
              YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
      switch (request.getDecommissionType()) {
      case NORMAL:
        rmContext.getNodesListManager().refreshNodes(conf);
        break;
      case GRACEFUL:
        rmContext.getNodesListManager().refreshNodesGracefully(conf);
        break;
      case FORCEFUL:
        rmContext.getNodesListManager().refreshNodesForcefully();
        break;
      }
      RMAuditLogger.logSuccess(user.getShortUserName(), argName,
          "AdminService");
      return recordFactory.newRecordInstance(RefreshNodesResponse.class);
    } catch (IOException ioe) {
      throw logAndWrapException(ioe, user.getShortUserName(), argName, msg);
    }
  }

  @Override
  public RefreshSuperUserGroupsConfigurationResponse refreshSuperUserGroupsConfiguration(
      RefreshSuperUserGroupsConfigurationRequest request)
      throws YarnException, IOException {
    String argName = "refreshSuperUserGroupsConfiguration";
    UserGroupInformation user = checkAcls(argName);

    checkRMStatus(user.getShortUserName(), argName, "refresh super-user-groups.");

    // Accept hadoop common configs in core-site.xml as well as RM specific
    // configurations in yarn-site.xml
    Configuration conf =
        getConfiguration(new Configuration(false),
            YarnConfiguration.CORE_SITE_CONFIGURATION_FILE,
            YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
    RMServerUtils.processRMProxyUsersConf(conf);
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
    RMAuditLogger.logSuccess(user.getShortUserName(),
        argName, "AdminService");
    
    return recordFactory.newRecordInstance(
        RefreshSuperUserGroupsConfigurationResponse.class);
  }

  @Override
  public RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(
      RefreshUserToGroupsMappingsRequest request)
      throws YarnException, IOException {
    String argName = "refreshUserToGroupsMappings";
    UserGroupInformation user = checkAcls(argName);

    checkRMStatus(user.getShortUserName(), argName, "refresh user-groups.");

    Groups.getUserToGroupsMappingService(
        getConfiguration(new Configuration(false),
            YarnConfiguration.CORE_SITE_CONFIGURATION_FILE)).refresh();

    RMAuditLogger.logSuccess(user.getShortUserName(), argName, "AdminService");

    return recordFactory.newRecordInstance(
        RefreshUserToGroupsMappingsResponse.class);
  }

  @Override
  public RefreshAdminAclsResponse refreshAdminAcls(
      RefreshAdminAclsRequest request) throws YarnException, IOException {
    return refreshAdminAcls(true);
  }

  private RefreshAdminAclsResponse refreshAdminAcls(boolean checkRMHAState)
      throws YarnException, IOException {
    String argName = "refreshAdminAcls";
    UserGroupInformation user = checkAcls(argName);

    if (checkRMHAState) {
      checkRMStatus(user.getShortUserName(), argName, "refresh Admin ACLs.");
    }
    Configuration conf =
        getConfiguration(new Configuration(false),
            YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
    authorizer.setAdmins(getAdminAclList(conf), UserGroupInformation
        .getCurrentUser());
    RMAuditLogger.logSuccess(user.getShortUserName(), argName,
        "AdminService");

    return recordFactory.newRecordInstance(RefreshAdminAclsResponse.class);
  }

  @Override
  public RefreshServiceAclsResponse refreshServiceAcls(
      RefreshServiceAclsRequest request) throws YarnException, IOException {
    if (!getConfig().getBoolean(
             CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
             false)) {
      throw RPCUtil.getRemoteException(
          new IOException("Service Authorization (" + 
              CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION + 
              ") not enabled."));
    }

    String argName = "refreshServiceAcls";
    UserGroupInformation user = checkAcls(argName);

    checkRMStatus(user.getShortUserName(), argName, "refresh Service ACLs.");

    PolicyProvider policyProvider = RMPolicyProvider.getInstance();
    Configuration conf =
        getConfiguration(new Configuration(false),
            YarnConfiguration.HADOOP_POLICY_CONFIGURATION_FILE);

    refreshServiceAcls(conf, policyProvider);
    rmContext.getClientRMService().refreshServiceAcls(conf, policyProvider);
    rmContext.getApplicationMasterService().refreshServiceAcls(
        conf, policyProvider);
    rmContext.getResourceTrackerService().refreshServiceAcls(
        conf, policyProvider);

    RMAuditLogger.logSuccess(user.getShortUserName(), argName, "AdminService");

    return recordFactory.newRecordInstance(RefreshServiceAclsResponse.class);
  }

  private synchronized void refreshServiceAcls(Configuration configuration,
      PolicyProvider policyProvider) {
    this.server.refreshServiceAclWithLoadedConfiguration(configuration,
        policyProvider);
  }

  @Override
  public String[] getGroupsForUser(String user) throws IOException {
    return UserGroupInformation.createRemoteUser(user).getGroupNames();
  }

  @SuppressWarnings("unchecked")
  @Override
  public UpdateNodeResourceResponse updateNodeResource(
      UpdateNodeResourceRequest request) throws YarnException, IOException {
    String argName = "updateNodeResource";
    UserGroupInformation user = checkAcls(argName);
    
    checkRMStatus(user.getShortUserName(), argName, "update node resource.");
    
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
    boolean allSuccess = true;
    for (Map.Entry<NodeId, ResourceOption> entry : nodeResourceMap.entrySet()) {
      ResourceOption newResourceOption = entry.getValue();
      NodeId nodeId = entry.getKey();
      RMNode node = this.rmContext.getRMNodes().get(nodeId);
      
      if (node == null) {
        LOG.warn("Resource update get failed on an unrecognized node: " + nodeId);
        allSuccess = false;
      } else {
        // update resource to RMNode
        this.rmContext.getDispatcher().getEventHandler()
          .handle(new RMNodeResourceUpdateEvent(nodeId, newResourceOption));
        LOG.info("Update resource on node(" + node.getNodeID()
            + ") with resource(" + newResourceOption.toString() + ")");

      }
    }
    if (allSuccess) {
      RMAuditLogger.logSuccess(user.getShortUserName(), argName,
          "AdminService");
    }
    UpdateNodeResourceResponse response = 
        UpdateNodeResourceResponse.newInstance();
    return response;
  }

  @Override
  public RefreshNodesResourcesResponse refreshNodesResources(
      RefreshNodesResourcesRequest request)
      throws YarnException, StandbyException {
    String argName = "refreshNodesResources";
    UserGroupInformation user = checkAcls(argName);
    final String msg = "refresh nodes.";

    checkRMStatus(user.getShortUserName(), argName, msg);

    RefreshNodesResourcesResponse response =
        recordFactory.newRecordInstance(RefreshNodesResourcesResponse.class);

    try {
      Configuration conf = getConfig();
      Configuration configuration = new Configuration(conf);
      DynamicResourceConfiguration newConf;

      InputStream drInputStream =
          this.rmContext.getConfigurationProvider().getConfigurationInputStream(
              configuration, YarnConfiguration.DR_CONFIGURATION_FILE);

      if (drInputStream != null) {
        newConf = new DynamicResourceConfiguration(configuration,
            drInputStream);
      } else {
        newConf = new DynamicResourceConfiguration(configuration);
      }

      if (newConf.getNodes() != null && newConf.getNodes().length != 0) {
        Map<NodeId, ResourceOption> nodeResourceMap =
            newConf.getNodeResourceMap();
        UpdateNodeResourceRequest updateRequest =
            UpdateNodeResourceRequest.newInstance(nodeResourceMap);
        updateNodeResource(updateRequest);
      }
      // refresh dynamic resource in ResourceTrackerService
      this.rmContext.getResourceTrackerService().
          updateDynamicResourceConfiguration(newConf);
      RMAuditLogger.logSuccess(user.getShortUserName(), argName,
              "AdminService");
      return response;
    } catch (IOException ioe) {
      throw logAndWrapException(ioe, user.getShortUserName(), argName, msg);
    }
  }

  private synchronized Configuration getConfiguration(Configuration conf,
      String... confFileNames) throws YarnException, IOException {
    for (String confFileName : confFileNames) {
      InputStream confFileInputStream = this.rmContext.getConfigurationProvider()
          .getConfigurationInputStream(conf, confFileName);
      if (confFileInputStream != null) {
        conf.addResource(confFileInputStream);
      }
    }
    return conf;
  }

  /*
   * Visibility could be private for test its made as default
   */
  @VisibleForTesting
  void refreshAll() throws ServiceFailedException {
    try {
      refreshQueues(RefreshQueuesRequest.newInstance());
      refreshNodes(RefreshNodesRequest.newInstance(DecommissionType.NORMAL));
      refreshSuperUserGroupsConfiguration(
          RefreshSuperUserGroupsConfigurationRequest.newInstance());
      refreshUserToGroupsMappings(
          RefreshUserToGroupsMappingsRequest.newInstance());
      if (getConfig().getBoolean(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
          false)) {
        refreshServiceAcls(RefreshServiceAclsRequest.newInstance());
      }
      refreshClusterMaxPriority(RefreshClusterMaxPriorityRequest.newInstance());
    } catch (Exception ex) {
      throw new ServiceFailedException(ex.getMessage());
    }
  }

  // only for testing
  @VisibleForTesting
  public AccessControlList getAccessControlList() {
    return ((ConfiguredYarnAuthorizer)authorizer).getAdminAcls();
  }

  @VisibleForTesting
  public Server getServer() {
    return this.server;
  }

  @Override
  public AddToClusterNodeLabelsResponse addToClusterNodeLabels(AddToClusterNodeLabelsRequest request)
      throws YarnException, IOException {
    String argName = "addToClusterNodeLabels";
    final String msg = "add labels.";
    UserGroupInformation user = checkAcls(argName);

    checkRMStatus(user.getShortUserName(), argName, msg);

    AddToClusterNodeLabelsResponse response =
        recordFactory.newRecordInstance(AddToClusterNodeLabelsResponse.class);
    try {
      rmContext.getNodeLabelManager().addToCluserNodeLabels(request.getNodeLabels());
      RMAuditLogger
          .logSuccess(user.getShortUserName(), argName, "AdminService");
      return response;
    } catch (IOException ioe) {
      throw logAndWrapException(ioe, user.getShortUserName(), argName, msg);
    }
  }

  @Override
  public RemoveFromClusterNodeLabelsResponse removeFromClusterNodeLabels(
      RemoveFromClusterNodeLabelsRequest request) throws YarnException, IOException {
    String operation = "removeFromClusterNodeLabels";
    final String msg = "remove labels.";

    UserGroupInformation user = checkAcls(operation);

    checkRMStatus(user.getShortUserName(), operation, msg);

    RemoveFromClusterNodeLabelsResponse response =
        recordFactory.newRecordInstance(RemoveFromClusterNodeLabelsResponse.class);
    try {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(request.getNodeLabels());
      RMAuditLogger
          .logSuccess(user.getShortUserName(), operation, "AdminService");
      return response;
    } catch (IOException ioe) {
      throw logAndWrapException(ioe, user.getShortUserName(), operation, msg);
    }
  }

  @Override
  public ReplaceLabelsOnNodeResponse replaceLabelsOnNode(
      ReplaceLabelsOnNodeRequest request) throws YarnException, IOException {
    String operation = "replaceLabelsOnNode";
    final String msg = "set node to labels.";

    try {
      NodeLabelsUtils.verifyCentralizedNodeLabelConfEnabled(operation,
          isCentralizedNodeLabelConfiguration);
    } catch (IOException ioe) {
      throw RPCUtil.getRemoteException(ioe);
    }

    UserGroupInformation user = checkAcls(operation);

    checkRMStatus(user.getShortUserName(), operation, msg);

    ReplaceLabelsOnNodeResponse response =
        recordFactory.newRecordInstance(ReplaceLabelsOnNodeResponse.class);
    try {
      rmContext.getNodeLabelManager().replaceLabelsOnNode(
          request.getNodeToLabels());
      RMAuditLogger
          .logSuccess(user.getShortUserName(), operation, "AdminService");
      return response;
    } catch (IOException ioe) {
      throw logAndWrapException(ioe, user.getShortUserName(), operation, msg);
    }
  }

  private void checkRMStatus(String user, String operation, String msg)
      throws StandbyException {
    if (!isRMActive()) {
      RMAuditLogger.logFailure(user, operation, "",
          "AdminService", "ResourceManager is not active. Can not " + msg);
      throwStandbyException();
    }
  }

  private YarnException logAndWrapException(Exception exception, String user,
      String operation, String msg) throws YarnException {
    LOG.warn("Exception " + msg, exception);
    RMAuditLogger.logFailure(user, operation, "",
        "AdminService", "Exception " + msg);
    return RPCUtil.getRemoteException(exception);
  }

  @Override
  public CheckForDecommissioningNodesResponse checkForDecommissioningNodes(
      CheckForDecommissioningNodesRequest checkForDecommissioningNodesRequest)
      throws IOException, YarnException {
    String argName = "checkForDecommissioningNodes";
    final String msg = "check for decommissioning nodes.";
    UserGroupInformation user = checkAcls("checkForDecommissioningNodes");

    checkRMStatus(user.getShortUserName(), argName, msg);

    Set<NodeId> decommissioningNodes = rmContext.getNodesListManager()
        .checkForDecommissioningNodes();
    RMAuditLogger.logSuccess(user.getShortUserName(), argName, "AdminService");
    CheckForDecommissioningNodesResponse response = recordFactory
        .newRecordInstance(CheckForDecommissioningNodesResponse.class);
    response.setDecommissioningNodes(decommissioningNodes);
    return response;
  }

  @Override
  public RefreshClusterMaxPriorityResponse refreshClusterMaxPriority(
      RefreshClusterMaxPriorityRequest request) throws YarnException,
      IOException {
    String argName = "refreshClusterMaxPriority";
    String msg = "refresh cluster max priority";
    UserGroupInformation user = checkAcls(argName);

    checkRMStatus(user.getShortUserName(), argName, msg);
    try {
      Configuration conf =
          getConfiguration(new Configuration(false),
              YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);

      rmContext.getScheduler().setClusterMaxPriority(conf);

      RMAuditLogger
          .logSuccess(user.getShortUserName(), argName, "AdminService");
      return recordFactory
          .newRecordInstance(RefreshClusterMaxPriorityResponse.class);
    } catch (YarnException e) {
      throw logAndWrapException(e, user.getShortUserName(), argName, msg);
    }
  }

  public String getHAZookeeperConnectionState() {
    if (!rmContext.isHAEnabled()) {
      return "ResourceManager HA is not enabled.";
    } else if (!autoFailoverEnabled) {
      return "Auto Failover is not enabled.";
    }
    if (curatorEnabled) {
      return "Connected to zookeeper : " + rmContext
          .getLeaderElectorService().getCuratorClient().getZookeeperClient()
          .isConnected();
    } else {
      return this.embeddedElector.getHAZookeeperConnectionState();
    }
  }
}
