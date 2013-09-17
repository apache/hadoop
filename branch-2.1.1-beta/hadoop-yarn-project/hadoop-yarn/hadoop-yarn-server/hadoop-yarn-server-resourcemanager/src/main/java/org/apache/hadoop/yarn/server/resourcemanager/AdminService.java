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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.service.AbstractService;
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
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;

public class AdminService extends AbstractService implements ResourceManagerAdministrationProtocol {

  private static final Log LOG = LogFactory.getLog(AdminService.class);

  private final Configuration conf;
  private final ResourceScheduler scheduler;
  private final RMContext rmContext;
  private final NodesListManager nodesListManager;
  
  private final ClientRMService clientRMService;
  private final ApplicationMasterService applicationMasterService;
  private final ResourceTrackerService resourceTrackerService;
  
  private Server server;
  private InetSocketAddress masterServiceAddress;
  private AccessControlList adminAcl;
  
  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  public AdminService(Configuration conf, ResourceScheduler scheduler, 
      RMContext rmContext, NodesListManager nodesListManager, 
      ClientRMService clientRMService, 
      ApplicationMasterService applicationMasterService,
      ResourceTrackerService resourceTrackerService) {
    super(AdminService.class.getName());
    this.conf = conf;
    this.scheduler = scheduler;
    this.rmContext = rmContext;
    this.nodesListManager = nodesListManager;
    this.clientRMService = clientRMService;
    this.applicationMasterService = applicationMasterService;
    this.resourceTrackerService = resourceTrackerService;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    masterServiceAddress = conf.getSocketAddr(
        YarnConfiguration.RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_PORT);
    adminAcl = new AccessControlList(conf.get(
        YarnConfiguration.YARN_ADMIN_ACL,
        YarnConfiguration.DEFAULT_YARN_ADMIN_ACL));
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    this.server =
      rpc.getServer(ResourceManagerAdministrationProtocol.class, this, masterServiceAddress,
          conf, null,
          conf.getInt(YarnConfiguration.RM_ADMIN_CLIENT_THREAD_COUNT, 
              YarnConfiguration.DEFAULT_RM_ADMIN_CLIENT_THREAD_COUNT));
    
    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      refreshServiceAcls(conf, new RMPolicyProvider());
    }

    this.server.start();
    conf.updateConnectAddr(YarnConfiguration.RM_ADMIN_ADDRESS,
                           server.getListenerAddress());
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.server != null) {
      this.server.stop();
    }
    super.serviceStop();
  }

  private UserGroupInformation checkAcls(String method) throws YarnException {
    UserGroupInformation user;
    try {
      user = UserGroupInformation.getCurrentUser();
    } catch (IOException ioe) {
      LOG.warn("Couldn't get current user", ioe);

      RMAuditLogger.logFailure("UNKNOWN", method,
          adminAcl.toString(), "AdminService",
          "Couldn't get current user");
      throw RPCUtil.getRemoteException(ioe);
    }

    if (!adminAcl.isUserAllowed(user)) {
      LOG.warn("User " + user.getShortUserName() + " doesn't have permission" +
      " to call '" + method + "'");

      RMAuditLogger.logFailure(user.getShortUserName(), method,
          adminAcl.toString(), "AdminService",
          AuditConstants.UNAUTHORIZED_USER);

      throw RPCUtil.getRemoteException(
          new AccessControlException("User " + user.getShortUserName() + 
              " doesn't have permission" +
              " to call '" + method + "'")
          );
    }
    LOG.info("RM Admin: " + method + " invoked by user " + 
        user.getShortUserName());
      
    return user;
  }
  
  @Override
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request)
      throws YarnException {
    UserGroupInformation user = checkAcls("refreshQueues");
    try {
      scheduler.reinitialize(conf, this.rmContext);
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
      throws YarnException {
    UserGroupInformation user = checkAcls("refreshNodes");
    try {
      this.nodesListManager.refreshNodes(new YarnConfiguration());
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
      throws YarnException {
    UserGroupInformation user = checkAcls("refreshSuperUserGroupsConfiguration");
    
    ProxyUsers.refreshSuperUserGroupsConfiguration(new Configuration());
    RMAuditLogger.logSuccess(user.getShortUserName(),
        "refreshSuperUserGroupsConfiguration", "AdminService");
    
    return recordFactory.newRecordInstance(
        RefreshSuperUserGroupsConfigurationResponse.class);
  }

  @Override
  public RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(
      RefreshUserToGroupsMappingsRequest request) throws YarnException {
    UserGroupInformation user = checkAcls("refreshUserToGroupsMappings");
    
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
    clientRMService.refreshServiceAcls(conf, policyProvider);
    applicationMasterService.refreshServiceAcls(conf, policyProvider);
    resourceTrackerService.refreshServiceAcls(conf, policyProvider);
    
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
  
}
