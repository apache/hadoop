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

package org.apache.hadoop.mapreduce.v2.hs.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogDeletionService;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserMappingsProtocolService;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolServerSideTranslatorPB;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetUserMappingsProtocolService;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolServerSideTranslatorPB;
import org.apache.hadoop.mapreduce.v2.api.HSAdminProtocol;
import org.apache.hadoop.mapreduce.v2.api.HSAdminRefreshProtocolPB;
import org.apache.hadoop.mapreduce.v2.app.security.authorize.ClientHSPolicyProvider;
import org.apache.hadoop.mapreduce.v2.hs.HSAuditLogger;
import org.apache.hadoop.mapreduce.v2.hs.HSAuditLogger.AuditConstants;
import org.apache.hadoop.mapreduce.v2.hs.JobHistory;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.HSAdminRefreshProtocolService;
import org.apache.hadoop.mapreduce.v2.hs.protocolPB.HSAdminRefreshProtocolServerSideTranslatorPB;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Private
public class HSAdminServer extends AbstractService implements HSAdminProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(HSAdminServer.class);
  private AccessControlList adminAcl;
  private AggregatedLogDeletionService aggLogDelService = null;

  /** The RPC server that listens to requests from clients */
  protected RPC.Server clientRpcServer;
  protected InetSocketAddress clientRpcAddress;
  private static final String HISTORY_ADMIN_SERVER = "HSAdminServer";
  private JobHistory jobHistoryService = null;

  private UserGroupInformation loginUGI;

  public HSAdminServer(AggregatedLogDeletionService aggLogDelService,
      JobHistory jobHistoryService) {
    super(HSAdminServer.class.getName());
    this.aggLogDelService = aggLogDelService;
    this.jobHistoryService = jobHistoryService;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    RPC.setProtocolEngine(conf, RefreshUserMappingsProtocolPB.class,
        ProtobufRpcEngine.class);

    RefreshUserMappingsProtocolServerSideTranslatorPB refreshUserMappingXlator = new RefreshUserMappingsProtocolServerSideTranslatorPB(
        this);
    BlockingService refreshUserMappingService = RefreshUserMappingsProtocolService
        .newReflectiveBlockingService(refreshUserMappingXlator);

    GetUserMappingsProtocolServerSideTranslatorPB getUserMappingXlator = new GetUserMappingsProtocolServerSideTranslatorPB(
        this);
    BlockingService getUserMappingService = GetUserMappingsProtocolService
        .newReflectiveBlockingService(getUserMappingXlator);

    HSAdminRefreshProtocolServerSideTranslatorPB refreshHSAdminProtocolXlator = new HSAdminRefreshProtocolServerSideTranslatorPB(
        this);
    BlockingService refreshHSAdminProtocolService = HSAdminRefreshProtocolService
        .newReflectiveBlockingService(refreshHSAdminProtocolXlator);

    clientRpcAddress = conf.getSocketAddr(
        JHAdminConfig.MR_HISTORY_BIND_HOST,
        JHAdminConfig.JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_PORT);
    clientRpcServer = new RPC.Builder(conf)
        .setProtocol(RefreshUserMappingsProtocolPB.class)
        .setInstance(refreshUserMappingService)
        .setBindAddress(clientRpcAddress.getHostName())
        .setPort(clientRpcAddress.getPort()).setVerbose(false).build();

    addProtocol(conf, GetUserMappingsProtocolPB.class, getUserMappingService);
    addProtocol(conf, HSAdminRefreshProtocolPB.class,
        refreshHSAdminProtocolService);

    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      clientRpcServer.refreshServiceAcl(conf, new ClientHSPolicyProvider());
    }

    adminAcl = new AccessControlList(conf.get(JHAdminConfig.JHS_ADMIN_ACL,
        JHAdminConfig.DEFAULT_JHS_ADMIN_ACL));

  }

  @Override
  protected void serviceStart() throws Exception {
    if (UserGroupInformation.isSecurityEnabled()) {
      loginUGI = UserGroupInformation.getLoginUser();
    } else {
      loginUGI = UserGroupInformation.getCurrentUser();
    }
    clientRpcServer.start();
  }

  @VisibleForTesting
  UserGroupInformation getLoginUGI() {
    return loginUGI;
  }

  @VisibleForTesting
  void setLoginUGI(UserGroupInformation ugi) {
    loginUGI = ugi;
  }

  @Override
  protected void serviceStop() throws Exception {
    if (clientRpcServer != null) {
      clientRpcServer.stop();
    }
  }

  private void addProtocol(Configuration conf, Class<?> protocol,
      BlockingService blockingService) throws IOException {
    RPC.setProtocolEngine(conf, protocol, ProtobufRpcEngine.class);
    clientRpcServer.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, protocol,
        blockingService);
  }

  private UserGroupInformation checkAcls(String method) throws IOException {
    UserGroupInformation user;
    try {
      user = UserGroupInformation.getCurrentUser();
    } catch (IOException ioe) {
      LOG.warn("Couldn't get current user", ioe);

      HSAuditLogger.logFailure("UNKNOWN", method, adminAcl.toString(),
          HISTORY_ADMIN_SERVER, "Couldn't get current user");

      throw ioe;
    }

    if (!adminAcl.isUserAllowed(user)) {
      LOG.warn("User " + user.getShortUserName() + " doesn't have permission"
          + " to call '" + method + "'");

      HSAuditLogger.logFailure(user.getShortUserName(), method,
          adminAcl.toString(), HISTORY_ADMIN_SERVER,
          AuditConstants.UNAUTHORIZED_USER);

      throw new AccessControlException("User " + user.getShortUserName()
          + " doesn't have permission" + " to call '" + method + "'");
    }
    LOG.info("HS Admin: " + method + " invoked by user "
        + user.getShortUserName());

    return user;
  }

  @Override
  public String[] getGroupsForUser(String user) throws IOException {
    return UserGroupInformation.createRemoteUser(user).getGroupNames();
  }

  @Override
  public void refreshUserToGroupsMappings() throws IOException {

    UserGroupInformation user = checkAcls("refreshUserToGroupsMappings");

    Groups.getUserToGroupsMappingService().refresh();

    HSAuditLogger.logSuccess(user.getShortUserName(),
        "refreshUserToGroupsMappings", HISTORY_ADMIN_SERVER);
  }

  @Override
  public void refreshSuperUserGroupsConfiguration() throws IOException {
    UserGroupInformation user = checkAcls("refreshSuperUserGroupsConfiguration");

    ProxyUsers.refreshSuperUserGroupsConfiguration(createConf());

    HSAuditLogger.logSuccess(user.getShortUserName(),
        "refreshSuperUserGroupsConfiguration", HISTORY_ADMIN_SERVER);
  }

  protected Configuration createConf() {
    return new Configuration();
  }

  @Override
  public void refreshAdminAcls() throws IOException {
    UserGroupInformation user = checkAcls("refreshAdminAcls");

    Configuration conf = createConf();
    adminAcl = new AccessControlList(conf.get(JHAdminConfig.JHS_ADMIN_ACL,
        JHAdminConfig.DEFAULT_JHS_ADMIN_ACL));
    HSAuditLogger.logSuccess(user.getShortUserName(), "refreshAdminAcls",
        HISTORY_ADMIN_SERVER);
  }

  @Override
  public void refreshLoadedJobCache() throws IOException {
    UserGroupInformation user = checkAcls("refreshLoadedJobCache");

    try {
      jobHistoryService.refreshLoadedJobCache();
    } catch (UnsupportedOperationException e) {
      HSAuditLogger.logFailure(user.getShortUserName(),
          "refreshLoadedJobCache", adminAcl.toString(), HISTORY_ADMIN_SERVER,
          e.getMessage());
      throw e;
    }
    HSAuditLogger.logSuccess(user.getShortUserName(), "refreshLoadedJobCache",
        HISTORY_ADMIN_SERVER);
  }

  @Override
  public void refreshLogRetentionSettings() throws IOException {
    UserGroupInformation user = checkAcls("refreshLogRetentionSettings");

    try {
      loginUGI.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws IOException {
          aggLogDelService.refreshLogRetentionSettings();
          return null;
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    HSAuditLogger.logSuccess(user.getShortUserName(),
        "refreshLogRetentionSettings", "HSAdminServer");
  }

  @Override
  public void refreshJobRetentionSettings() throws IOException {
    UserGroupInformation user = checkAcls("refreshJobRetentionSettings");

    try {
      loginUGI.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws IOException {
          jobHistoryService.refreshJobRetentionSettings();
          return null;
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    HSAuditLogger.logSuccess(user.getShortUserName(),
        "refreshJobRetentionSettings", HISTORY_ADMIN_SERVER);
  }
}
