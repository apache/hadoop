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

package org.apache.hadoop.yarn.server.sharedcachemanager;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.api.SCMAdminProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.RunSharedCacheCleanerTaskRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RunSharedCacheCleanerTaskResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;

/**
 * This service handles all SCMAdminProtocol rpc calls from administrators
 * to the shared cache manager.
 */
@Private
@Unstable
public class SCMAdminProtocolService extends AbstractService implements
    SCMAdminProtocol {

  private static final Log LOG = LogFactory.getLog(SCMAdminProtocolService.class);

  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private Server server;
  InetSocketAddress clientBindAddress;
  private final CleanerService cleanerService;
  private YarnAuthorizationProvider authorizer;
  public SCMAdminProtocolService(CleanerService cleanerService) {
    super(SCMAdminProtocolService.class.getName());
    this.cleanerService = cleanerService;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.clientBindAddress = getBindAddress(conf);
    authorizer = YarnAuthorizationProvider.getInstance(conf);
    super.serviceInit(conf);
  }

  InetSocketAddress getBindAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.SCM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_SCM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_SCM_ADMIN_PORT);
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    this.server =
        rpc.getServer(SCMAdminProtocol.class, this,
            clientBindAddress,
            conf, null, // Secret manager null for now (security not supported)
            conf.getInt(YarnConfiguration.SCM_ADMIN_CLIENT_THREAD_COUNT,
                YarnConfiguration.DEFAULT_SCM_ADMIN_CLIENT_THREAD_COUNT));

    // TODO: Enable service authorization (see YARN-2774)

    this.server.start();
    clientBindAddress =
        conf.updateConnectAddr(YarnConfiguration.SCM_ADMIN_ADDRESS,
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

  private void checkAcls(String method) throws YarnException {
    UserGroupInformation user;
    try {
      user = UserGroupInformation.getCurrentUser();
    } catch (IOException ioe) {
      LOG.warn("Couldn't get current user", ioe);
      throw RPCUtil.getRemoteException(ioe);
    }

    if (!authorizer.isAdmin(user)) {
      LOG.warn("User " + user.getShortUserName() + " doesn't have permission" +
          " to call '" + method + "'");

      throw RPCUtil.getRemoteException(
          new AccessControlException("User " + user.getShortUserName() +
          " doesn't have permission" + " to call '" + method + "'"));
    }
    LOG.info("SCM Admin: " + method + " invoked by user " +
        user.getShortUserName());
  }

  @Override
  public RunSharedCacheCleanerTaskResponse runCleanerTask(
      RunSharedCacheCleanerTaskRequest request) throws YarnException {
    checkAcls("runCleanerTask");
    RunSharedCacheCleanerTaskResponse response =
        recordFactory.newRecordInstance(RunSharedCacheCleanerTaskResponse.class);
    this.cleanerService.runCleanerTask();
    // if we are here, then we have submitted the request to the cleaner
    // service, ack the request to the admin client
    response.setAccepted(true);
    return response;
  }
}
