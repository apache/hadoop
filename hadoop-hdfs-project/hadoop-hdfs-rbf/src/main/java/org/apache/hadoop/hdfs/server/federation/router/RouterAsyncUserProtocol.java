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

package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer.merge;
import static org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil.getCompletableFuture;
import static org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil.setCurCompletableFuture;

public class RouterAsyncUserProtocol extends RouterUserProtocol{
  private static final Logger LOG =
      LoggerFactory.getLogger(RouterAsyncUserProtocol.class);

  /** RPC server to receive client calls. */
  private final RouterRpcServer rpcServer;
  /** RPC clients to connect to the Namenodes. */
  private final RouterRpcClient rpcClient;

  private final ActiveNamenodeResolver namenodeResolver;

  public RouterAsyncUserProtocol(RouterRpcServer server) {
    super(server);
    this.rpcServer = server;
    this.rpcClient = this.rpcServer.getRPCClient();
    this.namenodeResolver = this.rpcServer.getNamenodeResolver();
  }

  @Override
  public void refreshUserToGroupsMappings() throws IOException {
    LOG.debug("Refresh user groups mapping in Router.");
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    if (nss.isEmpty()) {
      Groups.getUserToGroupsMappingService().refresh();
      setCurCompletableFuture(CompletableFuture.completedFuture(null));
    } else {
      RemoteMethod method = new RemoteMethod(RefreshUserMappingsProtocol.class,
          "refreshUserToGroupsMappings");
      rpcClient.invokeConcurrent(nss, method);
    }
  }

  @Override
  public void refreshSuperUserGroupsConfiguration() throws IOException {
    LOG.debug("Refresh superuser groups configuration in Router.");
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    if (nss.isEmpty()) {
      ProxyUsers.refreshSuperUserGroupsConfiguration();
      setCurCompletableFuture(CompletableFuture.completedFuture(null));
    } else {
      RemoteMethod method = new RemoteMethod(RefreshUserMappingsProtocol.class,
          "refreshSuperUserGroupsConfiguration");
      rpcClient.invokeConcurrent(nss, method);
    }
  }

  @Override
  public String[] getGroupsForUser(String user) throws IOException {
    LOG.debug("Getting groups for user {}", user);
    rpcServer.checkOperation(NameNode.OperationCategory.UNCHECKED);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    if (nss.isEmpty()) {
      setCurCompletableFuture(
          CompletableFuture
              .completedFuture(UserGroupInformation.createRemoteUser(user)
                  .getGroupNames()));
    } else {
      RemoteMethod method = new RemoteMethod(GetUserMappingsProtocol.class,
          "getGroupsForUser", new Class<?>[] {String.class}, user);
      rpcClient.invokeConcurrent(nss, method, String[].class);
      CompletableFuture<Object> completableFuture = getCompletableFuture();
      completableFuture = completableFuture.thenApply(o -> {
        Map<FederationNamespaceInfo, String[]> results =
            (Map<FederationNamespaceInfo, String[]>) o;
        return merge(results, String.class);
      });
      setCurCompletableFuture(completableFuture);
    }
    return null;
  }
}
