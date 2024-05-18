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

import org.apache.hadoop.fs.BatchedRemoteIterator;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil.asyncRequestThenApply;

public class RouterAsyncCacheAdmin extends RouterCacheAdmin{
  /** RPC server to receive client calls. */
  private final RouterRpcServer rpcServer;
  /** RPC clients to connect to the Namenodes. */
  private final RouterRpcClient rpcClient;
  /** Interface to identify the active NN for a nameservice or blockpool ID. */
  private final ActiveNamenodeResolver namenodeResolver;
  public RouterAsyncCacheAdmin(RouterRpcServer server) {
    super(server);
    this.rpcServer = server;
    this.rpcClient = this.rpcServer.getRPCClient();
    this.namenodeResolver = this.rpcClient.getNamenodeResolver();
  }

  public long addCacheDirective(
      CacheDirectiveInfo path, EnumSet<CacheFlag> flags) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, true);
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(path.getPath().toString(), true,
            false);
    RemoteMethod method = new RemoteMethod("addCacheDirective",
        new Class<?>[] {CacheDirectiveInfo.class, EnumSet.class},
        new RemoteParam(getRemoteMap(path, locations)), flags);
    return asyncRequestThenApply(
        () -> rpcClient.invokeConcurrent(
            locations, method, false, false, long.class),
        response -> response.values().iterator().next(),
        Long.class);
  }

  public BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> listCacheDirectives(
      long prevId,
      CacheDirectiveInfo filter) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, true);
    CompletableFuture<Object> completableFuture = null;
    if (filter.getPath() != null) {
      final List<RemoteLocation> locations = rpcServer
          .getLocationsForPath(filter.getPath().toString(), true, false);
      RemoteMethod method = new RemoteMethod("listCacheDirectives",
          new Class<?>[] {long.class, CacheDirectiveInfo.class}, prevId,
          new RemoteParam(getRemoteMap(filter, locations)));
      return asyncRequestThenApply(
          () -> rpcClient.invokeConcurrent(locations, method, false,
              false, BatchedRemoteIterator.BatchedEntries.class),
          response -> response.values().iterator().next(),
          BatchedRemoteIterator.BatchedEntries.class);
    }
    RemoteMethod method = new RemoteMethod("listCacheDirectives",
        new Class<?>[] {long.class, CacheDirectiveInfo.class}, prevId,
        filter);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    return asyncRequestThenApply(
        () -> rpcClient.invokeConcurrent(nss, method, true,
            false, BatchedRemoteIterator.BatchedEntries.class),
        results -> results.values().iterator().next(),
        BatchedRemoteIterator.BatchedEntries.class);
  }

  public BatchedRemoteIterator.BatchedEntries<CachePoolEntry> listCachePools(String prevKey)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, true);
    RemoteMethod method = new RemoteMethod("listCachePools",
        new Class<?>[] {String.class}, prevKey);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    return asyncRequestThenApply(
        () -> rpcClient.invokeConcurrent(nss, method, true,
            false, BatchedRemoteIterator.BatchedEntries.class),
        results -> results.values().iterator().next(),
        BatchedRemoteIterator.BatchedEntries.class);
  }
}
