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
package org.apache.hadoop.hdfs.server.federation.router.async;

import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.router.Quota;
import org.apache.hadoop.hdfs.server.federation.router.RemoteMethod;
import org.apache.hadoop.hdfs.server.federation.router.RemoteParam;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcClient;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncApply;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncReturn;

/**
 * Provides asynchronous operations for managing quotas in HDFS Federation.
 * This class extends {@link org.apache.hadoop.hdfs.server.federation.router.Quota}
 * and overrides its methods to perform quota operations in a non-blocking manner,
 * allowing for concurrent execution and improved performance.
 */
public class AsyncQuota extends Quota {

  /** RPC server to receive client calls. */
  private final RouterRpcServer rpcServer;
  /** RPC clients to connect to the Namenodes. */
  private final RouterRpcClient rpcClient;
  private final Router router;

  public AsyncQuota(Router router, RouterRpcServer server) {
    super(router, server);
    this.router = router;
    this.rpcServer = server;
    this.rpcClient =  this.rpcServer.getRPCClient();
  }

  /**
   * Async get aggregated quota usage for the federation path.
   * @param path Federation path.
   * @return Aggregated quota.
   * @throws IOException If the quota system is disabled.
   */
  @Override
  public QuotaUsage getQuotaUsage(String path) throws IOException {
    getEachQuotaUsage(path);

    asyncApply(o -> {
      Map<RemoteLocation, QuotaUsage> results = (Map<RemoteLocation, QuotaUsage>) o;
      try {
        return aggregateQuota(path, results);
      } catch (IOException e) {
        throw new CompletionException(e);
      }
    });
    return asyncReturn(QuotaUsage.class);
  }

  /**
   * Get quota usage for the federation path.
   * @param path Federation path.
   * @return quota usage for each remote location.
   * @throws IOException If the quota system is disabled.
   */
  @Override
  protected Map<RemoteLocation, QuotaUsage> getEachQuotaUsage(String path)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);
    if (!router.isQuotaEnabled()) {
      throw new IOException("The quota system is disabled in Router.");
    }

    final List<RemoteLocation> quotaLocs = getValidQuotaLocations(path);
    RemoteMethod method = new RemoteMethod("getQuotaUsage",
        new Class<?>[] {String.class}, new RemoteParam());
    rpcClient.invokeConcurrent(
        quotaLocs, method, true, false, QuotaUsage.class);
    return asyncReturn(Map.class);
  }
}
