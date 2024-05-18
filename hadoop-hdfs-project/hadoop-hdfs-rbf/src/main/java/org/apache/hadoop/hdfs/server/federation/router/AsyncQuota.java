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

import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class AsyncQuota extends Quota{
  public AsyncQuota(Router router, RouterRpcServer server) {
    super(router, server);
  }

  /**
   * Get aggregated quota usage for the federation path.
   * @param path Federation path.
   * @return Aggregated quota.
   * @throws IOException If the quota system is disabled.
   */
  public QuotaUsage getQuotaUsage(String path) throws IOException {
    getEachQuotaUsage(path);
    CompletableFuture<Object> completableFuture =
        RouterAsyncRpcUtil.getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<RemoteLocation, QuotaUsage> results = (Map<RemoteLocation, QuotaUsage>) o;
      try {
        return AsyncQuota.super.aggregateQuota(path, results);
      } catch (IOException e) {
        throw new CompletionException(e);
      }
    });
    RouterAsyncRpcUtil.setCurCompletableFuture(completableFuture);
    return RouterAsyncRpcUtil.asyncReturn(QuotaUsage.class);
  }
}
