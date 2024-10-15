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

import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.router.async.ApplyFunction;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncApply;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncReturn;

/**
 * Module that implements all the asynchronous RPC calls in
 * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol} related to Cache Admin
 * in the {@link RouterRpcServer}.
 */
public class RouterAsyncCacheAdmin extends RouterCacheAdmin {

  public RouterAsyncCacheAdmin(RouterRpcServer server) {
    super(server);
  }

  @Override
  public long addCacheDirective(
      CacheDirectiveInfo path, EnumSet<CacheFlag> flags) throws IOException {
    invokeAddCacheDirective(path, flags);
    asyncApply((ApplyFunction<Map<RemoteLocation, Long>, Long>)
        response -> response.values().iterator().next());
    return asyncReturn(Long.class);
  }

  @Override
  public BatchedEntries<CacheDirectiveEntry> listCacheDirectives(
      long prevId, CacheDirectiveInfo filter) throws IOException {
    invokeListCacheDirectives(prevId, filter);
    asyncApply((ApplyFunction<Map,
        BatchedEntries<CacheDirectiveEntry>>)
        response -> (BatchedEntries<CacheDirectiveEntry>) response.values().iterator().next());
    return asyncReturn(BatchedEntries.class);
  }

  @Override
  public BatchedEntries<CachePoolEntry> listCachePools(String prevKey) throws IOException {
    invokeListCachePools(prevKey);
    asyncApply((ApplyFunction<Map<FederationNamespaceInfo, BatchedEntries>,
        BatchedEntries<CachePoolEntry>>)
        results -> results.values().iterator().next());
    return asyncReturn(BatchedEntries.class);
  }
}
