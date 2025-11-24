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

import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.router.RouterCacheAdmin;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.federation.router.async.utils.ApplyFunction;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncApply;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncReturn;

/**
 * Module that implements all the asynchronous RPC calls in
 * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol} related to Cache Admin
 * in the {@link RouterRpcServer}.
 */
public class RouterAsyncCacheAdmin extends RouterCacheAdmin {

  public RouterAsyncCacheAdmin(RouterRpcServer server) {
    super(server);
  }

  /**
   * Asynchronously adds a new cache directive with the given path and flags.
   * This method invokes the addCacheDirective method concurrently across all
   * namespaces, and returns the first response as a long value representing the
   * directive ID.
   *
   * @param path The cache directive path.
   * @param flags The cache flags.
   * @return The ID of the newly added cache directive.
   * @throws IOException If an I/O error occurs.
   */
  @Override
  public long addCacheDirective(
      CacheDirectiveInfo path, EnumSet<CacheFlag> flags) throws IOException {
    invokeAddCacheDirective(path, flags);
    asyncApply((ApplyFunction<Map<RemoteLocation, Long>, Long>)
        response -> response.values().iterator().next());
    return asyncReturn(Long.class);
  }

  /**
   * Asynchronously lists cache directives based on the provided previous ID and filter.
   * This method invokes the listCacheDirectives method concurrently across all
   * namespaces, and returns the first response as a BatchedEntries object containing
   * the cache directive entries.
   *
   * @param prevId The previous ID from which to start listing.
   * @param filter The filter to apply to the cache directives.
   * @return BatchedEntries of cache directive entries.
   * @throws IOException If an I/O error occurs.
   */
  @Override
  public BatchedEntries<CacheDirectiveEntry> listCacheDirectives(
      long prevId, CacheDirectiveInfo filter) throws IOException {
    invokeListCacheDirectives(prevId, filter);
    asyncApply((ApplyFunction<Map,
        BatchedEntries<CacheDirectiveEntry>>)
        response -> (BatchedEntries<CacheDirectiveEntry>) response.values().iterator().next());
    return asyncReturn(BatchedEntries.class);
  }

  /**
   * Asynchronously lists cache pools starting from the provided key.
   * This method invokes the listCachePools method concurrently across all namespaces,
   * and returns the first response as a BatchedEntries object containing the cache
   * pool entries.
   *
   * @param prevKey The previous key from which to start listing.
   * @return BatchedEntries of cache pool entries.
   * @throws IOException If an I/O error occurs.
   */
  @Override
  public BatchedEntries<CachePoolEntry> listCachePools(String prevKey) throws IOException {
    invokeListCachePools(prevKey);
    asyncApply((ApplyFunction<Map<FederationNamespaceInfo, BatchedEntries>,
        BatchedEntries<CachePoolEntry>>)
        results -> results.values().iterator().next());
    return asyncReturn(BatchedEntries.class);
  }
}
