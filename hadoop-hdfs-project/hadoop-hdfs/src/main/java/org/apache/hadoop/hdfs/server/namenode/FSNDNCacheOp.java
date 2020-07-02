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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.security.AccessControlException;

import java.io.IOException;
import java.util.EnumSet;

class FSNDNCacheOp {
  static CacheDirectiveInfo addCacheDirective(
      FSNamesystem fsn, CacheManager cacheManager,
      CacheDirectiveInfo directive, EnumSet<CacheFlag> flags,
      boolean logRetryCache)
      throws IOException {

    final FSPermissionChecker pc = getFsPermissionChecker(fsn);

    if (directive.getId() != null) {
      throw new IOException("addDirective: you cannot specify an ID " +
          "for this operation.");
    }
    CacheDirectiveInfo effectiveDirective =
        cacheManager.addDirective(directive, pc, flags);
    fsn.getEditLog().logAddCacheDirectiveInfo(effectiveDirective,
        logRetryCache);
    return effectiveDirective;
  }

  static void modifyCacheDirective(
      FSNamesystem fsn, CacheManager cacheManager, CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags, boolean logRetryCache) throws IOException {
    final FSPermissionChecker pc = getFsPermissionChecker(fsn);

    cacheManager.modifyDirective(directive, pc, flags);
    fsn.getEditLog().logModifyCacheDirectiveInfo(directive, logRetryCache);
  }

  static void removeCacheDirective(
      FSNamesystem fsn, CacheManager cacheManager, long id,
      boolean logRetryCache)
      throws IOException {
    final FSPermissionChecker pc = getFsPermissionChecker(fsn);

    cacheManager.removeDirective(id, pc);
    fsn.getEditLog().logRemoveCacheDirectiveInfo(id, logRetryCache);
  }

  static BatchedListEntries<CacheDirectiveEntry> listCacheDirectives(
      FSNamesystem fsn, CacheManager cacheManager,
      long startId, CacheDirectiveInfo filter) throws IOException {
    final FSPermissionChecker pc = getFsPermissionChecker(fsn);
    return cacheManager.listCacheDirectives(startId, filter, pc);
  }

  static CachePoolInfo addCachePool(
      FSNamesystem fsn, CacheManager cacheManager, CachePoolInfo req,
      boolean logRetryCache)
      throws IOException {
    CachePoolInfo info = cacheManager.addCachePool(req);
    fsn.getEditLog().logAddCachePool(info, logRetryCache);
    return info;
  }

  static void modifyCachePool(
      FSNamesystem fsn, CacheManager cacheManager, CachePoolInfo req,
      boolean logRetryCache) throws IOException {
    cacheManager.modifyCachePool(req);
    fsn.getEditLog().logModifyCachePool(req, logRetryCache);
  }

  static void removeCachePool(
      FSNamesystem fsn, CacheManager cacheManager, String cachePoolName,
      boolean logRetryCache) throws IOException {
    cacheManager.removeCachePool(cachePoolName);
    fsn.getEditLog().logRemoveCachePool(cachePoolName, logRetryCache);
  }

  static BatchedListEntries<CachePoolEntry> listCachePools(
      FSNamesystem fsn, CacheManager cacheManager, String prevKey)
      throws IOException {
    final FSPermissionChecker pc = getFsPermissionChecker(fsn);
    return cacheManager.listCachePools(pc, prevKey);
  }

  private static FSPermissionChecker getFsPermissionChecker(FSNamesystem fsn)
      throws AccessControlException {
    return fsn.isPermissionEnabled() ? fsn.getPermissionChecker() : null;
  }
}
