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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.EnumSet;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.syncReturn;
import static org.junit.Assert.assertEquals;

/**
 * Used to test the functionality of {@link RouterAsyncCacheAdmin}.
 */
public class TestRouterAsyncCacheAdmin extends RouterAsyncProtocolTestBase {
  private RouterAsyncCacheAdmin asyncCacheAdmin;

  @Before
  public void setup() throws IOException {
    asyncCacheAdmin = new RouterAsyncCacheAdmin(getRouterAsyncRpcServer());
    FSDataOutputStream fsDataOutputStream = getRouterFs().create(
        new Path("/testCache.file"), true);
    fsDataOutputStream.write(new byte[1024]);
    fsDataOutputStream.close();
  }

  @Test
  public void testRouterAsyncCacheAdmin() throws Exception {
    asyncCacheAdmin.addCachePool(new CachePoolInfo("pool"));
    syncReturn(null);

    CacheDirectiveInfo path = new CacheDirectiveInfo.Builder().
        setPool("pool").
        setPath(new Path("/testCache.file")).
        build();
    asyncCacheAdmin.addCacheDirective(path, EnumSet.of(CacheFlag.FORCE));
    long result = syncReturn(long.class);
    assertEquals(1, result);

    asyncCacheAdmin.listCachePools("");
    BatchedEntries<CachePoolEntry> cachePoolEntries = syncReturn(BatchedEntries.class);
    assertEquals("pool", cachePoolEntries.get(0).getInfo().getPoolName());

    CacheDirectiveInfo filter = new CacheDirectiveInfo.Builder().
        setPool("pool").
        build();
    asyncCacheAdmin.listCacheDirectives(0, filter);
    BatchedEntries<CacheDirectiveEntry> cacheDirectiveEntries = syncReturn(BatchedEntries.class);
    assertEquals(new Path("/testCache.file"), cacheDirectiveEntries.get(0).getInfo().getPath());

    CachePoolInfo pool = new CachePoolInfo("pool").setOwnerName("pool_user");
    asyncCacheAdmin.modifyCachePool(pool);
    syncReturn(null);

    asyncCacheAdmin.listCachePools("");
    cachePoolEntries = syncReturn(BatchedEntries.class);
    assertEquals("pool_user", cachePoolEntries.get(0).getInfo().getOwnerName());

    path = new CacheDirectiveInfo.Builder().
        setPool("pool").
        setPath(new Path("/testCache.file")).
        setReplication((short) 2).
        setId(1L).
        build();
    asyncCacheAdmin.modifyCacheDirective(path, EnumSet.of(CacheFlag.FORCE));
    syncReturn(null);

    asyncCacheAdmin.listCacheDirectives(0, filter);
    cacheDirectiveEntries = syncReturn(BatchedEntries.class);
    assertEquals(Short.valueOf((short) 2), cacheDirectiveEntries.get(0).getInfo().getReplication());

    asyncCacheAdmin.removeCacheDirective(1L);
    syncReturn(null);
    asyncCacheAdmin.removeCachePool("pool");
    syncReturn(null);
  }
}