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
package org.apache.hadoop.hdfs.nfs.nfs3;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.nfs.nfs3.OpenFileCtx.CommitCtx;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.IdUserGroup;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.junit.Test;
import org.mockito.Mockito;

public class TestOpenFileCtxCache {
  static boolean cleaned = false;

  @Test
  public void testEviction() throws IOException, InterruptedException {
    Configuration conf = new Configuration();

    // Only two entries will be in the cache
    conf.setInt(Nfs3Constant.MAX_OPEN_FILES, 2);

    DFSClient dfsClient = Mockito.mock(DFSClient.class);
    Nfs3FileAttributes attr = new Nfs3FileAttributes();
    HdfsDataOutputStream fos = Mockito.mock(HdfsDataOutputStream.class);
    Mockito.when(fos.getPos()).thenReturn((long) 0);

    OpenFileCtx context1 = new OpenFileCtx(fos, attr, "/dumpFilePath",
        dfsClient, new IdUserGroup());
    OpenFileCtx context2 = new OpenFileCtx(fos, attr, "/dumpFilePath",
        dfsClient, new IdUserGroup());
    OpenFileCtx context3 = new OpenFileCtx(fos, attr, "/dumpFilePath",
        dfsClient, new IdUserGroup());
    OpenFileCtx context4 = new OpenFileCtx(fos, attr, "/dumpFilePath",
        dfsClient, new IdUserGroup());
    OpenFileCtx context5 = new OpenFileCtx(fos, attr, "/dumpFilePath",
        dfsClient, new IdUserGroup());

    OpenFileCtxCache cache = new OpenFileCtxCache(conf, 10 * 60 * 100);

    boolean ret = cache.put(new FileHandle(1), context1);
    assertTrue(ret);
    Thread.sleep(1000);
    ret = cache.put(new FileHandle(2), context2);
    assertTrue(ret);
    ret = cache.put(new FileHandle(3), context3);
    assertFalse(ret);
    assertTrue(cache.size() == 2);

    // Wait for the oldest stream to be evict-able, insert again
    Thread.sleep(Nfs3Constant.OUTPUT_STREAM_TIMEOUT_MIN_DEFAULT);
    assertTrue(cache.size() == 2);

    ret = cache.put(new FileHandle(3), context3);
    assertTrue(ret);
    assertTrue(cache.size() == 2);
    assertTrue(cache.get(new FileHandle(1)) == null);

    // Test inactive entry is evicted immediately
    context3.setActiveStatusForTest(false);
    ret = cache.put(new FileHandle(4), context4);
    assertTrue(ret);

    // Now the cache has context2 and context4
    // Test eviction failure if all entries have pending work.
    context2.getPendingWritesForTest().put(new OffsetRange(0, 100),
        new WriteCtx(null, 0, 0, 0, null, null, null, 0, false, null));
    context4.getPendingCommitsForTest().put(new Long(100),
        new CommitCtx(0, null, 0, attr));
    Thread.sleep(Nfs3Constant.OUTPUT_STREAM_TIMEOUT_MIN_DEFAULT);
    ret = cache.put(new FileHandle(5), context5);
    assertFalse(ret);
  }

  @Test
  public void testScan() throws IOException, InterruptedException {
    Configuration conf = new Configuration();

    // Only two entries will be in the cache
    conf.setInt(Nfs3Constant.MAX_OPEN_FILES, 2);

    DFSClient dfsClient = Mockito.mock(DFSClient.class);
    Nfs3FileAttributes attr = new Nfs3FileAttributes();
    HdfsDataOutputStream fos = Mockito.mock(HdfsDataOutputStream.class);
    Mockito.when(fos.getPos()).thenReturn((long) 0);

    OpenFileCtx context1 = new OpenFileCtx(fos, attr, "/dumpFilePath",
        dfsClient, new IdUserGroup());
    OpenFileCtx context2 = new OpenFileCtx(fos, attr, "/dumpFilePath",
        dfsClient, new IdUserGroup());
    OpenFileCtx context3 = new OpenFileCtx(fos, attr, "/dumpFilePath",
        dfsClient, new IdUserGroup());
    OpenFileCtx context4 = new OpenFileCtx(fos, attr, "/dumpFilePath",
        dfsClient, new IdUserGroup());

    OpenFileCtxCache cache = new OpenFileCtxCache(conf, 10 * 60 * 100);

    // Test cleaning expired entry
    boolean ret = cache.put(new FileHandle(1), context1);
    assertTrue(ret);
    ret = cache.put(new FileHandle(2), context2);
    assertTrue(ret);
    Thread.sleep(Nfs3Constant.OUTPUT_STREAM_TIMEOUT_MIN_DEFAULT + 1);
    cache.scan(Nfs3Constant.OUTPUT_STREAM_TIMEOUT_MIN_DEFAULT);
    assertTrue(cache.size() == 0);

    // Test cleaning inactive entry
    ret = cache.put(new FileHandle(3), context3);
    assertTrue(ret);
    ret = cache.put(new FileHandle(4), context4);
    assertTrue(ret);
    context3.setActiveStatusForTest(false);
    cache.scan(Nfs3Constant.OUTPUT_STREAM_TIMEOUT_DEFAULT);
    assertTrue(cache.size() == 1);
    assertTrue(cache.get(new FileHandle(3)) == null);
    assertTrue(cache.get(new FileHandle(4)) != null);
  }
}
