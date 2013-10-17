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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentNavigableMap;

import junit.framework.Assert;

import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.nfs.nfs3.OpenFileCtx.COMMIT_STATUS;
import org.apache.hadoop.hdfs.nfs.nfs3.OpenFileCtx.CommitCtx;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.IdUserGroup;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant.WriteStableHow;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.request.WRITE3Request;
import org.junit.Test;
import org.mockito.Mockito;

public class TestWrites {
  @Test
  public void testAlterWriteRequest() throws IOException {
    int len = 20;
    byte[] data = new byte[len];
    ByteBuffer buffer = ByteBuffer.wrap(data);

    for (int i = 0; i < len; i++) {
      buffer.put((byte) i);
    }
    buffer.flip();
    int originalCount = buffer.array().length;
    WRITE3Request request = new WRITE3Request(new FileHandle(), 0, data.length,
        WriteStableHow.UNSTABLE, buffer);

    WriteCtx writeCtx1 = new WriteCtx(request.getHandle(), request.getOffset(),
        request.getCount(), WriteCtx.INVALID_ORIGINAL_COUNT,
        request.getStableHow(), request.getData(), null, 1, false,
        WriteCtx.DataState.NO_DUMP);

    Assert.assertTrue(writeCtx1.getData().array().length == originalCount);

    // Now change the write request
    OpenFileCtx.alterWriteRequest(request, 12);

    WriteCtx writeCtx2 = new WriteCtx(request.getHandle(), request.getOffset(),
        request.getCount(), originalCount, request.getStableHow(),
        request.getData(), null, 2, false, WriteCtx.DataState.NO_DUMP);
    ByteBuffer appendedData = writeCtx2.getData();

    int position = appendedData.position();
    int limit = appendedData.limit();
    Assert.assertTrue(position == 12);
    Assert.assertTrue(limit - position == 8);
    Assert.assertTrue(appendedData.get(position) == (byte) 12);
    Assert.assertTrue(appendedData.get(position + 1) == (byte) 13);
    Assert.assertTrue(appendedData.get(position + 2) == (byte) 14);
    Assert.assertTrue(appendedData.get(position + 7) == (byte) 19);

    // Test current file write offset is at boundaries
    buffer.position(0);
    request = new WRITE3Request(new FileHandle(), 0, data.length,
        WriteStableHow.UNSTABLE, buffer);
    OpenFileCtx.alterWriteRequest(request, 1);
    WriteCtx writeCtx3 = new WriteCtx(request.getHandle(), request.getOffset(),
        request.getCount(), originalCount, request.getStableHow(),
        request.getData(), null, 2, false, WriteCtx.DataState.NO_DUMP);
    appendedData = writeCtx3.getData();
    position = appendedData.position();
    limit = appendedData.limit();
    Assert.assertTrue(position == 1);
    Assert.assertTrue(limit - position == 19);
    Assert.assertTrue(appendedData.get(position) == (byte) 1);
    Assert.assertTrue(appendedData.get(position + 18) == (byte) 19);

    // Reset buffer position before test another boundary
    buffer.position(0);
    request = new WRITE3Request(new FileHandle(), 0, data.length,
        WriteStableHow.UNSTABLE, buffer);
    OpenFileCtx.alterWriteRequest(request, 19);
    WriteCtx writeCtx4 = new WriteCtx(request.getHandle(), request.getOffset(),
        request.getCount(), originalCount, request.getStableHow(),
        request.getData(), null, 2, false, WriteCtx.DataState.NO_DUMP);
    appendedData = writeCtx4.getData();
    position = appendedData.position();
    limit = appendedData.limit();
    Assert.assertTrue(position == 19);
    Assert.assertTrue(limit - position == 1);
    Assert.assertTrue(appendedData.get(position) == (byte) 19);
  }
  
  @Test
  // Validate all the commit check return codes OpenFileCtx.COMMIT_STATUS, which
  // includes COMMIT_FINISHED, COMMIT_WAIT, COMMIT_INACTIVE_CTX,
  // COMMIT_INACTIVE_WITH_PENDING_WRITE, COMMIT_ERROR, and COMMIT_DO_SYNC.
  public void testCheckCommit() throws IOException {
    DFSClient dfsClient = Mockito.mock(DFSClient.class);
    Nfs3FileAttributes attr = new Nfs3FileAttributes();
    HdfsDataOutputStream fos = Mockito.mock(HdfsDataOutputStream.class);
    Mockito.when(fos.getPos()).thenReturn((long) 0);

    OpenFileCtx ctx = new OpenFileCtx(fos, attr, "/dumpFilePath", dfsClient,
        new IdUserGroup());

    COMMIT_STATUS ret;

    // Test inactive open file context
    ctx.setActiveStatusForTest(false);
    ret = ctx.checkCommit(dfsClient, 0, null, 1, attr);
    Assert.assertTrue(ret == COMMIT_STATUS.COMMIT_INACTIVE_CTX);

    ctx.getPendingWritesForTest().put(new OffsetRange(5, 10),
        new WriteCtx(null, 0, 0, 0, null, null, null, 0, false, null));
    ret = ctx.checkCommit(dfsClient, 0, null, 1, attr);
    Assert.assertTrue(ret == COMMIT_STATUS.COMMIT_INACTIVE_WITH_PENDING_WRITE);

    // Test request with non zero commit offset
    ctx.setActiveStatusForTest(true);
    Mockito.when(fos.getPos()).thenReturn((long) 10);
    ret = ctx.checkCommit(dfsClient, 5, null, 1, attr);
    Assert.assertTrue(ret == COMMIT_STATUS.COMMIT_DO_SYNC);
    ret = ctx.checkCommit(dfsClient, 10, null, 1, attr);
    Assert.assertTrue(ret == COMMIT_STATUS.COMMIT_DO_SYNC);

    ConcurrentNavigableMap<Long, CommitCtx> commits = ctx
        .getPendingCommitsForTest();
    Assert.assertTrue(commits.size() == 0);
    ret = ctx.checkCommit(dfsClient, 11, null, 1, attr);
    Assert.assertTrue(ret == COMMIT_STATUS.COMMIT_WAIT);
    Assert.assertTrue(commits.size() == 1);
    long key = commits.firstKey();
    Assert.assertTrue(key == 11);

    // Test request with zero commit offset
    commits.remove(new Long(11));
    // There is one pending write [5,10]
    ret = ctx.checkCommit(dfsClient, 0, null, 1, attr);
    Assert.assertTrue(ret == COMMIT_STATUS.COMMIT_WAIT);
    Assert.assertTrue(commits.size() == 1);
    key = commits.firstKey();
    Assert.assertTrue(key == 9);

    // Empty pending writes
    ctx.getPendingWritesForTest().remove(new OffsetRange(5, 10));
    ret = ctx.checkCommit(dfsClient, 0, null, 1, attr);
    Assert.assertTrue(ret == COMMIT_STATUS.COMMIT_FINISHED);
  }
}
