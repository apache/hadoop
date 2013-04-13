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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.security.token.Token;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * This class tests the client connection caching in a single node
 * mini-cluster.
 */
public class TestConnCache {
  static final Log LOG = LogFactory.getLog(TestConnCache.class);

  static final int BLOCK_SIZE = 4096;
  static final int FILE_SIZE = 3 * BLOCK_SIZE;

  /**
   * A mock Answer to remember the BlockReader used.
   *
   * It verifies that all invocation to DFSInputStream.getBlockReader()
   * use the same peer.
   */
  private class MockGetBlockReader implements Answer<RemoteBlockReader2> {
    public RemoteBlockReader2 reader = null;
    private Peer peer = null;

    @Override
    public RemoteBlockReader2 answer(InvocationOnMock invocation) throws Throwable {
      RemoteBlockReader2 prevReader = reader;
      reader = (RemoteBlockReader2) invocation.callRealMethod();
      if (peer == null) {
        peer = reader.getPeer();
      } else if (prevReader != null) {
        Assert.assertSame("DFSInputStream should use the same peer",
                   peer, reader.getPeer());
      }
      return reader;
    }
  }

  /**
   * (Optionally) seek to position, read and verify data.
   *
   * Seek to specified position if pos is non-negative.
   */
  private void pread(DFSInputStream in,
                     long pos,
                     byte[] buffer,
                     int offset,
                     int length,
                     byte[] authenticData)
      throws IOException {
    Assert.assertTrue("Test buffer too small", buffer.length >= offset + length);

    if (pos >= 0)
      in.seek(pos);

    LOG.info("Reading from file of size " + in.getFileLength() +
             " at offset " + in.getPos());

    while (length > 0) {
      int cnt = in.read(buffer, offset, length);
      Assert.assertTrue("Error in read", cnt > 0);
      offset += cnt;
      length -= cnt;
    }

    // Verify
    for (int i = 0; i < length; ++i) {
      byte actual = buffer[i];
      byte expect = authenticData[(int)pos + i];
      assertEquals("Read data mismatch at file offset " + (pos + i) +
                   ". Expects " + expect + "; got " + actual,
                   actual, expect);
    }
  }

  /**
   * Read a file served entirely from one DN. Seek around and read from
   * different offsets. And verify that they all use the same socket.
   * @throws Exception 
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testReadFromOneDN() throws Exception {
    BlockReaderTestUtil util = new BlockReaderTestUtil(1,
        new HdfsConfiguration());
    final Path testFile = new Path("/testConnCache.dat");
    byte authenticData[] = util.writeFile(testFile, FILE_SIZE / 1024);
    DFSClient client = new DFSClient(
        new InetSocketAddress("localhost",
            util.getCluster().getNameNodePort()), util.getConf());
    DFSInputStream in = Mockito.spy(client.open(testFile.toString()));
    LOG.info("opened " + testFile.toString());
    byte[] dataBuf = new byte[BLOCK_SIZE];

    MockGetBlockReader answer = new MockGetBlockReader();
    Mockito.doAnswer(answer).when(in).getBlockReader(
                           (InetSocketAddress) Matchers.anyObject(),
                           (DatanodeInfo) Matchers.anyObject(),
                           Matchers.anyString(),
                           (ExtendedBlock) Matchers.anyObject(),
                           (Token<BlockTokenIdentifier>) Matchers.anyObject(),
                           Matchers.anyLong(),
                           Matchers.anyLong(),
                           Matchers.anyInt(),
                           Matchers.anyBoolean(),
                           Matchers.anyString());

    // Initial read
    pread(in, 0, dataBuf, 0, dataBuf.length, authenticData);
    // Read again and verify that the socket is the same
    pread(in, FILE_SIZE - dataBuf.length, dataBuf, 0, dataBuf.length,
        authenticData);
    pread(in, 1024, dataBuf, 0, dataBuf.length, authenticData);
    // No seek; just read
    pread(in, -1, dataBuf, 0, dataBuf.length, authenticData);
    pread(in, 64, dataBuf, 0, dataBuf.length / 2, authenticData);

    in.close();
  }
}
