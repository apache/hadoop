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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.security.token.Token;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * This class tests the client connection caching in a single node
 * mini-cluster.
 */
public class TestSocketCache {
  static final Log LOG = LogFactory.getLog(TestSocketCache.class);

  static final int BLOCK_SIZE = 4096;
  static final int FILE_SIZE = 3 * BLOCK_SIZE;
  final static int CACHE_SIZE = 4;
  final static long CACHE_EXPIRY_MS = 200;
  static Configuration conf = null;
  static MiniDFSCluster cluster = null;
  static FileSystem fs = null;
  static SocketCache cache;

  static final Path testFile = new Path("/testConnCache.dat");
  static byte authenticData[] = null;

  static BlockReaderTestUtil util = null;


  /**
   * A mock Answer to remember the BlockReader used.
   *
   * It verifies that all invocation to DFSInputStream.getBlockReader()
   * use the same socket.
   */
  private class MockGetBlockReader implements Answer<RemoteBlockReader2> {
    public RemoteBlockReader2 reader = null;
    private Socket sock = null;

    @Override
    public RemoteBlockReader2 answer(InvocationOnMock invocation) throws Throwable {
      RemoteBlockReader2 prevReader = reader;
      reader = (RemoteBlockReader2) invocation.callRealMethod();
      if (sock == null) {
        sock = reader.dnSock;
      } else if (prevReader != null) {
        assertSame("DFSInputStream should use the same socket",
                   sock, reader.dnSock);
      }
      return reader;
    }
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    final int REPLICATION_FACTOR = 1;

    HdfsConfiguration confWithoutCache = new HdfsConfiguration();
    confWithoutCache.setInt(
        DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY, 0);
    util = new BlockReaderTestUtil(REPLICATION_FACTOR, confWithoutCache);
    cluster = util.getCluster();
    conf = util.getConf();

    authenticData = util.writeFile(testFile, FILE_SIZE / 1024);
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
                     int length)
      throws IOException {
    assertTrue("Test buffer too small", buffer.length >= offset + length);

    if (pos >= 0)
      in.seek(pos);

    LOG.info("Reading from file of size " + in.getFileLength() +
             " at offset " + in.getPos());

    while (length > 0) {
      int cnt = in.read(buffer, offset, length);
      assertTrue("Error in read", cnt > 0);
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
   * Test that the socket cache can be disabled by setting the capacity to
   * 0. Regression test for HDFS-3365.
   */
  @Test
  public void testDisableCache() throws IOException {
    LOG.info("Starting testDisableCache()");

    // Configure a new instance with no caching, ensure that it doesn't
    // cache anything

    FileSystem fsWithoutCache = FileSystem.newInstance(conf);
    try {
      DFSTestUtil.readFile(fsWithoutCache, testFile);
      assertEquals(0, ((DistributedFileSystem)fsWithoutCache).dfs.socketCache.size());
    } finally {
      fsWithoutCache.close();
    }
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    util.shutdown();
  }
}
