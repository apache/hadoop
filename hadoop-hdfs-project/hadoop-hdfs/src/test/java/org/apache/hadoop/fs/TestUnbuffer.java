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
package org.apache.hadoop.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.PeerCache;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

public class TestUnbuffer {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestUnbuffer.class.getName());

  @Rule
  public ExpectedException exception = ExpectedException.none();

  /**
   * Test that calling Unbuffer closes sockets.
   */
  @Test
  public void testUnbufferClosesSockets() throws Exception {
    Configuration conf = new Configuration();
    // Set a new ClientContext.  This way, we will have our own PeerCache,
    // rather than sharing one with other unit tests.
    conf.set(HdfsClientConfigKeys.DFS_CLIENT_CONTEXT,
        "testUnbufferClosesSocketsContext");

    // Disable short-circuit reads.  With short-circuit, we wouldn't hold open a
    // TCP socket.
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, false);

    // Set a really long socket timeout to avoid test timing issues.
    conf.setLong(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY,
        100000000L);
    conf.setLong(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY,
        100000000L);

    MiniDFSCluster cluster = null;
    FSDataInputStream stream = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      DistributedFileSystem dfs = (DistributedFileSystem)
          FileSystem.newInstance(conf);
      final Path TEST_PATH = new Path("/test1");
      DFSTestUtil.createFile(dfs, TEST_PATH, 128, (short)1, 1);
      stream = dfs.open(TEST_PATH);
      // Read a byte.  This will trigger the creation of a block reader.
      stream.seek(2);
      int b = stream.read();
      Assert.assertTrue(-1 != b);

      // The Peer cache should start off empty.
      PeerCache cache = dfs.getClient().getClientContext().getPeerCache();
      Assert.assertEquals(0, cache.size());

      // Unbuffer should clear the block reader and return the socket to the
      // cache.
      stream.unbuffer();
      stream.seek(2);
      Assert.assertEquals(1, cache.size());
      int b2 = stream.read();
      Assert.assertEquals(b, b2);
    } finally {
      if (stream != null) {
        IOUtils.cleanup(null, stream);
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test opening many files via TCP (not short-circuit).
   *
   * This is practical when using unbuffer, because it reduces the number of
   * sockets and amount of memory that we use.
   */
  @Test
  public void testOpenManyFilesViaTcp() throws Exception {
    final int NUM_OPENS = 500;
    Configuration conf = new Configuration();
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, false);
    MiniDFSCluster cluster = null;
    FSDataInputStream[] streams = new FSDataInputStream[NUM_OPENS];
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      DistributedFileSystem dfs = cluster.getFileSystem();
      final Path TEST_PATH = new Path("/testFile");
      DFSTestUtil.createFile(dfs, TEST_PATH, 131072, (short)1, 1);

      for (int i = 0; i < NUM_OPENS; i++) {
        streams[i] = dfs.open(TEST_PATH);
        LOG.info("opening file " + i + "...");
        Assert.assertTrue(-1 != streams[i].read());
        streams[i].unbuffer();
      }
    } finally {
      for (FSDataInputStream stream : streams) {
        IOUtils.cleanup(null, stream);
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test that a InputStream should throw an exception when not implementing
   * CanUnbuffer
   *
   * This should throw an exception when the stream claims to have the
   * unbuffer capability, but actually does not implement CanUnbuffer.
   */
  @Test
  public void testUnbufferException() {
    abstract class BuggyStream
            extends FSInputStream
            implements StreamCapabilities {
    }

    BuggyStream bs = Mockito.mock(BuggyStream.class);
    Mockito.when(bs.hasCapability(Mockito.anyString())).thenReturn(true);

    exception.expect(UnsupportedOperationException.class);
    exception.expectMessage(
            StreamCapabilitiesPolicy.CAN_UNBUFFER_NOT_IMPLEMENTED_MESSAGE);

    FSDataInputStream fs = new FSDataInputStream(bs);
    fs.unbuffer();
  }
}
