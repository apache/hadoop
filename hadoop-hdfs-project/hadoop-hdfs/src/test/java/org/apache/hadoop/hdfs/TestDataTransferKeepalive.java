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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CONTEXT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Supplier;

public class TestDataTransferKeepalive {
  final Configuration conf = new HdfsConfiguration();
  private MiniDFSCluster cluster;
  private DataNode dn;
  private static final Path TEST_FILE = new Path("/test");
  
  private static final int KEEPALIVE_TIMEOUT = 1000;
  private static final int WRITE_TIMEOUT = 3000;
  
  @Before
  public void setup() throws Exception {
    conf.setInt(DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_KEY,
        KEEPALIVE_TIMEOUT);
    conf.setInt(DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY,
        0);
    
    cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(1).build();
    dn = cluster.getDataNodes().get(0);
  }
  
  @After
  public void teardown() {
    cluster.shutdown();
  }
  
  /**
   * Regression test for HDFS-3357. Check that the datanode is respecting
   * its configured keepalive timeout.
   */
  @Test(timeout=30000)
  public void testDatanodeRespectsKeepAliveTimeout() throws Exception {
    Configuration clientConf = new Configuration(conf);
    // Set a client socket cache expiry time much longer than 
    // the datanode-side expiration time.
    final long CLIENT_EXPIRY_MS = 60000L;
    clientConf.setLong(DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY, CLIENT_EXPIRY_MS);
    clientConf.set(DFS_CLIENT_CONTEXT, "testDatanodeRespectsKeepAliveTimeout");
    DistributedFileSystem fs =
        (DistributedFileSystem)FileSystem.get(cluster.getURI(),
            clientConf);
    PeerCache peerCache = ClientContext.getFromConf(clientConf).getPeerCache();

    DFSTestUtil.createFile(fs, TEST_FILE, 1L, (short)1, 0L);

    // Clients that write aren't currently re-used.
    assertEquals(0, peerCache.size());
    assertXceiverCount(0);

    // Reads the file, so we should get a
    // cached socket, and should have an xceiver on the other side.
    DFSTestUtil.readFile(fs, TEST_FILE);
    assertEquals(1, peerCache.size());
    assertXceiverCount(1);

    // Sleep for a bit longer than the keepalive timeout
    // and make sure the xceiver died.
    Thread.sleep(DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_DEFAULT + 50);
    assertXceiverCount(0);
    
    // The socket is still in the cache, because we don't
    // notice that it's closed until we try to read
    // from it again.
    assertEquals(1, peerCache.size());
    
    // Take it out of the cache - reading should
    // give an EOF.
    Peer peer = peerCache.get(dn.getDatanodeId(), false);
    assertNotNull(peer);
    assertEquals(-1, peer.getInputStream().read());
  }

  /**
   * Test that the client respects its keepalive timeout.
   */
  @Test(timeout=30000)
  public void testClientResponsesKeepAliveTimeout() throws Exception {
    Configuration clientConf = new Configuration(conf);
    // Set a client socket cache expiry time much shorter than 
    // the datanode-side expiration time.
    final long CLIENT_EXPIRY_MS = 10L;
    clientConf.setLong(DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY, CLIENT_EXPIRY_MS);
    clientConf.set(DFS_CLIENT_CONTEXT, "testClientResponsesKeepAliveTimeout");
    DistributedFileSystem fs =
        (DistributedFileSystem)FileSystem.get(cluster.getURI(),
            clientConf);
    PeerCache peerCache = ClientContext.getFromConf(clientConf).getPeerCache();

    DFSTestUtil.createFile(fs, TEST_FILE, 1L, (short)1, 0L);

    // Clients that write aren't currently re-used.
    assertEquals(0, peerCache.size());
    assertXceiverCount(0);

    // Reads the file, so we should get a
    // cached socket, and should have an xceiver on the other side.
    DFSTestUtil.readFile(fs, TEST_FILE);
    assertEquals(1, peerCache.size());
    assertXceiverCount(1);

    // Sleep for a bit longer than the client keepalive timeout.
    Thread.sleep(CLIENT_EXPIRY_MS + 50);
    
    // Taking out a peer which is expired should give a null.
    Peer peer = peerCache.get(dn.getDatanodeId(), false);
    assertTrue(peer == null);

    // The socket cache is now empty.
    assertEquals(0, peerCache.size());
  }

  /**
   * Test for the case where the client beings to read a long block, but doesn't
   * read bytes off the stream quickly. The datanode should time out sending the
   * chunks and the transceiver should die, even if it has a long keepalive.
   */
  @Test(timeout=300000)
  public void testSlowReader() throws Exception {
    // Set a client socket cache expiry time much longer than 
    // the datanode-side expiration time.
    final long CLIENT_EXPIRY_MS = 600000L;
    Configuration clientConf = new Configuration(conf);
    clientConf.setLong(DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY, CLIENT_EXPIRY_MS);
    clientConf.set(DFS_CLIENT_CONTEXT, "testSlowReader");
    DistributedFileSystem fs =
        (DistributedFileSystem)FileSystem.get(cluster.getURI(),
            clientConf);
    // Restart the DN with a shorter write timeout.
    DataNodeProperties props = cluster.stopDataNode(0);
    props.conf.setInt(DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY,
        WRITE_TIMEOUT);
    props.conf.setInt(DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_KEY,
        120000);
    assertTrue(cluster.restartDataNode(props, true));
    dn = cluster.getDataNodes().get(0);
    // Wait for heartbeats to avoid a startup race where we
    // try to write the block while the DN is still starting.
    cluster.triggerHeartbeats();
    
    DFSTestUtil.createFile(fs, TEST_FILE, 1024*1024*8L, (short)1, 0L);
    FSDataInputStream stm = fs.open(TEST_FILE);
    stm.read();
    assertXceiverCount(1);

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      public Boolean get() {
        // DN should time out in sendChunks, and this should force
        // the xceiver to exit.
        return getXceiverCountWithoutServer() == 0;
      }
    }, 500, 50000);

    IOUtils.closeStream(stm);
  }
  
  @Test(timeout=30000)
  public void testManyClosedSocketsInCache() throws Exception {
    // Make a small file
    Configuration clientConf = new Configuration(conf);
    clientConf.set(DFS_CLIENT_CONTEXT, "testManyClosedSocketsInCache");
    DistributedFileSystem fs =
        (DistributedFileSystem)FileSystem.get(cluster.getURI(),
            clientConf);
    PeerCache peerCache = ClientContext.getFromConf(clientConf).getPeerCache();
    DFSTestUtil.createFile(fs, TEST_FILE, 1L, (short)1, 0L);

    // Insert a bunch of dead sockets in the cache, by opening
    // many streams concurrently, reading all of the data,
    // and then closing them.
    InputStream[] stms = new InputStream[5];
    try {
      for (int i = 0; i < stms.length; i++) {
        stms[i] = fs.open(TEST_FILE);
      }
      for (InputStream stm : stms) {
        IOUtils.copyBytes(stm, new IOUtils.NullOutputStream(), 1024);
      }
    } finally {
      IOUtils.cleanup(null, stms);
    }
    
    assertEquals(5, peerCache.size());
    
    // Let all the xceivers timeout
    Thread.sleep(1500);
    assertXceiverCount(0);

    // Client side still has the sockets cached
    assertEquals(5, peerCache.size());

    // Reading should not throw an exception.
    DFSTestUtil.readFile(fs, TEST_FILE);
  }

  private void assertXceiverCount(int expected) {
    int count = getXceiverCountWithoutServer();
    if (count != expected) {
      ReflectionUtils.printThreadInfo(System.err, "Thread dumps");
      fail("Expected " + expected + " xceivers, found " +
          count);
    }
  }

  /**
   * Returns the datanode's xceiver count, but subtracts 1, since the
   * DataXceiverServer counts as one.
   * 
   * @return int xceiver count, not including DataXceiverServer
   */
  private int getXceiverCountWithoutServer() {
    return dn.getXceiverCount() - 1;
  }
}
