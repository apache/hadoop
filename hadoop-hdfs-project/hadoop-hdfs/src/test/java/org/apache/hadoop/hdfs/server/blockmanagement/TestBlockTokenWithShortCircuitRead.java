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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.SecurityTestUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.shortcircuit.DfsClientShm;
import org.apache.hadoop.hdfs.shortcircuit.DfsClientShmManager.PerDatanodeVisitorInfo;
import org.apache.hadoop.hdfs.shortcircuit.DfsClientShmManager.Visitor;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitCache;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.Slot;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestBlockTokenWithShortCircuitRead {

  private static final int BLOCK_SIZE = 1024;
  private static final int FILE_SIZE = 2 * BLOCK_SIZE;
  private static final String FILE_TO_SHORT_CIRCUIT_READ = "/fileToSSR.dat";

  static {
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.TRACE);
  }

  private void readFile(FSDataInputStream in) throws IOException {
    byte[] toRead = new byte[FILE_SIZE];
    int totalRead = 0;
    int nRead;
    while ((nRead = in.read(toRead, totalRead,
      toRead.length - totalRead)) > 0) {
      totalRead += nRead;
    }
    assertEquals("Cannot read file.", toRead.length, totalRead);
  }

  @Test
  public void testShortCircuitReadWithInvalidToken() throws Exception {
    MiniDFSCluster cluster = null;
    short numDataNodes = 1;
    Configuration conf = new Configuration();
    conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setLong(DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFS_BYTES_PER_CHECKSUM_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, numDataNodes);
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    // Set short retry timeouts so this test runs faster
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    TemporarySocketDirectory sockDir = new TemporarySocketDirectory();
    conf.set(DFS_DOMAIN_SOCKET_PATH_KEY, new File(sockDir.getDir(),
        "testShortCircuitReadWithInvalidToken").getAbsolutePath());
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    // avoid caching
    conf.setInt(HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_SIZE_KEY, 0);
    DomainSocket.disableBindPathValidation();

    try {
      cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDataNodes).format(true).build();
      cluster.waitActive();

      final NameNode nn = cluster.getNameNode();
      final NamenodeProtocols nnProto = nn.getRpcServer();
      final BlockManager bm = nn.getNamesystem().getBlockManager();
      final BlockTokenSecretManager sm = bm.getBlockTokenSecretManager();
      // set a short token lifetime (1 second) initially
      SecurityTestUtil.setBlockTokenLifetime(sm, 1000L);

      Path fileToRead = new Path(FILE_TO_SHORT_CIRCUIT_READ);
      DistributedFileSystem fs = cluster.getFileSystem();

      final ShortCircuitCache cache =
          fs.getClient().getClientContext().getShortCircuitCache();
      final DatanodeInfo datanode =
          new DatanodeInfo.DatanodeInfoBuilder()
          .setNodeID(cluster.getDataNodes().get(0).getDatanodeId())
          .build();

      cache.getDfsClientShmManager().visit(new Visitor() {
        @Override
        public void visit(HashMap<DatanodeInfo, PerDatanodeVisitorInfo> info) {
          // The ClientShmManager starts off empty
          Assert.assertEquals(0, info.size());
        }
      });

      // create file to read
      DFSTestUtil.createFile(fs, fileToRead, FILE_SIZE, numDataNodes, 0);

      try(FSDataInputStream in = fs.open(fileToRead)) {
        // acquire access token
        readFile(in);

        // verify token is not expired
        List<LocatedBlock> locatedBlocks = nnProto.getBlockLocations(
            FILE_TO_SHORT_CIRCUIT_READ, 0, FILE_SIZE).getLocatedBlocks();
        LocatedBlock lblock = locatedBlocks.get(0); // first block
        Token<BlockTokenIdentifier> myToken = lblock.getBlockToken();
        assertFalse(SecurityTestUtil.isBlockTokenExpired(myToken));

        // check the number of slot objects
        checkSlotsAfterSSRWithTokenExpiration(cache, datanode, in, myToken);

        // check once more. the number of slot objects should not be changed
        checkSlotsAfterSSRWithTokenExpiration(cache, datanode, in, myToken);
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      sockDir.close();
    }
  }

  private void checkSlotsAfterSSRWithTokenExpiration(
      ShortCircuitCache cache, DatanodeInfo datanode, FSDataInputStream in,
      Token<BlockTokenIdentifier> myToken) throws IOException {
    // wait token expiration
    while (!SecurityTestUtil.isBlockTokenExpired(myToken)) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException ignored) {
      }
    }

    // short circuit read after token expiration
    in.seek(0);
    readFile(in);

    checkShmAndSlots(cache, datanode, 1);
  }

  private void checkShmAndSlots(ShortCircuitCache cache,
      final DatanodeInfo datanode,
      final int expectedSlotCnt) throws IOException {
    cache.getDfsClientShmManager().visit(new Visitor() {
      @Override
      public void visit(HashMap<DatanodeInfo, PerDatanodeVisitorInfo> info) {
        Assert.assertEquals(1, info.size());
        PerDatanodeVisitorInfo vinfo = info.get(datanode);
        Assert.assertFalse(vinfo.disabled);
        Assert.assertEquals(0, vinfo.full.size());
        Assert.assertEquals(1, vinfo.notFull.size());

        int slotCnt = 0;
        DfsClientShm shm = vinfo.notFull.values().iterator().next();
        for (Iterator<Slot> iter = shm.slotIterator(); iter.hasNext();) {
          iter.next();
          slotCnt++;
        }
        Assert.assertEquals(expectedSlotCnt, slotCnt);
      }
    });
  }
}
