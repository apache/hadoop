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
package org.apache.hadoop.hdfs.client.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBlockReaderLocalLegacy {
  @BeforeClass
  public static void setupCluster() throws IOException {
    DFSInputStream.tcpReadsDisabledForTesting = true;
    DomainSocket.disableBindPathValidation();
  }

  private static HdfsConfiguration getConfiguration(
      TemporarySocketDirectory socketDir) throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    if (socketDir == null) {
      conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY, "");
    } else {
      conf.set(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
        new File(socketDir.getDir(), "TestBlockReaderLocalLegacy.%d.sock").
          getAbsolutePath());
    }
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY, true);
    conf.setBoolean(HdfsClientConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL, true);
    conf.setBoolean(HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY,
        false);
    conf.set(DFSConfigKeys.DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY,
        UserGroupInformation.getCurrentUser().getShortUserName());
    conf.setBoolean(HdfsClientConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC, false);
    // Set short retry timeouts so this test runs faster
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    return conf;
  }

  /**
   * Test that, in the case of an error, the position and limit of a ByteBuffer
   * are left unchanged. This is not mandated by ByteBufferReadable, but clients
   * of this class might immediately issue a retry on failure, so it's polite.
   */
  @Test
  public void testStablePositionAfterCorruptRead() throws Exception {
    final short REPL_FACTOR = 1;
    final long FILE_LENGTH = 512L;

    HdfsConfiguration conf = getConfiguration(null);
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();

    Path path = new Path("/corrupted");

    DFSTestUtil.createFile(fs, path, FILE_LENGTH, REPL_FACTOR, 12345L);
    DFSTestUtil.waitReplication(fs, path, REPL_FACTOR);

    ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, path);
    int blockFilesCorrupted = cluster.corruptBlockOnDataNodes(block);
    assertEquals("All replicas not corrupted", REPL_FACTOR, blockFilesCorrupted);

    FSDataInputStream dis = cluster.getFileSystem().open(path);
    ByteBuffer buf = ByteBuffer.allocateDirect((int)FILE_LENGTH);
    boolean sawException = false;
    try {
      dis.read(buf);
    } catch (ChecksumException ex) {
      sawException = true;
    }

    assertTrue(sawException);
    assertEquals(0, buf.position());
    assertEquals(buf.capacity(), buf.limit());

    dis = cluster.getFileSystem().open(path);
    buf.position(3);
    buf.limit(25);
    sawException = false;
    try {
      dis.read(buf);
    } catch (ChecksumException ex) {
      sawException = true;
    }

    assertTrue(sawException);
    assertEquals(3, buf.position());
    assertEquals(25, buf.limit());
    cluster.shutdown();
  }

  @Test
  public void testBothOldAndNewShortCircuitConfigured() throws Exception {
    final short REPL_FACTOR = 1;
    final int FILE_LENGTH = 512;
    Assume.assumeTrue(null == DomainSocket.getLoadingFailureReason());
    TemporarySocketDirectory socketDir = new TemporarySocketDirectory();
    HdfsConfiguration conf = getConfiguration(socketDir);
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    socketDir.close();
    FileSystem fs = cluster.getFileSystem();

    Path path = new Path("/foo");
    byte orig[] = new byte[FILE_LENGTH];
    for (int i = 0; i < orig.length; i++) {
      orig[i] = (byte)(i%10);
    }
    FSDataOutputStream fos = fs.create(path, (short)1);
    fos.write(orig);
    fos.close();
    DFSTestUtil.waitReplication(fs, path, REPL_FACTOR);
    FSDataInputStream fis = cluster.getFileSystem().open(path);
    byte buf[] = new byte[FILE_LENGTH];
    IOUtils.readFully(fis, buf, 0, FILE_LENGTH);
    fis.close();
    Assert.assertArrayEquals(orig, buf);
    Arrays.equals(orig, buf);
    cluster.shutdown();
  }

  @Test(timeout=20000)
  public void testBlockReaderLocalLegacyWithAppend() throws Exception {
    final short REPL_FACTOR = 1;
    final HdfsConfiguration conf = getConfiguration(null);
    conf.setBoolean(HdfsClientConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL, true);

    final MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();

    final DistributedFileSystem dfs = cluster.getFileSystem();
    final Path path = new Path("/testBlockReaderLocalLegacy");
    DFSTestUtil.createFile(dfs, path, 10, REPL_FACTOR, 0);
    DFSTestUtil.waitReplication(dfs, path, REPL_FACTOR);

    final ClientDatanodeProtocol proxy;
    final Token<BlockTokenIdentifier> token;
    final ExtendedBlock originalBlock;
    final long originalGS;
    {
      final LocatedBlock lb = cluster.getNameNode().getRpcServer()
          .getBlockLocations(path.toString(), 0, 1).get(0);
      proxy = DFSUtilClient.createClientDatanodeProtocolProxy(
          lb.getLocations()[0], conf, 60000, false);
      token = lb.getBlockToken();

      // get block and generation stamp
      final ExtendedBlock blk = new ExtendedBlock(lb.getBlock());
      originalBlock = new ExtendedBlock(blk);
      originalGS = originalBlock.getGenerationStamp();

      // test getBlockLocalPathInfo
      final BlockLocalPathInfo info = proxy.getBlockLocalPathInfo(blk, token);
      Assert.assertEquals(originalGS, info.getBlock().getGenerationStamp());
    }

    { // append one byte
      FSDataOutputStream out = dfs.append(path);
      out.write(1);
      out.close();
    }

    {
      // get new generation stamp
      final LocatedBlock lb = cluster.getNameNode().getRpcServer()
          .getBlockLocations(path.toString(), 0, 1).get(0);
      final long newGS = lb.getBlock().getGenerationStamp();
      Assert.assertTrue(newGS > originalGS);

      // getBlockLocalPathInfo using the original block.
      Assert.assertEquals(originalGS, originalBlock.getGenerationStamp());
      final BlockLocalPathInfo info = proxy.getBlockLocalPathInfo(
          originalBlock, token);
      Assert.assertEquals(newGS, info.getBlock().getGenerationStamp());
    }
    cluster.shutdown();
  }
}
