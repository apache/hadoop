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
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.security.UserGroupInformation;
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
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL, true);
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY,
        false);
    conf.set(DFSConfigKeys.DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY,
        UserGroupInformation.getCurrentUser().getShortUserName());
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC, false);
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
}
