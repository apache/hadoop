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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.hadoop.hdfs.StripedFileTestUtil.*;

public class TestReadStripedFileWithDecoding {
  static final Log LOG = LogFactory.getLog(TestReadStripedFileWithDecoding.class);

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;

  @Before
  public void setup() throws IOException {
    cluster = new MiniDFSCluster.Builder(new HdfsConfiguration())
        .numDataNodes(numDNs).build();
    cluster.getFileSystem().getClient().createErasureCodingZone("/",
        null, cellSize);
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testReadWithDNFailure1() throws IOException {
    testReadWithDNFailure("/foo", cellSize * (dataBlocks + 2), 0);
  }

  @Test
  public void testReadWithDNFailure2() throws IOException {
    testReadWithDNFailure("/foo", cellSize * (dataBlocks + 2), cellSize * 5);
  }

  @Test
  public void testReadWithDNFailure3() throws IOException {
    testReadWithDNFailure("/foo", cellSize * dataBlocks, 0);
  }

  /**
   * Delete a data block before reading. Verify the decoding works correctly.
   */
  @Test
  public void testReadCorruptedData() throws IOException {
    // create file
    final Path file = new Path("/partially_deleted");
    final int length = cellSize * dataBlocks * 2;
    final byte[] bytes = StripedFileTestUtil.generateBytes(length);
    DFSTestUtil.writeFile(fs, file, bytes);

    // corrupt the first data block
    // find the corresponding data node
    int dnIndex = findFirstDataNode(file, cellSize * dataBlocks);
    Assert.assertNotEquals(-1, dnIndex);
    // find the target block
    LocatedStripedBlock slb = (LocatedStripedBlock)fs.getClient()
        .getLocatedBlocks(file.toString(), 0, cellSize * dataBlocks).get(0);
    final LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroup(slb,
        cellSize, dataBlocks, parityBlocks);
    // find the target block file
    File storageDir = cluster.getInstanceStorageDir(dnIndex, 0);
    File blkFile = MiniDFSCluster.getBlockFile(storageDir, blks[0].getBlock());
    Assert.assertTrue("Block file does not exist", blkFile.exists());
    // delete the block file
    LOG.info("Deliberately removing file " + blkFile.getName());
    Assert.assertTrue("Cannot remove file", blkFile.delete());
    verifyRead(file, length, bytes);
  }

  /**
   * Corrupt the content of the data block before reading.
   */
  @Test
  public void testReadCorruptedData2() throws IOException {
    // create file
    final Path file = new Path("/partially_corrupted");
    final int length = cellSize * dataBlocks * 2;
    final byte[] bytes = StripedFileTestUtil.generateBytes(length);
    DFSTestUtil.writeFile(fs, file, bytes);

    // corrupt the first data block
    // find the first data node
    int dnIndex = findFirstDataNode(file, cellSize * dataBlocks);
    Assert.assertNotEquals(-1, dnIndex);
    // find the first data block
    LocatedStripedBlock slb = (LocatedStripedBlock)fs.getClient()
        .getLocatedBlocks(file.toString(), 0, cellSize * dataBlocks).get(0);
    final LocatedBlock[] blks = StripedBlockUtil.parseStripedBlockGroup(slb,
        cellSize, dataBlocks, parityBlocks);
    // find the first block file
    File storageDir = cluster.getInstanceStorageDir(dnIndex, 0);
    File blkFile = MiniDFSCluster.getBlockFile(storageDir, blks[0].getBlock());
    Assert.assertTrue("Block file does not exist", blkFile.exists());
    // corrupt the block file
    LOG.info("Deliberately corrupting file " + blkFile.getName());
    try (FileOutputStream out = new FileOutputStream(blkFile)) {
      out.write("corruption".getBytes());
    }

    verifyRead(file, length, bytes);
  }

  private int findFirstDataNode(Path file, long length) throws IOException {
    BlockLocation[] locs = fs.getFileBlockLocations(file, 0, length);
    String name = (locs[0].getNames())[0];
    int dnIndex = 0;
    for (DataNode dn : cluster.getDataNodes()) {
      int port = dn.getXferPort();
      if (name.contains(Integer.toString(port))) {
        return dnIndex;
      }
      dnIndex++;
    }
    return -1;
  }

  private void verifyRead(Path file, int length, byte[] expected)
      throws IOException {
    // pread
    try (FSDataInputStream fsdis = fs.open(file)) {
      byte[] buf = new byte[length];
      int readLen = fsdis.read(0, buf, 0, buf.length);
      Assert.assertEquals("The fileSize of file should be the same to write size",
          length, readLen);
      Assert.assertArrayEquals(expected, buf);
    }

    // stateful read
    ByteBuffer result = ByteBuffer.allocate(length);
    ByteBuffer buf = ByteBuffer.allocate(1024);
    int readLen = 0;
    int ret;
    try (FSDataInputStream in = fs.open(file)) {
      while ((ret = in.read(buf)) >= 0) {
        readLen += ret;
        buf.flip();
        result.put(buf);
        buf.clear();
      }
    }
    Assert.assertEquals("The length of file should be the same to write size",
        length, readLen);
    Assert.assertArrayEquals(expected, result.array());
  }

  private void testReadWithDNFailure(String file, int fileSize,
      int startOffsetInFile) throws IOException {
    final int failedDNIdx = 2;
    Path testPath = new Path(file);
    final byte[] bytes = StripedFileTestUtil.generateBytes(fileSize);
    DFSTestUtil.writeFile(fs, testPath, bytes);

    // shut down the DN that holds an internal data block
    BlockLocation[] locs = fs.getFileBlockLocations(testPath, cellSize * 5,
        cellSize);
    String name = (locs[0].getNames())[failedDNIdx];
    for (DataNode dn : cluster.getDataNodes()) {
      int port = dn.getXferPort();
      if (name.contains(Integer.toString(port))) {
        dn.shutdown();
        break;
      }
    }

    // pread
    try (FSDataInputStream fsdis = fs.open(testPath)) {
      byte[] buf = new byte[fileSize];
      int readLen = fsdis.read(startOffsetInFile, buf, 0, buf.length);
      Assert.assertEquals("The fileSize of file should be the same to write size",
          fileSize - startOffsetInFile, readLen);

      byte[] expected = new byte[readLen];
      System.arraycopy(bytes, startOffsetInFile, expected, 0,
          fileSize - startOffsetInFile);

      for (int i = startOffsetInFile; i < fileSize; i++) {
        Assert.assertEquals("Byte at " + i + " should be the same",
            expected[i - startOffsetInFile], buf[i - startOffsetInFile]);
      }
    }

    // stateful read
    ByteBuffer result = ByteBuffer.allocate(fileSize);
    ByteBuffer buf = ByteBuffer.allocate(1024);
    int readLen = 0;
    int ret;
    try (FSDataInputStream in = fs.open(testPath)) {
      while ((ret = in.read(buf)) >= 0) {
        readLen += ret;
        buf.flip();
        result.put(buf);
        buf.clear();
      }
    }
    Assert.assertEquals("The length of file should be the same to write size",
        fileSize, readLen);
    Assert.assertArrayEquals(bytes, result.array());
  }
}
