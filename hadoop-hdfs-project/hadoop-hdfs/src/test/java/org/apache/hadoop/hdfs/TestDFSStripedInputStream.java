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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TestDFSStripedInputStream {
  private static int dataBlocks = HdfsConstants.NUM_DATA_BLOCKS;
  private static int parityBlocks = HdfsConstants.NUM_PARITY_BLOCKS;


  private static DistributedFileSystem fs;
  private final static int cellSize = HdfsConstants.BLOCK_STRIPED_CELL_SIZE;
  private final static int stripesPerBlock = 4;
  static int blockSize = cellSize * stripesPerBlock;
  static int numDNs = dataBlocks + parityBlocks + 2;

  private static MiniDFSCluster cluster;

  @BeforeClass
  public static void setup() throws IOException {
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.getFileSystem().getClient().createErasureCodingZone("/", null);
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testFileEmpty() throws IOException {
    testOneFileUsingDFSStripedInputStream("/EmptyFile", 0);
  }

  @Test
  public void testFileSmallerThanOneCell1() throws IOException {
    testOneFileUsingDFSStripedInputStream("/SmallerThanOneCell", 1);
  }

  @Test
  public void testFileSmallerThanOneCell2() throws IOException {
    testOneFileUsingDFSStripedInputStream("/SmallerThanOneCell", cellSize - 1);
  }

  @Test
  public void testFileEqualsWithOneCell() throws IOException {
    testOneFileUsingDFSStripedInputStream("/EqualsWithOneCell", cellSize);
  }

  @Test
  public void testFileSmallerThanOneStripe1() throws IOException {
    testOneFileUsingDFSStripedInputStream("/SmallerThanOneStripe",
        cellSize * dataBlocks - 1);
  }

  @Test
  public void testFileSmallerThanOneStripe2() throws IOException {
    testOneFileUsingDFSStripedInputStream("/SmallerThanOneStripe",
        cellSize + 123);
  }

  @Test
  public void testFileEqualsWithOneStripe() throws IOException {
    testOneFileUsingDFSStripedInputStream("/EqualsWithOneStripe",
        cellSize * dataBlocks);
  }

  @Test
  public void testFileMoreThanOneStripe1() throws IOException {
    testOneFileUsingDFSStripedInputStream("/MoreThanOneStripe1",
        cellSize * dataBlocks + 123);
  }

  @Test
  public void testFileMoreThanOneStripe2() throws IOException {
    testOneFileUsingDFSStripedInputStream("/MoreThanOneStripe2",
        cellSize * dataBlocks + cellSize * dataBlocks + 123);
  }

  @Test
  public void testLessThanFullBlockGroup() throws IOException {
    testOneFileUsingDFSStripedInputStream("/LessThanFullBlockGroup",
        cellSize * dataBlocks * (stripesPerBlock - 1) + cellSize);
  }

  @Test
  public void testFileFullBlockGroup() throws IOException {
    testOneFileUsingDFSStripedInputStream("/FullBlockGroup",
        blockSize * dataBlocks);
  }

  @Test
  public void testFileMoreThanABlockGroup1() throws IOException {
    testOneFileUsingDFSStripedInputStream("/MoreThanABlockGroup1",
        blockSize * dataBlocks + 123);
  }

  @Test
  public void testFileMoreThanABlockGroup2() throws IOException {
    testOneFileUsingDFSStripedInputStream("/MoreThanABlockGroup2",
        blockSize * dataBlocks + cellSize+ 123);
  }


  @Test
  public void testFileMoreThanABlockGroup3() throws IOException {
    testOneFileUsingDFSStripedInputStream("/MoreThanABlockGroup3",
        blockSize * dataBlocks * 3 + cellSize * dataBlocks
            + cellSize + 123);
  }

  private byte[] generateBytes(int cnt) {
    byte[] bytes = new byte[cnt];
    for (int i = 0; i < cnt; i++) {
      bytes[i] = getByte(i);
    }
    return bytes;
  }

  private byte getByte(long pos) {
    final int mod = 29;
    return (byte) (pos % mod + 1);
  }

  private void testOneFileUsingDFSStripedInputStream(String src, int writeBytes)
      throws IOException {
    Path testPath = new Path(src);
    byte[] bytes = generateBytes(writeBytes);
    DFSTestUtil.writeFile(fs, testPath, new String(bytes));

    //check file length
    FileStatus status = fs.getFileStatus(testPath);
    long fileLength = status.getLen();
    Assert.assertEquals("File length should be the same",
        writeBytes, fileLength);

    // pread
    try (FSDataInputStream fsdis = fs.open(new Path(src))) {
      byte[] buf = new byte[writeBytes + 100];
      int readLen = fsdis.read(0, buf, 0, buf.length);
      readLen = readLen >= 0 ? readLen : 0;
      Assert.assertEquals("The length of file should be the same to write size",
          writeBytes, readLen);
      for (int i = 0; i < writeBytes; i++) {
        Assert.assertEquals("Byte at i should be the same", getByte(i), buf[i]);
      }
    }

    // stateful read with byte array
    try (FSDataInputStream fsdis = fs.open(new Path(src))) {
      byte[] buf = new byte[writeBytes + 100];
      int readLen = 0;
      int ret;
      do {
        ret = fsdis.read(buf, readLen, buf.length - readLen);
        if (ret > 0) {
          readLen += ret;
        }
      } while (ret >= 0);

      readLen = readLen >= 0 ? readLen : 0;
      Assert.assertEquals("The length of file should be the same to write size",
          writeBytes, readLen);
      for (int i = 0; i < writeBytes; i++) {
        Assert.assertEquals("Byte at i should be the same", getByte(i), buf[i]);
      }
    }

    // stateful read with ByteBuffer
    try (FSDataInputStream fsdis = fs.open(new Path(src))) {
      ByteBuffer buf = ByteBuffer.allocate(writeBytes + 100);
      int readLen = 0;
      int ret;
      do {
        ret = fsdis.read(buf);
        if (ret > 0) {
          readLen += ret;
        }
      } while (ret >= 0);
      readLen = readLen >= 0 ? readLen : 0;
      Assert.assertEquals("The length of file should be the same to write size",
          writeBytes, readLen);
      for (int i = 0; i < writeBytes; i++) {
        Assert.assertEquals("Byte at i should be the same", getByte(i), buf.array()[i]);
      }
    }
  }
}
