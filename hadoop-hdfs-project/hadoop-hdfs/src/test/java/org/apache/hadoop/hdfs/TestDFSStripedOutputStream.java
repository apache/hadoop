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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDFSStripedOutputStream {
  public static final Log LOG = LogFactory.getLog(
      TestDFSStripedOutputStream.class);

  static {
    GenericTestUtils.setLogLevel(DFSOutputStream.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(DataStreamer.LOG, Level.ALL);
  }

  private int dataBlocks = HdfsConstants.NUM_DATA_BLOCKS;
  private int parityBlocks = HdfsConstants.NUM_PARITY_BLOCKS;

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private Configuration conf;
  private final int cellSize = HdfsConstants.BLOCK_STRIPED_CELL_SIZE;
  private final int stripesPerBlock = 4;
  private final int blockSize = cellSize * stripesPerBlock;

  @Before
  public void setup() throws IOException {
    int numDNs = dataBlocks + parityBlocks + 2;
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.getFileSystem().getClient().createErasureCodingZone("/", null);
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testFileEmpty() throws Exception {
    testOneFile("/EmptyFile", 0);
  }

  @Test
  public void testFileSmallerThanOneCell1() throws Exception {
    testOneFile("/SmallerThanOneCell", 1);
  }

  @Test
  public void testFileSmallerThanOneCell2() throws Exception {
    testOneFile("/SmallerThanOneCell", cellSize - 1);
  }

  @Test
  public void testFileEqualsWithOneCell() throws Exception {
    testOneFile("/EqualsWithOneCell", cellSize);
  }

  @Test
  public void testFileSmallerThanOneStripe1() throws Exception {
    testOneFile("/SmallerThanOneStripe", cellSize * dataBlocks - 1);
  }

  @Test
  public void testFileSmallerThanOneStripe2() throws Exception {
    testOneFile("/SmallerThanOneStripe", cellSize + 123);
  }

  @Test
  public void testFileEqualsWithOneStripe() throws Exception {
    testOneFile("/EqualsWithOneStripe", cellSize * dataBlocks);
  }

  @Test
  public void testFileMoreThanOneStripe1() throws Exception {
    testOneFile("/MoreThanOneStripe1", cellSize * dataBlocks + 123);
  }

  @Test
  public void testFileMoreThanOneStripe2() throws Exception {
    testOneFile("/MoreThanOneStripe2", cellSize * dataBlocks
            + cellSize * dataBlocks + 123);
  }

  @Test
  public void testFileLessThanFullBlockGroup() throws Exception {
    testOneFile("/LessThanFullBlockGroup",
        cellSize * dataBlocks * (stripesPerBlock - 1) + cellSize);
  }

  @Test
  public void testFileFullBlockGroup() throws Exception {
    testOneFile("/FullBlockGroup", blockSize * dataBlocks);
  }

  @Test
  public void testFileMoreThanABlockGroup1() throws Exception {
    testOneFile("/MoreThanABlockGroup1", blockSize * dataBlocks + 123);
  }

  @Test
  public void testFileMoreThanABlockGroup2() throws Exception {
    testOneFile("/MoreThanABlockGroup2",
        blockSize * dataBlocks + cellSize+ 123);
  }


  @Test
  public void testFileMoreThanABlockGroup3() throws Exception {
    testOneFile("/MoreThanABlockGroup3",
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
    int mod = 29;
    return (byte) (pos % mod + 1);
  }

  private void testOneFile(String src, int writeBytes) throws Exception {
    src += "_" + writeBytes;
    Path testPath = new Path(src);

    byte[] bytes = generateBytes(writeBytes);
    DFSTestUtil.writeFile(fs, testPath, new String(bytes));
    StripedFileTestUtil.waitBlockGroupsReported(fs, src);

    // check file length
    FileStatus status = fs.getFileStatus(testPath);
    Assert.assertEquals(writeBytes, status.getLen());

    checkData(src, writeBytes);
  }

  void checkData(String src, int writeBytes) throws IOException {
    List<List<LocatedBlock>> blockGroupList = new ArrayList<>();
    LocatedBlocks lbs = fs.getClient().getLocatedBlocks(src, 0L);

    for (LocatedBlock firstBlock : lbs.getLocatedBlocks()) {
      Assert.assertTrue(firstBlock instanceof LocatedStripedBlock);
      LocatedBlock[] blocks = StripedBlockUtil.
          parseStripedBlockGroup((LocatedStripedBlock) firstBlock,
              cellSize, dataBlocks, parityBlocks);
      List<LocatedBlock> oneGroup = Arrays.asList(blocks);
      blockGroupList.add(oneGroup);
    }

    // test each block group
    for (int group = 0; group < blockGroupList.size(); group++) {
      //get the data of this block
      List<LocatedBlock> blockList = blockGroupList.get(group);
      byte[][] dataBlockBytes = new byte[dataBlocks][];
      byte[][] parityBlockBytes = new byte[parityBlocks][];

      // for each block, use BlockReader to read data
      for (int i = 0; i < blockList.size(); i++) {
        LocatedBlock lblock = blockList.get(i);
        if (lblock == null) {
          continue;
        }
        ExtendedBlock block = lblock.getBlock();
        byte[] blockBytes = new byte[(int)block.getNumBytes()];
        if (i < dataBlocks) {
          dataBlockBytes[i] = blockBytes;
        } else {
          parityBlockBytes[i - dataBlocks] = blockBytes;
        }

        if (block.getNumBytes() == 0) {
          continue;
        }

        final BlockReader blockReader = BlockReaderTestUtil.getBlockReader(
            fs, lblock, 0, block.getNumBytes());
        blockReader.readAll(blockBytes, 0, (int) block.getNumBytes());
        blockReader.close();
      }

      // check if we write the data correctly
      for (int blkIdxInGroup = 0; blkIdxInGroup < dataBlockBytes.length;
           blkIdxInGroup++) {
        final byte[] actualBlkBytes = dataBlockBytes[blkIdxInGroup];
        if (actualBlkBytes == null) {
          continue;
        }
        for (int posInBlk = 0; posInBlk < actualBlkBytes.length; posInBlk++) {
          // calculate the position of this byte in the file
          long posInFile = StripedBlockUtil.offsetInBlkToOffsetInBG(cellSize,
              dataBlocks, posInBlk, blkIdxInGroup) +
              group * blockSize * dataBlocks;
          Assert.assertTrue(posInFile < writeBytes);
          final byte expected = getByte(posInFile);

          String s = "Unexpected byte " + actualBlkBytes[posInBlk]
              + ", expect " + expected
              + ". Block group index is " + group
              + ", stripe index is " + posInBlk / cellSize
              + ", cell index is " + blkIdxInGroup
              + ", byte index is " + posInBlk % cellSize;
          Assert.assertEquals(s, expected, actualBlkBytes[posInBlk]);
        }
      }

      verifyParity(lbs.getLocatedBlocks().get(group).getBlockSize(),
          cellSize, dataBlockBytes, parityBlockBytes);
    }
  }

  void verifyParity(final long size, final int cellSize,
      byte[][] dataBytes, byte[][] parityBytes) {
    verifyParity(conf, size, cellSize, dataBytes, parityBytes, -1);
  }

  static void verifyParity(Configuration conf, final long size,
                           final int cellSize, byte[][] dataBytes,
                           byte[][] parityBytes, int killedDnIndex) {
    // verify the parity blocks
    int parityBlkSize = (int) StripedBlockUtil.getInternalBlockLength(
        size, cellSize, dataBytes.length, dataBytes.length);
    final byte[][] expectedParityBytes = new byte[parityBytes.length][];
    for (int i = 0; i < parityBytes.length; i++) {
      expectedParityBytes[i] = new byte[parityBlkSize];
    }
    for (int i = 0; i < dataBytes.length; i++) {
      if (dataBytes[i] == null) {
        dataBytes[i] = new byte[dataBytes[0].length];
      } else if (dataBytes[i].length < dataBytes[0].length) {
        final byte[] tmp = dataBytes[i];
        dataBytes[i] = new byte[dataBytes[0].length];
        System.arraycopy(tmp, 0, dataBytes[i], 0, tmp.length);
      }
    }
    final RawErasureEncoder encoder =
            CodecUtil.createRSRawEncoder(conf,
                dataBytes.length, parityBytes.length);
    encoder.encode(dataBytes, expectedParityBytes);
    for (int i = 0; i < parityBytes.length; i++) {
      if (i != killedDnIndex) {
        Assert.assertArrayEquals("i=" + i + ", killedDnIndex=" + killedDnIndex,
            expectedParityBytes[i], parityBytes[i]);
      }
    }
  }
}
