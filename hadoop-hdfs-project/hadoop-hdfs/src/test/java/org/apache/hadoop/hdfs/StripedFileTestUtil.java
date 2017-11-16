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

import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.impl.BlockReaderTestUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem.WebHdfsInputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class StripedFileTestUtil {
  public static final Logger LOG =
      LoggerFactory.getLogger(StripedFileTestUtil.class);

  public static byte[] generateBytes(int cnt) {
    byte[] bytes = new byte[cnt];
    for (int i = 0; i < cnt; i++) {
      bytes[i] = getByte(i);
    }
    return bytes;
  }

  static byte getByte(long pos) {
    final int mod = 29;
    return (byte) (pos % mod + 1);
  }

  static void verifyLength(FileSystem fs, Path srcPath, int fileLength)
      throws IOException {
    FileStatus status = fs.getFileStatus(srcPath);
    assertEquals("File length should be the same", fileLength, status.getLen());
  }

  static void verifyPread(DistributedFileSystem fs, Path srcPath,
      int fileLength, byte[] expected, byte[] buf) throws IOException {
    final ErasureCodingPolicy ecPolicy = fs.getErasureCodingPolicy(srcPath);
    verifyPread(fs, srcPath, fileLength, expected, buf, ecPolicy);
  }

  static void verifyPread(FileSystem fs, Path srcPath, int fileLength,
      byte[] expected, byte[] buf, ErasureCodingPolicy ecPolicy)
      throws IOException {
    try (FSDataInputStream in = fs.open(srcPath)) {
      int[] startOffsets = {0, 1, ecPolicy.getCellSize() - 102,
          ecPolicy.getCellSize(), ecPolicy.getCellSize() + 102,
          ecPolicy.getCellSize() * (ecPolicy.getNumDataUnits() - 1),
          ecPolicy.getCellSize() * (ecPolicy.getNumDataUnits() - 1) + 102,
          ecPolicy.getCellSize() * ecPolicy.getNumDataUnits(),
          fileLength - 102, fileLength - 1};
      for (int startOffset : startOffsets) {
        startOffset = Math.max(0, Math.min(startOffset, fileLength - 1));
        int remaining = fileLength - startOffset;
        int offset = startOffset;
        final byte[] result = new byte[remaining];
        while (remaining > 0) {
          int target = Math.min(remaining, buf.length);
          in.readFully(offset, buf, 0, target);
          System.arraycopy(buf, 0, result, offset - startOffset, target);
          remaining -= target;
          offset += target;
        }
        for (int i = 0; i < fileLength - startOffset; i++) {
          assertEquals("Byte at " + (startOffset + i) + " is different, "
              + "the startOffset is " + startOffset, expected[startOffset + i],
              result[i]);
        }
      }
    }
  }

  static void verifyStatefulRead(FileSystem fs, Path srcPath, int fileLength,
      byte[] expected, byte[] buf) throws IOException {
    try (FSDataInputStream in = fs.open(srcPath)) {
      final byte[] result = new byte[fileLength];
      int readLen = 0;
      int ret;
      while ((ret = in.read(buf, 0, buf.length)) >= 0) {
        System.arraycopy(buf, 0, result, readLen, ret);
        readLen += ret;
      }
      assertEquals("The length of file should be the same to write size", fileLength, readLen);
      Assert.assertArrayEquals(expected, result);
    }
  }

  static void verifyStatefulRead(FileSystem fs, Path srcPath, int fileLength,
      byte[] expected, ByteBuffer buf) throws IOException {
    try (FSDataInputStream in = fs.open(srcPath)) {
      ByteBuffer result = ByteBuffer.allocate(fileLength);
      int readLen = 0;
      int ret;
      while ((ret = in.read(buf)) >= 0) {
        readLen += ret;
        buf.flip();
        result.put(buf);
        buf.clear();
      }
      assertEquals("The length of file should be the same to write size", fileLength, readLen);
      Assert.assertArrayEquals(expected, result.array());
    }
  }

  static void verifySeek(FileSystem fs, Path srcPath, int fileLength,
      ErasureCodingPolicy ecPolicy, int blkGroupSize) throws IOException {
    try (FSDataInputStream in = fs.open(srcPath)) {
      // seek to 1/2 of content
      int pos = fileLength / 2;
      assertSeekAndRead(in, pos, fileLength);

      // seek to 1/3 of content
      pos = fileLength / 3;
      assertSeekAndRead(in, pos, fileLength);

      // seek to 0 pos
      pos = 0;
      assertSeekAndRead(in, pos, fileLength);

      if (fileLength > ecPolicy.getCellSize()) {
        // seek to cellSize boundary
        pos = ecPolicy.getCellSize() - 1;
        assertSeekAndRead(in, pos, fileLength);
      }

      if (fileLength > ecPolicy.getCellSize() * ecPolicy.getNumDataUnits()) {
        // seek to striped cell group boundary
        pos = ecPolicy.getCellSize() * ecPolicy.getNumDataUnits() - 1;
        assertSeekAndRead(in, pos, fileLength);
      }

      if (fileLength > blkGroupSize) {
        // seek to striped block group boundary
        pos = blkGroupSize - 1;
        assertSeekAndRead(in, pos, fileLength);
      }

      if (!(in.getWrappedStream() instanceof WebHdfsInputStream)) {
        try {
          in.seek(-1);
          Assert.fail("Should be failed if seek to negative offset");
        } catch (EOFException e) {
          // expected
        }

        try {
          in.seek(fileLength + 1);
          Assert.fail("Should be failed if seek after EOF");
        } catch (EOFException e) {
          // expected
        }
      }
    }
  }

  static void assertSeekAndRead(FSDataInputStream fsdis, int pos,
      int writeBytes) throws IOException {
    fsdis.seek(pos);
    byte[] buf = new byte[writeBytes - pos];
    IOUtils.readFully(fsdis, buf, 0, buf.length);
    for (int i = 0; i < buf.length; i++) {
      assertEquals("Byte at " + i + " should be the same",
          StripedFileTestUtil.getByte(pos + i), buf[i]);
    }
  }

  static void killDatanode(MiniDFSCluster cluster, DFSStripedOutputStream out,
      final int dnIndex, final AtomicInteger pos) {
    final StripedDataStreamer s = out.getStripedDataStreamer(dnIndex);
    final DatanodeInfo datanode = getDatanodes(s);
    assert datanode != null;
    LOG.info("killDatanode " + dnIndex + ": " + datanode + ", pos=" + pos);
    cluster.stopDataNode(datanode.getXferAddr());
  }

  static DatanodeInfo getDatanodes(StripedDataStreamer streamer) {
    for(;;) {
      final DatanodeInfo[] datanodes = streamer.getNodes();
      if (datanodes != null) {
        assertEquals(1, datanodes.length);
        Assert.assertNotNull(datanodes[0]);
        return datanodes[0];
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignored) {
        return null;
      }
    }
  }

  /**
   * If the length of blockGroup is less than a full stripe, it returns the the
   * number of actual data internal blocks. Otherwise returns NUM_DATA_BLOCKS.
   */
  public static short getRealDataBlockNum(int numBytesInStrip,
      ErasureCodingPolicy ecPolicy) {
    return (short) Math.min(ecPolicy.getNumDataUnits(),
        (numBytesInStrip - 1) / ecPolicy.getCellSize() + 1);
  }

  public static short getRealTotalBlockNum(int numBytesInStrip,
      ErasureCodingPolicy ecPolicy) {
    return (short) (getRealDataBlockNum(numBytesInStrip, ecPolicy) +
        ecPolicy.getNumParityUnits());
  }

  public static void waitBlockGroupsReported(DistributedFileSystem fs,
      String src) throws Exception {
    waitBlockGroupsReported(fs, src, 0);
  }

  /**
   * Wait for all the internalBlocks of the blockGroups of the given file to be
   * reported.
   */
  public static void waitBlockGroupsReported(DistributedFileSystem fs,
      String src, int numDeadDNs) throws Exception {
    boolean success;
    final int ATTEMPTS = 40;
    int count = 0;
    final ErasureCodingPolicy ecPolicy =
        fs.getErasureCodingPolicy(new Path(src));
    do {
      success = true;
      count++;
      LocatedBlocks lbs = fs.getClient().getLocatedBlocks(src, 0);
      for (LocatedBlock lb : lbs.getLocatedBlocks()) {
        short expected = (short) (getRealTotalBlockNum((int) lb.getBlockSize(),
            ecPolicy) - numDeadDNs);
        int reported = lb.getLocations().length;
        if (reported < expected){
          success = false;
          LOG.info("blockGroup " + lb.getBlock() + " of file " + src
              + " has reported internalBlocks " + reported
              + " (desired " + expected + "); locations "
              + Joiner.on(' ').join(lb.getLocations()));
          Thread.sleep(1000);
          break;
        }
      }
      if (success) {
        LOG.info("All blockGroups of file " + src
            + " verified to have all internalBlocks.");
      }
    } while (!success && count < ATTEMPTS);

    if (count == ATTEMPTS) {
      throw new TimeoutException("Timed out waiting for " + src +
          " to have all the internalBlocks");
    }
  }

  /**
   * Generate n random and different numbers within
   * specified non-negative integer range
   * @param min minimum of the range
   * @param max maximum of the range
   * @param n number to be generated
   */
  public static int[] randomArray(int min, int max, int n){
    if (n > (max - min + 1) || max < min || min < 0 || max < 0) {
      return null;
    }
    int[] result = new int[n];
    for (int i = 0; i < n; i++) {
      result[i] = -1;
    }

    int count = 0;
    while(count < n) {
      int num = (int) (Math.random() * (max - min)) + min;
      boolean flag = true;
      for (int j = 0; j < n; j++) {
        if(num == result[j]){
          flag = false;
          break;
        }
      }
      if(flag){
        result[count] = num;
        count++;
      }
    }
    return result;
  }

  /**
   * Verify that blocks in striped block group are on different nodes, and every
   * internal blocks exists.
   */
  public static void verifyLocatedStripedBlocks(LocatedBlocks lbs, int groupSize) {
    for (LocatedBlock lb : lbs.getLocatedBlocks()) {
      assert lb instanceof LocatedStripedBlock;
      HashSet<DatanodeInfo> locs = new HashSet<>();
      Collections.addAll(locs, lb.getLocations());
      assertEquals(groupSize, lb.getLocations().length);
      assertEquals(groupSize, locs.size());

      // verify that every internal blocks exists
      byte[] blockIndices = ((LocatedStripedBlock) lb).getBlockIndices();
      assertEquals(groupSize, blockIndices.length);
      HashSet<Integer> found = new HashSet<>();
      for (int index : blockIndices) {
        assert index >=0;
        found.add(index);
      }
      assertEquals(groupSize, found.size());
    }
  }

  static void checkData(DistributedFileSystem dfs, Path srcPath, int length,
      List<DatanodeInfo> killedList, List<Long> oldGSList, int blkGroupSize)
      throws IOException {

    StripedFileTestUtil.verifyLength(dfs, srcPath, length);
    List<List<LocatedBlock>> blockGroupList = new ArrayList<>();
    LocatedBlocks lbs = dfs.getClient().getLocatedBlocks(srcPath.toString(), 0L,
        Long.MAX_VALUE);
    int expectedNumGroup = 0;
    if (length > 0) {
      expectedNumGroup = (length - 1) / blkGroupSize + 1;
    }
    assertEquals(expectedNumGroup, lbs.getLocatedBlocks().size());

    final ErasureCodingPolicy ecPolicy = dfs.getErasureCodingPolicy(srcPath);
    final int cellSize = ecPolicy.getCellSize();
    final int dataBlkNum = ecPolicy.getNumDataUnits();
    final int parityBlkNum = ecPolicy.getNumParityUnits();
    int index = 0;
    for (LocatedBlock firstBlock : lbs.getLocatedBlocks()) {
      Assert.assertTrue(firstBlock instanceof LocatedStripedBlock);

      final long gs = firstBlock.getBlock().getGenerationStamp();
      final long oldGS = oldGSList != null ? oldGSList.get(index++) : -1L;
      final String s = "gs=" + gs + ", oldGS=" + oldGS;
      LOG.info(s);
      Assert.assertTrue(s, gs >= oldGS);

      LocatedBlock[] blocks = StripedBlockUtil.parseStripedBlockGroup(
          (LocatedStripedBlock) firstBlock, cellSize,
          dataBlkNum, parityBlkNum);
      blockGroupList.add(Arrays.asList(blocks));
    }

    // test each block group
    for (int group = 0; group < blockGroupList.size(); group++) {
      final boolean isLastGroup = group == blockGroupList.size() - 1;
      final int groupSize = !isLastGroup? blkGroupSize
          : length - (blockGroupList.size() - 1)*blkGroupSize;
      final int numCellInGroup = (groupSize - 1) / cellSize + 1;
      final int lastCellIndex = (numCellInGroup - 1) % dataBlkNum;
      final int lastCellSize = groupSize - (numCellInGroup - 1) * cellSize;

      //get the data of this block
      List<LocatedBlock> blockList = blockGroupList.get(group);
      byte[][] dataBlockBytes = new byte[dataBlkNum][];
      byte[][] parityBlockBytes = new byte[parityBlkNum][];

      Set<Integer> checkSet = new HashSet<>();
      // for each block, use BlockReader to read data
      for (int i = 0; i < blockList.size(); i++) {
        final int j = i >= dataBlkNum? 0: i;
        final int numCellInBlock = (numCellInGroup - 1) / dataBlkNum
            + (j <= lastCellIndex? 1: 0);
        final int blockSize = numCellInBlock * cellSize
            + (isLastGroup && j == lastCellIndex? lastCellSize - cellSize: 0);

        final byte[] blockBytes = new byte[blockSize];
        if (i < dataBlkNum) {
          dataBlockBytes[i] = blockBytes;
        } else {
          parityBlockBytes[i - dataBlkNum] = blockBytes;
        }

        final LocatedBlock lb = blockList.get(i);
        LOG.info("i,j=" + i + ", " + j + ", numCellInBlock=" + numCellInBlock
            + ", blockSize=" + blockSize + ", lb=" + lb);
        if (lb == null) {
          continue;
        }
        final ExtendedBlock block = lb.getBlock();
        assertEquals(blockSize, block.getNumBytes());

        if (block.getNumBytes() == 0) {
          continue;
        }

        DatanodeInfo dn = blockList.get(i).getLocations()[0];
        if (!killedList.contains(dn)) {
          final BlockReader blockReader = BlockReaderTestUtil.getBlockReader(
              dfs, lb, 0, block.getNumBytes());
          blockReader.readAll(blockBytes, 0, (int) block.getNumBytes());
          blockReader.close();
          checkSet.add(i);
        }
      }
      LOG.info("Internal blocks to check: " + checkSet);

      // check data
      final int groupPosInFile = group * blkGroupSize;
      for (int i = 0; i < dataBlockBytes.length; i++) {
        boolean killed = false;
        if (!checkSet.contains(i)) {
          killed = true;
        }
        final byte[] actual = dataBlockBytes[i];
        for (int posInBlk = 0; posInBlk < actual.length; posInBlk++) {
          final long posInFile = StripedBlockUtil.offsetInBlkToOffsetInBG(
              cellSize, dataBlkNum, posInBlk, i) + groupPosInFile;
          Assert.assertTrue(posInFile < length);
          final byte expected = getByte(posInFile);

          if (killed) {
            actual[posInBlk] = expected;
          } else {
            if(expected != actual[posInBlk]){
              String s = "expected=" + expected + " but actual=" + actual[posInBlk]
                  + ", posInFile=" + posInFile + ", posInBlk=" + posInBlk
                  + ". group=" + group + ", i=" + i;
              Assert.fail(s);
            }
          }
        }
      }

      // check parity
      verifyParityBlocks(dfs.getConf(),
          lbs.getLocatedBlocks().get(group).getBlockSize(),
          cellSize, dataBlockBytes, parityBlockBytes, checkSet,
          ecPolicy.getCodecName());
    }
  }

  static void verifyParityBlocks(Configuration conf, final long size,
      final int cellSize, byte[][] dataBytes, byte[][] parityBytes,
      Set<Integer> checkSet, String codecName) {
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

    ErasureCoderOptions coderOptions = new ErasureCoderOptions(
        dataBytes.length, parityBytes.length);
    final RawErasureEncoder encoder =
        CodecUtil.createRawEncoder(conf, codecName, coderOptions);
    try {
      encoder.encode(dataBytes, expectedParityBytes);
    } catch (IOException e) {
      Assert.fail("Unexpected IOException: " + e.getMessage());
    }
    for (int i = 0; i < parityBytes.length; i++) {
      if (checkSet.contains(i + dataBytes.length)){
        Assert.assertArrayEquals("i=" + i, expectedParityBytes[i],
            parityBytes[i]);
      }
    }
  }

  /**
   * Wait for the reconstruction to be finished when the file has
   * corrupted blocks.
   */
  public static LocatedBlocks waitForReconstructionFinished(Path file,
                                  DistributedFileSystem fs, int groupSize)
      throws Exception {
    LOG.info("Waiting for reconstruction to be finished for the file:" + file
        + ", groupSize:" + groupSize);
    final int attempts = 60;
    for (int i = 0; i < attempts; i++) {
      LocatedBlocks locatedBlocks = getLocatedBlocks(file, fs);
      LocatedStripedBlock lastBlock =
          (LocatedStripedBlock)locatedBlocks.getLastLocatedBlock();
      DatanodeInfo[] storageInfos = lastBlock.getLocations();
      if (storageInfos.length >= groupSize) {
        return locatedBlocks;
      }
      Thread.sleep(1000);
    }
    throw new IOException("Time out waiting for EC block reconstruction.");
  }

  /**
   * Wait for the reconstruction to be finished when the file has
   * corrupted blocks. The function can take care file with any length.
   */
  public static void waitForAllReconstructionFinished(Path file,
      DistributedFileSystem fs, long expectedBlocks) throws Exception {
    LOG.info("Waiting for reconstruction to be finished for the file:" + file
        + ", expectedBlocks:" + expectedBlocks);
    final int attempts = 60;
    for (int i = 0; i < attempts; i++) {
      int totalBlocks = 0;
      LocatedBlocks locatedBlocks = getLocatedBlocks(file, fs);
      for (LocatedBlock locatedBlock: locatedBlocks.getLocatedBlocks()) {
        DatanodeInfo[] storageInfos = locatedBlock.getLocations();
        totalBlocks += storageInfos.length;
      }
      if (totalBlocks >= expectedBlocks) {
        return;
      }
      Thread.sleep(1000);
    }
    throw new IOException("Time out waiting for EC block reconstruction.");
  }

  /**
   * Get the located blocks of a file.
   */
  public static LocatedBlocks getLocatedBlocks(Path file,
                                               DistributedFileSystem fs)
      throws IOException {
    return fs.getClient().getLocatedBlocks(file.toString(), 0, Long.MAX_VALUE);
  }

  /**
   * Get system-wide default Erasure Coding Policy, which can be
   * used by default when no policy is specified for a path.
   * @return ErasureCodingPolicy
   */
  public static ErasureCodingPolicy getDefaultECPolicy() {
    return SystemErasureCodingPolicies.getPolicies().get(0);
  }

  /**
   * Get non-default Erasure Coding Policy randomly.
   * @return ErasureCodingPolicy
   */
  public static ErasureCodingPolicy getRandomNonDefaultECPolicy() {
    Random rand = new Random();
    List<ErasureCodingPolicy> policies = SystemErasureCodingPolicies
            .getPolicies();
    return policies.get(1 + rand.nextInt(policies.size() - 1));
  }
}
