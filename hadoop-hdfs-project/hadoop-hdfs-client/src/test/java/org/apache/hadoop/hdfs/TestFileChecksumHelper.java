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
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32C;

import org.apache.hadoop.fs.CompositeCrcFileChecksum;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32CastagnoliFileChecksum;
import org.apache.hadoop.fs.Options.ChecksumCombineMode;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockChecksumOptionsProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockChecksumTypeProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ChecksumTypeProto;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.util.CrcUtil;
import org.apache.hadoop.util.DataChecksum;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import static org.apache.hadoop.fs.Options.ChecksumCombineMode.COMPOSITE_CRC;
import static org.apache.hadoop.fs.Options.ChecksumCombineMode.MD5MD5CRC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Tests how {@link FileChecksumHelper} combines block checksums, in
 * particular for files that contain empty blocks.
 */
public class TestFileChecksumHelper {
  private static final int BYTES_PER_CRC = 512;
  private static final byte[] EMPTY = new byte[0];

  /**
   * Feeds block checksums the way DataNodes return them, without talking to
   * any DataNode. For COMPOSITE_CRC a non-empty block yields a 4-byte CRC,
   * while an empty block yields no bytes at all.
   */
  private static class FakeChecksumComputer
      extends FileChecksumHelper.FileChecksumComputer {
    private final List<byte[]> blockChecksums;

    FakeChecksumComputer(LocatedBlocks blocks, List<byte[]> blockChecksums,
        ChecksumCombineMode mode) throws IOException {
      // DistributedFileSystem#getFileChecksum(Path) asks for Long.MAX_VALUE.
      super("/test", Long.MAX_VALUE, blocks, null, null, mode);
      this.blockChecksums = blockChecksums;
    }

    @Override
    void checksumBlocks() throws IOException {
      setBytesPerCRC(BYTES_PER_CRC);
      setCrcType(DataChecksum.Type.CRC32C);
      for (byte[] blockChecksum : blockChecksums) {
        getBlockChecksumBuf().write(blockChecksum);
      }
    }
  }

  private static byte[] randomBytes(int len, long seed) {
    byte[] data = new byte[len];
    new Random(seed).nextBytes(data);
    return data;
  }

  private static int crc32c(byte[]... parts) {
    CRC32C crc = new CRC32C();
    for (byte[] part : parts) {
      crc.update(part);
    }
    return (int) crc.getValue();
  }

  /**
   * Computes a COMPOSITE_CRC file checksum for a file whose blocks hold the
   * given contents.
   */
  private static FileChecksum compositeChecksum(byte[]... blocks)
      throws IOException {
    List<LocatedBlock> located = new ArrayList<>();
    List<byte[]> checksums = new ArrayList<>();
    long fileLength = 0;
    for (int i = 0; i < blocks.length; i++) {
      located.add(new LocatedBlock(
          new ExtendedBlock("BP-test", i + 1, blocks[i].length, 1001L),
          new DatanodeInfo[0]));
      checksums.add(blocks[i].length == 0
          ? EMPTY : CrcUtil.intToBytes(crc32c(blocks[i])));
      fileLength += blocks[i].length;
    }
    LocatedBlocks locatedBlocks = new LocatedBlocks(fileLength, false,
        located, located.isEmpty() ? null : located.get(located.size() - 1),
        true, null, null);
    FakeChecksumComputer computer =
        new FakeChecksumComputer(locatedBlocks, checksums, COMPOSITE_CRC);
    computer.compute();
    return computer.getFileChecksum();
  }

  private static FileChecksum expectedComposite(byte[]... data) {
    return new CompositeCrcFileChecksum(
        crc32c(data), DataChecksum.Type.CRC32C, BYTES_PER_CRC);
  }

  @Test
  public void testCompositeCrcOfNonEmptyBlocks() throws IOException {
    byte[] a = randomBytes(1000, 1);
    byte[] b = randomBytes(700, 2);
    assertEquals(expectedComposite(a), compositeChecksum(a));
    assertEquals(expectedComposite(a, b), compositeChecksum(a, b));
  }

  /**
   * A zero-length file may still have an empty block, e.g. when its writer
   * died before any data was acknowledged. Its checksum must not fail and
   * must equal that of a zero-length file without blocks.
   */
  @Test
  public void testCompositeCrcOfZeroLengthFileWithEmptyBlock()
      throws IOException {
    assertEquals(compositeChecksum(), compositeChecksum(EMPTY));
  }

  @Test
  public void testCompositeCrcSkipsTrailingEmptyBlock() throws IOException {
    byte[] a = randomBytes(1000, 3);
    assertEquals(expectedComposite(a), compositeChecksum(a, EMPTY));
  }

  @Test
  public void testCompositeCrcSkipsEmptyBlockInTheMiddle() throws IOException {
    byte[] a = randomBytes(1000, 4);
    byte[] b = randomBytes(700, 5);
    assertEquals(expectedComposite(a, b), compositeChecksum(a, EMPTY, b));
  }

  @Test
  public void testMd5CrcOfZeroLengthFileWithEmptyBlockIsUnchanged()
      throws IOException {
    LocatedBlock empty = new LocatedBlock(
        new ExtendedBlock("BP-test", 1, 0, 1001L), new DatanodeInfo[0]);
    List<LocatedBlock> located = new ArrayList<>();
    located.add(empty);
    List<byte[]> checksums = new ArrayList<>();
    // An MD5CRC block checksum is always a 16-byte MD5, even for empty blocks.
    checksums.add(new byte[16]);
    FakeChecksumComputer computer = new FakeChecksumComputer(
        new LocatedBlocks(0, false, located, empty, true, null, null),
        checksums, MD5MD5CRC);
    computer.compute();
    assertInstanceOf(MD5MD5CRC32CastagnoliFileChecksum.class,
        computer.getFileChecksum());
  }

  /**
   * The debug representation of an empty COMPOSITE_CRC block checksum must
   * not fail when debug logging is enabled.
   */
  @Test
  public void testPopulateEmptyCompositeBlockChecksumWithDebugLogging()
      throws IOException {
    GenericTestUtils.setLogLevel(FileChecksumHelper.LOG, Level.DEBUG);
    FakeChecksumComputer computer = new FakeChecksumComputer(
        new LocatedBlocks(), new ArrayList<>(), COMPOSITE_CRC);
    OpBlockChecksumResponseProto response =
        OpBlockChecksumResponseProto.newBuilder()
            .setBytesPerCrc(BYTES_PER_CRC)
            .setCrcPerBlock(0)
            .setBlockChecksum(ByteString.EMPTY)
            .setCrcType(ChecksumTypeProto.CHECKSUM_CRC32C)
            .setBlockChecksumOptions(BlockChecksumOptionsProto.newBuilder()
                .setBlockChecksumType(BlockChecksumTypeProto.COMPOSITE_CRC))
            .build();
    assertNotEquals(null, computer.populateBlockChecksumBuf(response));
    assertEquals(0, computer.getBlockChecksumBuf().getLength());
  }
}
