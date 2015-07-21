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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.web.ByteRangeInputStream;
import org.junit.Assert;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

public class StripedFileTestUtil {
  static int dataBlocks = HdfsConstants.NUM_DATA_BLOCKS;
  static int parityBlocks = HdfsConstants.NUM_PARITY_BLOCKS;

  static final int cellSize = HdfsConstants.BLOCK_STRIPED_CELL_SIZE;
  static final int stripesPerBlock = 4;
  static final int blockSize = cellSize * stripesPerBlock;
  static final int numDNs = dataBlocks + parityBlocks + 2;

  static final Random random = new Random();

  static byte[] generateBytes(int cnt) {
    byte[] bytes = new byte[cnt];
    for (int i = 0; i < cnt; i++) {
      bytes[i] = getByte(i);
    }
    return bytes;
  }

  static int readAll(FSDataInputStream in, byte[] buf) throws IOException {
    int readLen = 0;
    int ret;
    while ((ret = in.read(buf, readLen, buf.length - readLen)) >= 0 &&
        readLen <= buf.length) {
      readLen += ret;
    }
    return readLen;
  }

  static byte getByte(long pos) {
    final int mod = 29;
    return (byte) (pos % mod + 1);
  }

  static void verifyLength(FileSystem fs, Path srcPath, int fileLength)
      throws IOException {
    FileStatus status = fs.getFileStatus(srcPath);
    Assert.assertEquals("File length should be the same", fileLength, status.getLen());
  }

  static void verifyPread(FileSystem fs, Path srcPath,  int fileLength,
      byte[] expected, byte[] buf) throws IOException {
    try (FSDataInputStream in = fs.open(srcPath)) {
      int[] startOffsets = {0, 1, cellSize - 102, cellSize, cellSize + 102,
          cellSize * (dataBlocks - 1), cellSize * (dataBlocks - 1) + 102,
          cellSize * dataBlocks, fileLength - 102, fileLength - 1};
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
          Assert.assertEquals("Byte at " + (startOffset + i) + " is different, "
                  + "the startOffset is " + startOffset,
              expected[startOffset + i], result[i]);
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
      Assert.assertEquals("The length of file should be the same to write size",
          fileLength, readLen);
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
      Assert.assertEquals("The length of file should be the same to write size",
          fileLength, readLen);
      Assert.assertArrayEquals(expected, result.array());
    }
  }

  static void verifySeek(FileSystem fs, Path srcPath, int fileLength)
      throws IOException {
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

      if (fileLength > cellSize) {
        // seek to cellSize boundary
        pos = cellSize - 1;
        assertSeekAndRead(in, pos, fileLength);
      }

      if (fileLength > cellSize * dataBlocks) {
        // seek to striped cell group boundary
        pos = cellSize * dataBlocks - 1;
        assertSeekAndRead(in, pos, fileLength);
      }

      if (fileLength > blockSize * dataBlocks) {
        // seek to striped block group boundary
        pos = blockSize * dataBlocks - 1;
        assertSeekAndRead(in, pos, fileLength);
      }

      if (!(in.getWrappedStream() instanceof ByteRangeInputStream)) {
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
    byte[] buf = new byte[writeBytes];
    int readLen = StripedFileTestUtil.readAll(fsdis, buf);
    Assert.assertEquals(readLen, writeBytes - pos);
    for (int i = 0; i < readLen; i++) {
      Assert.assertEquals("Byte at " + i + " should be the same",
          StripedFileTestUtil.getByte(pos + i), buf[i]);
    }
  }
}
