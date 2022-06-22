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

package org.apache.hadoop.runc.squashfs.data;

import org.apache.hadoop.runc.squashfs.SquashFsException;
import org.apache.hadoop.runc.squashfs.data.DataBlockCache.Key;
import org.apache.hadoop.runc.squashfs.inode.FileINode;
import org.apache.hadoop.runc.squashfs.io.ByteBufferDataInput;
import org.apache.hadoop.runc.squashfs.io.MappedFile;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlockFlag;
import org.apache.hadoop.runc.squashfs.table.FragmentTable;
import org.apache.hadoop.runc.squashfs.table.FragmentTableEntry;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.IOException;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

public final class MappedDataBlockReader {

  private static final byte[] EMPTY = new byte[0];

  private MappedDataBlockReader() {
  }

  public static DataBlock readBlock(
      int tag,
      MappedFile mmap,
      SuperBlock sb,
      FileINode inode,
      int blockNum) throws IOException, SquashFsException {

    return readBlock(tag, mmap, sb, inode, blockNum, DataBlockCache.NO_CACHE);
  }

  public static DataBlock readBlock(
      int tag,
      MappedFile mmap,
      SuperBlock sb,
      FileINode inode,
      int blockNum,
      DataBlockCache cache) throws IOException, SquashFsException {

    int blockCount = inode.getBlockSizes().length;
    if (blockNum >= blockCount) {
      throw new SquashFsException(
          String.format("Attempted to read out of bounds block %d (count = %d)",
              blockNum, blockCount));
    }
    int blockSize = sb.getBlockSize();

    int[] blockSizes = inode.getBlockSizes();

    long blocksStart = inode.getBlocksStart();
    long fileSize = inode.getFileSize();
    long fileOffset = getFileOffset(blocksStart, blockNum, blockSizes);

    int dataSize = blockSizes[blockNum];
    boolean compressed = (dataSize & 0x1000000) == 0;
    int actualSize = (dataSize & 0xfffff);

    long expectedSize = blockSize;

    if (blockNum == blockCount - 1 && !inode.isFragmentPresent()) {
      expectedSize = fileSize - (blockSize * (blockCount - 1L));
    }

    if (actualSize == 0) {
      // sparse block
      return new DataBlock(EMPTY, (int) expectedSize, 0);
    }

    DataBlockCache.Key key =
        new Key(tag, compressed, fileOffset, actualSize, (int) expectedSize);
    DataBlock block = cache.get(key);
    if (block == null) {
      block = readData(sb, mmap, compressed, fileOffset, actualSize,
          (int) expectedSize);
      cache.put(key, block);
    }
    return block;
  }

  public static DataBlock readFragment(
      int tag,
      MappedFile mmap,
      SuperBlock sb,
      FileINode inode,
      FragmentTable fragTable,
      int length) throws IOException, SquashFsException {

    return readFragment(tag, mmap, sb, inode, fragTable, length,
        DataBlockCache.NO_CACHE);
  }

  public static DataBlock readFragment(
      int tag,
      MappedFile mmap,
      SuperBlock sb,
      FileINode inode,
      FragmentTable fragTable,
      int length,
      DataBlockCache cache) throws IOException, SquashFsException {

    FragmentTableEntry fragEntry =
        fragTable.getEntry(inode.getFragmentBlockIndex());

    boolean compressed = fragEntry.isCompressed();
    int dataSize = fragEntry.getDiskSize();

    long fileOffset = fragEntry.getStart();

    DataBlockCache.Key key =
        new Key(tag, compressed, fileOffset, dataSize, dataSize);
    DataBlock fragment = cache.get(key);
    if (fragment == null) {
      fragment = readData(sb, mmap, compressed, fileOffset, dataSize, dataSize);
      cache.put(key, fragment);
    }

    int offset = inode.getFragmentOffset();
    if (offset + length > fragment.getPhysicalSize()) {
      throw new SquashFsException(String.format("Attempted to read %d bytes "
              + "from a fragment with only %d bytes remaining",
          length, fragment.getLogicalSize() - offset));
    }

    byte[] data = new byte[length];
    System.arraycopy(fragment.getData(), offset, data, 0, length);
    return new DataBlock(data, data.length, data.length);
  }

  private static DataBlock readData(
      SuperBlock sb,
      MappedFile mmap,
      boolean compressed,
      long fileOffset,
      int dataSize,
      int expectedSize) throws IOException, SquashFsException {

    DataInput in = new ByteBufferDataInput(mmap.from(fileOffset));

    DataBlock data = compressed
        ? readCompressed(sb, in, dataSize, expectedSize)
        : readUncompressed(sb, in, dataSize, expectedSize);

    return data;
  }

  private static DataBlock readUncompressed(
      SuperBlock sb,
      DataInput in,
      int dataSize,
      int expectedSize) throws IOException, SquashFsException {
    byte[] data = new byte[dataSize];
    in.readFully(data);
    return new DataBlock(data, expectedSize, data.length);
  }

  private static DataBlock readCompressed(
      SuperBlock sb,
      DataInput in,
      int dataSize,
      int expectedSize) throws IOException, SquashFsException {
    switch (sb.getCompressionId()) {
    case NONE:
      throw new SquashFsException(
          "Archive claims no compression, but found compressed data");
    case ZLIB:
      return readCompressedZlib(sb, in, dataSize, expectedSize);
    default:
      throw new UnsupportedOperationException(
          String.format("Reading compressed data of type %s not yet supported",
              sb.getCompressionId()));
    }
  }

  private static DataBlock readCompressedZlib(
      SuperBlock sb,
      DataInput in,
      int dataSize,
      int expectedSize) throws IOException, SquashFsException {
    // see if there are compression flags
    if (sb.hasFlag(SuperBlockFlag.COMPRESSOR_OPTIONS)) {
      throw new UnsupportedOperationException("Reading ZLIB compressed data "
          + "with non-standard options not yet supported");
    }

    byte[] buf = new byte[dataSize];
    in.readFully(buf);
    byte[] data;

    byte[] xfer = new byte[4096];
    try (ByteArrayInputStream bis = new ByteArrayInputStream(buf)) {
      try (InflaterInputStream iis =
          new InflaterInputStream(bis, new Inflater(), 4096)) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(4096)) {
          int c = 0;
          while ((c = iis.read(xfer, 0, 4096)) >= 0) {
            if (c > 0) {
              bos.write(xfer, 0, c);
            }
          }
          data = bos.toByteArray();
          if (data.length > sb.getBlockSize()) {
            throw new SquashFsException(String.format(
                "Corrupt metadata block: Got size %d (max = %d)", data.length,
                sb.getBlockSize()));
          }
        }
      }
    }

    return new DataBlock(data, expectedSize, data.length);
  }

  static long getFileOffset(long blockStart, int blockNum, int[] blockSizes) {
    for (int i = 0; i < blockNum; i++) {
      blockStart += (blockSizes[i] & 0xfffff);
    }
    return blockStart;
  }

}
