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

package org.apache.hadoop.runc.squashfs.inode;

import org.apache.hadoop.runc.squashfs.SquashFsException;
import org.apache.hadoop.runc.squashfs.metadata.MetadataWriter;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;

import java.io.DataInput;
import java.io.IOException;

import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.DECIMAL;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.UNSIGNED;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.dumpBin;

public class BasicFileINode extends AbstractINode implements FileINode {

  public static final long MAX_BLOCKS_START = 0xffff_ffffL;
  public static final long MAX_FILE_SIZE = 0xffff_ffffL;

  private static final int[] EMPTY = new int[0];

  private int blocksStart;
  private int fragmentBlockIndex = FRAGMENT_BLOCK_INDEX_NONE;
  private int fragmentOffset = 0;
  private int fileSize;
  private int[] blockSizes = EMPTY;

  static FileINode simplify(FileINode src) {
    if (src instanceof BasicFileINode) {
      return src;
    }

    if (src.getBlocksStart() > MAX_BLOCKS_START) {
      return src;
    }

    if (src.getFileSize() > MAX_FILE_SIZE) {
      return src;
    }

    if (src.getNlink() > 1) {
      return src;
    }

    if (src.isSparseBlockPresent()) {
      return src;
    }

    if (src.isXattrPresent()) {
      return src;
    }

    BasicFileINode dest = new BasicFileINode();
    src.copyTo(dest);

    dest.setBlocksStart(src.getBlocksStart());
    dest.setFragmentBlockIndex(src.getFragmentBlockIndex());
    dest.setFragmentOffset(src.getFragmentOffset());
    dest.setFileSize(src.getFileSize());
    dest.setBlockSizes(src.getBlockSizes());

    return dest;
  }

  @Override
  public long getBlocksStart() {
    return blocksStart & MAX_BLOCKS_START;
  }

  @Override
  public void setBlocksStart(long blocksStart) {
    if (blocksStart > MAX_BLOCKS_START) {
      throw new IllegalArgumentException(
          "Basic file inodes do not support blocks starting > 4G");
    }
    this.blocksStart = (int) (blocksStart & MAX_BLOCKS_START);
  }

  @Override
  public int getFragmentBlockIndex() {
    return fragmentBlockIndex;
  }

  @Override
  public void setFragmentBlockIndex(int fragmentBlockIndex) {
    this.fragmentBlockIndex = fragmentBlockIndex;
  }

  @Override
  public int getFragmentOffset() {
    return fragmentOffset;
  }

  @Override
  public void setFragmentOffset(int fragmentOffset) {
    this.fragmentOffset = fragmentOffset;
  }

  @Override
  public long getFileSize() {
    return fileSize & MAX_FILE_SIZE;
  }

  @Override
  public void setFileSize(long fileSize) {
    if (fileSize > MAX_FILE_SIZE) {
      throw new IllegalArgumentException(
          "Basic file inodes do not support size > 4G");
    }

    this.fileSize = (int) (fileSize & MAX_FILE_SIZE);
  }

  @Override
  public boolean isFragmentPresent() {
    return fragmentBlockIndex != FRAGMENT_BLOCK_INDEX_NONE;
  }

  @Override
  public long getSparse() {
    return 0L;
  }

  @Override
  public void setSparse(long sparse) {
    if (sparse != 0L) {
      throw new IllegalArgumentException(
          "Basic file inodes do not support sparse blocks");
    }
  }

  @Override
  public int getNlink() {
    return 1;
  }

  @Override
  public void setNlink(int nlink) {
    if (nlink > 1) {
      throw new IllegalArgumentException(
          "Basic file inodes do not support multiple links");
    }
  }

  @Override
  public int getXattrIndex() {
    return XATTR_NOT_PRESENT;
  }

  @Override
  public void setXattrIndex(int xattrIndex) {
    if (xattrIndex != XATTR_NOT_PRESENT) {
      throw new IllegalArgumentException(
          "Basic file inodes do not support extended attributes");
    }
  }

  @Override
  public boolean isXattrPresent() {
    return false;
  }

  @Override
  public boolean isSparseBlockPresent() {
    return false;
  }

  @Override
  public int[] getBlockSizes() {
    return blockSizes;
  }

  @Override
  public void setBlockSizes(int[] blockSizes) {
    this.blockSizes = blockSizes;
  }

  @Override
  protected int getChildSerializedSize() {
    return 16 + (blockSizes.length * 4);
  }

  private int fullBlockCount(SuperBlock sb) {
    int blockSize = sb.getBlockSize();
    int blockCount = (fileSize / blockSize);
    if (!isFragmentPresent() && (fileSize % blockSize != 0)) {
      blockCount++;
    }
    return blockCount;
  }

  @Override
  protected String getName() {
    return "basic-file-inode";
  }

  @Override
  public INodeType getInodeType() {
    return INodeType.BASIC_FILE;
  }

  @Override
  protected void readExtraData(SuperBlock sb, DataInput in)
      throws SquashFsException, IOException {
    blocksStart = in.readInt();
    fragmentBlockIndex = in.readInt();
    fragmentOffset = in.readInt();
    fileSize = in.readInt();
    int blockCount = fullBlockCount(sb);
    blockSizes = new int[blockCount];
    for (int i = 0; i < blockCount; i++) {
      blockSizes[i] = in.readInt();
    }
  }

  @Override
  protected void writeExtraData(MetadataWriter out) throws IOException {
    out.writeInt(blocksStart);
    out.writeInt(fragmentBlockIndex);
    out.writeInt(fragmentOffset);
    out.writeInt(fileSize);
    for (int bs : blockSizes) {
      out.writeInt(bs);
    }
  }

  @Override
  protected int getPreferredDumpWidth() {
    return 20;
  }

  @Override
  protected void dumpProperties(StringBuilder buf, int width) {
    dumpBin(buf, width, "blocksStart", blocksStart, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "fragmentBlockIndex", fragmentBlockIndex, DECIMAL,
        UNSIGNED);
    dumpBin(buf, width, "fragmentOffset", fragmentOffset, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "fileSize", fileSize, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "blocks", blockSizes.length, DECIMAL, UNSIGNED);
    for (int i = 0; i < blockSizes.length; i++) {
      dumpBin(buf, width, String.format("blockSizes[%d]", i), blockSizes[i],
          DECIMAL, UNSIGNED);
    }
  }

  @Override
  public FileINode simplify() {
    return this;
  }
}
