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

public class ExtendedFileINode extends AbstractINode implements FileINode {

  private long blocksStart;
  private long fileSize;
  private long sparse;
  private int nlink = 1;
  private int fragmentBlockIndex = FRAGMENT_BLOCK_INDEX_NONE;
  private int fragmentOffset = 0;
  private int xattrIndex = XATTR_NOT_PRESENT;
  private int[] blockSizes;

  @Override
  public long getBlocksStart() {
    return blocksStart;
  }

  @Override
  public void setBlocksStart(long blocksStart) {
    this.blocksStart = blocksStart;
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
    return fileSize;
  }

  @Override
  public void setFileSize(long fileSize) {
    this.fileSize = fileSize;
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
  public long getSparse() {
    return sparse;
  }

  @Override
  public void setSparse(long sparse) {
    this.sparse = sparse;
  }

  @Override
  public int getNlink() {
    return nlink;
  }

  @Override
  public void setNlink(int nlink) {
    this.nlink = nlink;
  }

  @Override
  public int getXattrIndex() {
    return xattrIndex;
  }

  @Override
  public void setXattrIndex(int xattrIndex) {
    this.xattrIndex = xattrIndex;
  }

  @Override
  public boolean isXattrPresent() {
    return xattrIndex != XATTR_NOT_PRESENT;
  }

  @Override
  protected int getChildSerializedSize() {
    return 40 + (blockSizes.length * 4);
  }

  @Override
  public boolean isFragmentPresent() {
    return fragmentBlockIndex != FRAGMENT_BLOCK_INDEX_NONE;
  }

  @Override
  public boolean isSparseBlockPresent() {
    return sparse != 0L;
  }

  private int fullBlockCount(SuperBlock sb) {
    int blockSize = sb.getBlockSize();
    int blockCount = (int) (fileSize / blockSize);
    if (!isFragmentPresent() && (fileSize % blockSize != 0)) {
      blockCount++;
    }
    return blockCount;
  }

  @Override
  protected String getName() {
    return "extended-file-inode";
  }

  @Override
  public INodeType getInodeType() {
    return INodeType.EXTENDED_FILE;
  }

  @Override
  protected void readExtraData(SuperBlock sb, DataInput in)
      throws SquashFsException, IOException {
    blocksStart = in.readLong();
    fileSize = in.readLong();
    sparse = in.readLong();
    nlink = in.readInt();
    fragmentBlockIndex = in.readInt();
    fragmentOffset = in.readInt();
    xattrIndex = in.readInt();

    int blockCount = fullBlockCount(sb);
    blockSizes = new int[blockCount];
    for (int i = 0; i < blockCount; i++) {
      blockSizes[i] = in.readInt();
    }
  }

  @Override
  protected void writeExtraData(MetadataWriter out) throws IOException {
    out.writeLong(blocksStart);
    out.writeLong(fileSize);
    out.writeLong(sparse);
    out.writeInt(nlink);
    out.writeInt(fragmentBlockIndex);
    out.writeInt(fragmentOffset);
    out.writeInt(xattrIndex);
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
    dumpBin(buf, width, "fileSize", fileSize, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "sparse", sparse, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "nlink", nlink, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "fragmentBlockIndex", fragmentBlockIndex, DECIMAL,
        UNSIGNED);
    dumpBin(buf, width, "fragmentOffset", fragmentOffset, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "xattrIndex", xattrIndex, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "blocks", blockSizes.length, DECIMAL, UNSIGNED);
    for (int i = 0; i < blockSizes.length; i++) {
      dumpBin(buf, width, String.format("blockSizes[%d]", i), blockSizes[i],
          DECIMAL, UNSIGNED);
    }
  }

  @Override
  public FileINode simplify() {
    return BasicFileINode.simplify(this);
  }
}
