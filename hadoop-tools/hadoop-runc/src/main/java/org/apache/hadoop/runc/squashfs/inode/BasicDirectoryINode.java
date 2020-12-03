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

public class BasicDirectoryINode extends AbstractINode
    implements DirectoryINode {

  private int startBlock;
  private int nlink = 1;
  private short fileSize; // 3 + # of uncompressed bytes in directory table
  private short offset;
  private int parentInodeNumber;

  static DirectoryINode simplify(DirectoryINode src) {
    if (src instanceof BasicDirectoryINode) {
      return src;
    }

    if (src.getFileSize() > 0xffff) {
      return src;
    }

    if (src.isIndexPresent()) {
      return src;
    }

    if (src.isXattrPresent()) {
      return src;
    }

    BasicDirectoryINode dest = new BasicDirectoryINode();
    src.copyTo(dest);

    dest.setStartBlock(src.getStartBlock());
    dest.setNlink(src.getNlink());
    dest.setFileSize(src.getFileSize());
    dest.setOffset(src.getOffset());
    dest.setParentInodeNumber(src.getParentInodeNumber());

    return dest;
  }

  @Override
  public int getStartBlock() {
    return startBlock;
  }

  @Override
  public void setStartBlock(int startBlock) {
    this.startBlock = startBlock;
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
  public int getFileSize() {
    return fileSize & 0xffff;
  }

  @Override
  public void setFileSize(int fileSize) {
    if (fileSize >= 0xffff) {
      throw new IllegalArgumentException(
          "Basic directory inodes do not support filesizes > 64K");
    }
    this.fileSize = (short) (fileSize & 0xffff);
  }

  @Override
  public short getOffset() {
    return offset;
  }

  @Override
  public void setOffset(short offset) {
    this.offset = offset;
  }

  @Override
  public int getParentInodeNumber() {
    return parentInodeNumber;
  }

  @Override
  public void setParentInodeNumber(int parentInodeNumber) {
    this.parentInodeNumber = parentInodeNumber;
  }

  @Override
  public short getIndexCount() {
    return 0;
  }

  @Override
  public void setIndexCount(short indexCount) {
    if (indexCount != (short) 0) {
      throw new IllegalArgumentException(
          "Basic directory inodes do not support indexes");
    }
  }

  @Override
  public boolean isIndexPresent() {
    return false;
  }

  @Override
  public int getXattrIndex() {
    return XATTR_NOT_PRESENT;
  }

  @Override
  public void setXattrIndex(int xattrIndex) {
    if (xattrIndex != XATTR_NOT_PRESENT) {
      throw new IllegalArgumentException(
          "Basic directory inodes do not support extended attributes");
    }
  }

  @Override
  public boolean isXattrPresent() {
    return false;
  }

  @Override
  protected int getChildSerializedSize() {
    return 16;
  }

  @Override
  protected String getName() {
    return "basic-directory-inode";
  }

  @Override
  public INodeType getInodeType() {
    return INodeType.BASIC_DIRECTORY;
  }

  @Override
  protected void readExtraData(SuperBlock sb, DataInput in)
      throws SquashFsException, IOException {
    startBlock = in.readInt();
    nlink = in.readInt();
    fileSize = in.readShort();
    offset = in.readShort();
    parentInodeNumber = in.readInt();
  }

  @Override
  protected void writeExtraData(MetadataWriter out) throws IOException {
    out.writeInt(startBlock);
    out.writeInt(nlink);
    out.writeShort(fileSize);
    out.writeShort(offset);
    out.writeInt(parentInodeNumber);
  }

  @Override
  protected int getPreferredDumpWidth() {
    return 19;
  }

  @Override
  protected void dumpProperties(StringBuilder buf, int width) {
    dumpBin(buf, width, "startBlock", startBlock, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "nlink", nlink, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "fileSize", fileSize, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "offset", offset, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "parentInodeNumber", parentInodeNumber, DECIMAL,
        UNSIGNED);
  }

  @Override
  public DirectoryINode simplify() {
    return this;
  }
}
