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

public class ExtendedDirectoryINode extends AbstractINode
    implements DirectoryINode {

  private int nlink = 1;
  private int fileSize; // 3 + # of uncompressed bytes in directory table
  private int startBlock;
  private int parentInodeNumber;
  private short indexCount;
  private short offset;
  private int xattrIndex = XATTR_NOT_PRESENT;

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
    return fileSize;
  }

  @Override
  public void setFileSize(int fileSize) {
    this.fileSize = fileSize;
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
    return indexCount;
  }

  @Override
  public void setIndexCount(short indexCount) {
    this.indexCount = indexCount;
  }

  @Override
  public boolean isIndexPresent() {
    return indexCount != (short) 0;
  }

  public int getXattrIndex() {
    return xattrIndex;
  }

  public void setXattrIndex(int xattrIndex) {
    this.xattrIndex = xattrIndex;
  }

  @Override
  public boolean isXattrPresent() {
    return xattrIndex != XATTR_NOT_PRESENT;
  }

  @Override
  protected int getChildSerializedSize() {
    return 24;
  }

  @Override
  protected String getName() {
    return "extended-directory-inode";
  }

  @Override
  public INodeType getInodeType() {
    return INodeType.EXTENDED_DIRECTORY;
  }

  @Override
  protected void readExtraData(SuperBlock sb, DataInput in)
      throws SquashFsException, IOException {
    nlink = in.readInt();
    fileSize = in.readInt();
    startBlock = in.readInt();
    parentInodeNumber = in.readInt();
    indexCount = in.readShort();
    offset = in.readShort();
    xattrIndex = in.readInt();
  }

  @Override
  protected void writeExtraData(MetadataWriter out) throws IOException {
    out.writeInt(nlink);
    out.writeInt(fileSize);
    out.writeInt(startBlock);
    out.writeInt(parentInodeNumber);
    out.writeShort(indexCount);
    out.writeShort(offset);
    out.writeInt(xattrIndex);
  }

  @Override
  protected int getPreferredDumpWidth() {
    return 19;
  }

  @Override
  protected void dumpProperties(StringBuilder buf, int width) {
    dumpBin(buf, width, "nlink", nlink, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "fileSize", fileSize, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "startBlock", startBlock, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "parentInodeNumber", parentInodeNumber, DECIMAL,
        UNSIGNED);
    dumpBin(buf, width, "indexCount", indexCount, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "offset", offset, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "xattrIndex", xattrIndex, DECIMAL, UNSIGNED);
  }

  @Override
  public DirectoryINode simplify() {
    return BasicDirectoryINode.simplify(this);
  }
}
