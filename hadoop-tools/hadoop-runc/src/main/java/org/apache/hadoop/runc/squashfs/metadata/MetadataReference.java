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

package org.apache.hadoop.runc.squashfs.metadata;

import org.apache.hadoop.runc.squashfs.SquashFsException;
import org.apache.hadoop.runc.squashfs.directory.DirectoryEntry;
import org.apache.hadoop.runc.squashfs.inode.DirectoryINode;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;

import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.DECIMAL;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.UNSIGNED;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.dumpBin;

public class MetadataReference {

  private final int tag;
  private final long blockLocation;
  private final short offset;
  private final int maxLength;

  MetadataReference(int tag, long blockLocation, short offset, int maxLength) {
    this.tag = tag;
    this.blockLocation = blockLocation;
    this.offset = offset;
    this.maxLength = maxLength;
  }

  public static MetadataReference inode(int tag, SuperBlock sb, long inodeRef)
      throws SquashFsException {
    long inodeBlockRel = (inodeRef & 0x0000ffffffff0000L) >> 16;
    long inodeBlock = sb.getInodeTableStart() + inodeBlockRel;

    short inodeOffset = (short) (inodeRef & 0x7fff);
    if (inodeOffset >= MetadataBlock.MAX_SIZE) {
      throw new SquashFsException(
          String.format("Invalid inode reference with offset %d (max = %d",
              inodeOffset, MetadataBlock.MAX_SIZE - 1));
    }

    return new MetadataReference(tag, inodeBlock, inodeOffset,
        Integer.MAX_VALUE);
  }

  public static MetadataReference inode(int tag, SuperBlock sb,
      DirectoryEntry dirEnt)
      throws SquashFsException {

    long inodeBlockRel = (dirEnt.getHeader().getStartBlock() & 0xffffffffL);
    return inode(tag, sb, inodeBlockRel, dirEnt.getOffset());
  }

  private static MetadataReference inode(int tag, SuperBlock sb,
      long inodeBlockRel, short inodeOffset)
      throws SquashFsException {

    long inodeBlock = sb.getInodeTableStart() + inodeBlockRel;
    inodeOffset = (short) (inodeOffset & 0x7fff);

    if (inodeOffset >= MetadataBlock.MAX_SIZE) {
      throw new SquashFsException(
          String.format("Invalid inode reference with offset %d (max = %d",
              inodeOffset, MetadataBlock.MAX_SIZE - 1));
    }

    return new MetadataReference(tag, inodeBlock, inodeOffset,
        Integer.MAX_VALUE);
  }

  public static MetadataReference raw(int tag, long blockLocation, short offset)
      throws SquashFsException {

    offset = (short) (offset & 0x7fff);

    if (offset >= MetadataBlock.MAX_SIZE) {
      throw new SquashFsException(
          String.format("Invalid raw reference with offset %d (max = %d",
              offset, MetadataBlock.MAX_SIZE - 1));
    }

    return new MetadataReference(tag, blockLocation, offset, Integer.MAX_VALUE);
  }

  public static MetadataReference directory(int tag, SuperBlock sb,
      DirectoryINode dir) throws SquashFsException {
    long dirBlockRel = dir.getStartBlock() & 0xffffffffL;
    long dirBlock = sb.getDirectoryTableStart() + dirBlockRel;
    short dirOffset = (short) (dir.getOffset() & 0x7fff);

    if (dirOffset >= MetadataBlock.MAX_SIZE) {
      throw new SquashFsException(String
          .format("Invalid directory table reference with offset %d (max = %d",
              dirOffset, MetadataBlock.MAX_SIZE - 1));
    }

    return new MetadataReference(tag, dirBlock, dirOffset,
        dir.getFileSize() - 3);
  }

  public int getTag() {
    return tag;
  }

  public long getBlockLocation() {
    return blockLocation;
  }

  public short getOffset() {
    return offset;
  }

  public int getMaxLength() {
    return maxLength;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(String.format("metadata-reference: {%n"));
    int width = 22;
    dumpBin(buf, width, "tag", tag, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "blockLocation", blockLocation, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "offset", offset, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "maxLength", maxLength, DECIMAL, UNSIGNED);
    buf.append("}");
    return buf.toString();
  }
}
