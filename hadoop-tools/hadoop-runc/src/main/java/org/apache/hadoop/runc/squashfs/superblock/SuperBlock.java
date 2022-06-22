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

package org.apache.hadoop.runc.squashfs.superblock;

import org.apache.hadoop.runc.squashfs.SquashFsException;
import org.apache.hadoop.runc.squashfs.inode.INodeRef;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.BINARY;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.DECIMAL;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.UNIX_TIMESTAMP;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.UNSIGNED;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.dumpBin;

public class SuperBlock {

  public static final int SIZE = 96;
  public static final int SQUASHFS_MAGIC = 0x73717368;

  public static final short DEFAULT_VERSION_MAJOR = 4;
  public static final short DEFUALT_VERSION_MINOR = 0;

  public static final short DEFAULT_BLOCK_LOG = 17;
  public static final int DEFAULT_BLOCK_SIZE = 1 << DEFAULT_BLOCK_LOG;

  public static final short DEFAULT_FLAGS = SuperBlockFlag.flagsFor(
      SuperBlockFlag.DUPLICATES, SuperBlockFlag.EXPORTABLE);

  public static final long TABLE_NOT_PRESENT = 0xffff_ffff_ffff_ffffL;

  private int inodeCount;
  private int modificationTime = (int) (System.currentTimeMillis() / 1000);
  private int blockSize = DEFAULT_BLOCK_SIZE;
  private int fragmentEntryCount;
  private CompressionId compressionId = CompressionId.ZLIB;
  private short blockLog = DEFAULT_BLOCK_LOG;
  private short flags = DEFAULT_FLAGS;
  private short idCount;
  private short versionMajor = DEFAULT_VERSION_MAJOR;
  private short versionMinor = DEFUALT_VERSION_MINOR;
  private long rootInodeRef;
  private long bytesUsed;
  private long idTableStart;
  private long xattrIdTableStart = TABLE_NOT_PRESENT;
  private long inodeTableStart;
  private long directoryTableStart;
  private long fragmentTableStart;
  private long exportTableStart;

  public static SuperBlock read(DataInput in)
      throws IOException, SquashFsException {
    SuperBlock block = new SuperBlock();
    block.readData(in);
    return block;
  }

  public int getInodeCount() {
    return inodeCount;
  }

  public void setInodeCount(int inodeCount) {
    this.inodeCount = inodeCount;
  }

  public int getModificationTime() {
    return modificationTime;
  }

  public void setModificationTime(int modificationTime) {
    this.modificationTime = modificationTime;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public void setBlockSize(int blockSize) {
    this.blockSize = blockSize;
  }

  public int getFragmentEntryCount() {
    return fragmentEntryCount;
  }

  public void setFragmentEntryCount(int fragmentEntryCount) {
    this.fragmentEntryCount = fragmentEntryCount;
  }

  public CompressionId getCompressionId() {
    return compressionId;
  }

  public void setCompressionId(CompressionId compressionId) {
    this.compressionId = compressionId;
  }

  public short getBlockLog() {
    return blockLog;
  }

  public void setBlockLog(short blockLog) {
    this.blockLog = blockLog;
  }

  public short getFlags() {
    return flags;
  }

  public void setFlags(short flags) {
    this.flags = flags;
  }

  public short getIdCount() {
    return idCount;
  }

  public void setIdCount(short idCount) {
    this.idCount = idCount;
  }

  public short getVersionMajor() {
    return versionMajor;
  }

  public void setVersionMajor(short versionMajor) {
    this.versionMajor = versionMajor;
  }

  public short getVersionMinor() {
    return versionMinor;
  }

  public void setVersionMinor(short versionMinor) {
    this.versionMinor = versionMinor;
  }

  public long getRootInodeRef() {
    return rootInodeRef;
  }

  public void setRootInodeRef(long rootInodeRef) {
    this.rootInodeRef = rootInodeRef;
  }

  public long getBytesUsed() {
    return bytesUsed;
  }

  public void setBytesUsed(long bytesUsed) {
    this.bytesUsed = bytesUsed;
  }

  public long getIdTableStart() {
    return idTableStart;
  }

  public void setIdTableStart(long idTableStart) {
    this.idTableStart = idTableStart;
  }

  public long getXattrIdTableStart() {
    return xattrIdTableStart;
  }

  public void setXattrIdTableStart(long xattrIdTableStart) {
    this.xattrIdTableStart = xattrIdTableStart;
  }

  public long getInodeTableStart() {
    return inodeTableStart;
  }

  public void setInodeTableStart(long inodeTableStart) {
    this.inodeTableStart = inodeTableStart;
  }

  public long getDirectoryTableStart() {
    return directoryTableStart;
  }

  public void setDirectoryTableStart(long directoryTableStart) {
    this.directoryTableStart = directoryTableStart;
  }

  public long getFragmentTableStart() {
    return fragmentTableStart;
  }

  public void setFragmentTableStart(long fragmentTableStart) {
    this.fragmentTableStart = fragmentTableStart;
  }

  public long getExportTableStart() {
    return exportTableStart;
  }

  public void setExportTableStart(long exportTableStart) {
    this.exportTableStart = exportTableStart;
  }

  public boolean hasFlag(SuperBlockFlag flag) {
    return flag.isSet(flags);
  }

  public void writeData(DataOutput out) throws IOException {
    byte[] raw = new byte[SIZE];
    ByteBuffer buffer = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(SQUASHFS_MAGIC);
    buffer.putInt(inodeCount);
    buffer.putInt(modificationTime);
    buffer.putInt(blockSize);
    buffer.putInt(fragmentEntryCount);
    buffer.putShort(compressionId.value());
    buffer.putShort(blockLog);
    buffer.putShort(flags);
    buffer.putShort(idCount);
    buffer.putShort(versionMajor);
    buffer.putShort(versionMinor);
    buffer.putLong(rootInodeRef);
    buffer.putLong(bytesUsed);
    buffer.putLong(idTableStart);
    buffer.putLong(xattrIdTableStart);
    buffer.putLong(inodeTableStart);
    buffer.putLong(directoryTableStart);
    buffer.putLong(fragmentTableStart);
    buffer.putLong(exportTableStart);
    out.write(raw);
  }

  public void readData(DataInput in) throws IOException, SquashFsException {
    byte[] raw = new byte[SIZE];
    in.readFully(raw);
    ByteBuffer buffer = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);

    int magic = buffer.getInt();
    if (magic != SQUASHFS_MAGIC) {
      throw new SquashFsException(
          String.format("Unknown magic %8x found", magic));
    }

    inodeCount = buffer.getInt();
    modificationTime = buffer.getInt();
    blockSize = buffer.getInt();
    fragmentEntryCount = buffer.getInt();
    compressionId = CompressionId.fromValue(buffer.getShort());
    blockLog = buffer.getShort();
    int expectedBlockSize = 1 << blockLog;
    if (blockSize != expectedBlockSize) {
      throw new SquashFsException(
          String.format("Corrupt archive, expected block size %d, got %d",
              expectedBlockSize, blockSize));
    }
    flags = buffer.getShort();
    idCount = buffer.getShort();
    versionMajor = buffer.getShort();
    versionMinor = buffer.getShort();

    String version = String.format("%d.%d", versionMajor, versionMinor);
    if (!("4.0".equals(version))) {
      throw new SquashFsException(
          String.format("Unknown version %s found", version));
    }

    rootInodeRef = buffer.getLong();
    bytesUsed = buffer.getLong();
    idTableStart = buffer.getLong();
    xattrIdTableStart = buffer.getLong();
    inodeTableStart = buffer.getLong();
    directoryTableStart = buffer.getLong();
    fragmentTableStart = buffer.getLong();
    exportTableStart = buffer.getLong();
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(String.format("superblock: {%n"));
    int width = 25;
    dumpBin(buf, width, "inodeCount", inodeCount, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "modificationTime", modificationTime, DECIMAL,
        UNIX_TIMESTAMP, UNSIGNED);
    dumpBin(buf, width, "blockSize", blockSize, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "fragmentEntryCount", fragmentEntryCount, DECIMAL,
        UNSIGNED);
    dumpBin(buf, width, "compressionId", compressionId.value(), BINARY,
        UNSIGNED);
    dumpBin(buf, width, "compressionId (decoded)", compressionId.toString());
    dumpBin(buf, width, "blockLog", blockLog, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "blockSize (calculated)", (int) 1 << blockLog, DECIMAL,
        UNSIGNED);
    dumpBin(buf, width, "flags", flags, BINARY, UNSIGNED);
    dumpBin(buf, width, "flags (decoded)",
        SuperBlockFlag.flagsPresent(flags).toString());
    dumpBin(buf, width, "idCount", idCount, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "versionMajor", versionMajor, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "versionMinor", versionMinor, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "version (decoded)",
        String.format("%d.%d", versionMajor, versionMinor));
    dumpBin(buf, width, "rootInodeRef", rootInodeRef, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "rootInodeRef (decoded)",
        new INodeRef(rootInodeRef).toString());
    dumpBin(buf, width, "bytesUsed", bytesUsed, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "idTableStart", idTableStart, DECIMAL);
    dumpBin(buf, width, "xattrIdTableStart", xattrIdTableStart, DECIMAL);
    dumpBin(buf, width, "inodeTableStart", inodeTableStart, DECIMAL);
    dumpBin(buf, width, "directoryTableStart", directoryTableStart, DECIMAL);
    dumpBin(buf, width, "fragmentTableStart", fragmentTableStart, DECIMAL);
    dumpBin(buf, width, "exportTableStart", exportTableStart, DECIMAL);
    buf.append("}");
    return buf.toString();
  }
}
