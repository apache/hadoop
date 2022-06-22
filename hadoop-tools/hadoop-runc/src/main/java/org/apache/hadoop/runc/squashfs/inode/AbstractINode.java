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
import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.OCTAL;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.UNIX_TIMESTAMP;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.UNSIGNED;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.dumpBin;

abstract public class AbstractINode implements INode {

  private short permissions;
  private short uidIdx;
  private short gidIdx;
  private int modifiedTime;
  private int inodeNumber;

  @Override
  public final void copyTo(INode dest) {
    dest.setPermissions(permissions);
    dest.setUidIdx(uidIdx);
    dest.setGidIdx(gidIdx);
    dest.setModifiedTime(modifiedTime);
    dest.setInodeNumber(inodeNumber);
  }

  @Override
  public final int getSerializedSize() {
    return 16 + getChildSerializedSize();
  }

  @Override
  public short getPermissions() {
    return permissions;
  }

  @Override
  public void setPermissions(short permissions) {
    this.permissions = permissions;
  }

  @Override
  public short getUidIdx() {
    return uidIdx;
  }

  @Override
  public void setUidIdx(short uidIdx) {
    this.uidIdx = uidIdx;
  }

  @Override
  public short getGidIdx() {
    return gidIdx;
  }

  @Override
  public void setGidIdx(short gidIdx) {
    this.gidIdx = gidIdx;
  }

  @Override
  public int getModifiedTime() {
    return modifiedTime;
  }

  @Override
  public void setModifiedTime(int modifiedTime) {
    this.modifiedTime = modifiedTime;
  }

  @Override
  public int getInodeNumber() {
    return inodeNumber;
  }

  @Override
  public void setInodeNumber(int inodeNumber) {
    this.inodeNumber = inodeNumber;
  }

  abstract protected int getChildSerializedSize();

  @Override
  public final void readData(SuperBlock sb, DataInput in)
      throws SquashFsException, IOException {
    permissions = in.readShort();
    uidIdx = in.readShort();
    gidIdx = in.readShort();
    modifiedTime = in.readInt();
    inodeNumber = in.readInt();

    readExtraData(sb, in);
  }

  @Override
  public final void writeData(MetadataWriter out) throws IOException {
    out.writeShort(getInodeType().value());
    out.writeShort(permissions);
    out.writeShort(uidIdx);
    out.writeShort(gidIdx);
    out.writeInt(modifiedTime);
    out.writeInt(inodeNumber);

    writeExtraData(out);
  }

  abstract protected String getName();

  abstract public INodeType getInodeType();

  abstract protected void writeExtraData(MetadataWriter out) throws IOException;

  abstract protected void readExtraData(SuperBlock sb, DataInput in)
      throws SquashFsException, IOException;

  abstract protected int getPreferredDumpWidth();

  abstract protected void dumpProperties(StringBuilder buf, int width);

  @Override
  public String toString() {
    int width = Math.max(21, getPreferredDumpWidth());
    INodeType it = getInodeType();

    StringBuilder buf = new StringBuilder();
    buf.append(String.format("%s {%n", getName()));
    dumpBin(buf, width, "inodeType", it.value(), DECIMAL, UNSIGNED);
    dumpBin(buf, width, "inodeType (decoded)", it.name());
    dumpBin(buf, width, "permissions", permissions, OCTAL, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "uidIdx", uidIdx, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "gidIdx", gidIdx, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "modifiedTime", modifiedTime, DECIMAL, UNSIGNED,
        UNIX_TIMESTAMP);
    dumpBin(buf, width, "inodeNumber", inodeNumber, DECIMAL, UNSIGNED);
    dumpProperties(buf, width);
    buf.append("}");
    return buf.toString();
  }
}
