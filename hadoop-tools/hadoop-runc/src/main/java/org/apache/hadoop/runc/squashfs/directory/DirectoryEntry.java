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

package org.apache.hadoop.runc.squashfs.directory;

import org.apache.hadoop.runc.squashfs.SquashFsException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.DECIMAL;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.UNSIGNED;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.dumpBin;

public class DirectoryEntry implements DirectoryElement {

  private static final byte[] EMPTY = new byte[0];

  public static final short MAX_FILENAME_LENGTH = 256;

  private DirectoryHeader header;

  private short offset; // offset into inode block where data starts
  private short inodeNumberDelta; // amount to add to header inodeNumber
  private short type; // inode type
  private short size; // size of name (1 less than actual size)
  private byte[] name = EMPTY; // filename (not null terminated)

  DirectoryEntry() {
  }

  DirectoryEntry(short offset, short inodeNumberDelta, short type, short size,
      byte[] name, DirectoryHeader header) {
    this.offset = offset;
    this.inodeNumberDelta = inodeNumberDelta;
    this.type = type;
    this.size = size;
    this.name = name;
    this.header = header;
  }

  public DirectoryHeader getHeader() {
    return header;
  }

  public short getOffset() {
    return offset;
  }

  public short getInodeNumberDelta() {
    return inodeNumberDelta;
  }

  public short getType() {
    return type;
  }

  public short getSize() {
    return size;
  }

  public byte[] getName() {
    return name;
  }

  public String getNameAsString() {
    return new String(name, StandardCharsets.ISO_8859_1);
  }

  public int getStructureSize() {
    return 8 + name.length;
  }

  public static DirectoryEntry read(DirectoryHeader header, DataInput in)
      throws SquashFsException, IOException {
    DirectoryEntry entry = new DirectoryEntry();
    entry.readData(header, in);
    return entry;
  }

  public void readData(DirectoryHeader directoryHeader, DataInput in)
      throws SquashFsException, IOException {
    this.header = directoryHeader;
    offset = in.readShort();
    inodeNumberDelta = in.readShort();
    type = in.readShort();
    size = (short) (in.readShort() & 0x7fff);
    if (size + 1 > MAX_FILENAME_LENGTH) {
      throw new SquashFsException(String.format(
          "Invalid directory entry: Found filename of length %d (max = %d)%n%s",
          size + 1,
          MAX_FILENAME_LENGTH,
          this));
    }
    name = new byte[size + 1];
    in.readFully(name);
  }

  @Override
  public void writeData(DataOutput out) throws IOException {
    out.writeShort(offset);
    out.writeShort(inodeNumberDelta);
    out.writeShort(type);
    out.writeShort(size);
    out.write(name);
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(String.format("directory-entry {%n"));
    int width = 18;
    dumpBin(buf, width, "offset", offset, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "inodeNumberDelta", inodeNumberDelta, DECIMAL,
        UNSIGNED);
    dumpBin(buf, width, "type", type, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "size", size, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "name", name, 0, name.length, 16, 2);
    buf.append("}");
    return buf.toString();
  }

}
