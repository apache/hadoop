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

import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.DECIMAL;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.UNSIGNED;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.dumpBin;

public class DirectoryHeader implements DirectoryElement {

  public static final short MAX_DIR_ENTRIES = 256;

  private int count; // number of entries (1 less than actual length)
  private int startBlock; // starting inode block
  private int inodeNumber; // starting inode number

  public int getCount() {
    return count;
  }

  void incrementCount() {
    count++;
  }

  public int getStartBlock() {
    return startBlock;
  }

  public int getInodeNumber() {
    return inodeNumber;
  }

  public int getStructureSize() {
    return 12;
  }

  DirectoryHeader() {
  }

  DirectoryHeader(int count, int startBlock, int inodeNumber) {
    this.count = count;
    this.startBlock = startBlock;
    this.inodeNumber = inodeNumber;
  }

  public static DirectoryHeader read(DataInput in)
      throws SquashFsException, IOException {
    DirectoryHeader entry = new DirectoryHeader();
    entry.readData(in);
    return entry;
  }

  public void readData(DataInput in) throws SquashFsException, IOException {
    count = in.readInt();
    startBlock = in.readInt();
    inodeNumber = in.readInt();
    if (count + 1 > MAX_DIR_ENTRIES) {
      throw new SquashFsException(String
          .format("Invalid directory header: found %d entries (max = %d)%n%s",
              count + 1, MAX_DIR_ENTRIES, this));
    }
  }

  @Override
  public void writeData(DataOutput out) throws IOException {
    out.writeInt(count);
    out.writeInt(startBlock);
    out.writeInt(inodeNumber);
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(String.format("directory-header {%n"));
    int width = 13;
    dumpBin(buf, width, "count", count, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "startBlock", startBlock, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "inodeNumber", inodeNumber, DECIMAL, UNSIGNED);
    buf.append("}");
    return buf.toString();
  }

}
