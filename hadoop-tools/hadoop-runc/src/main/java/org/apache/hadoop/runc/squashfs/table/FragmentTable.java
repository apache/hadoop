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

package org.apache.hadoop.runc.squashfs.table;

import org.apache.hadoop.runc.squashfs.SquashFsException;
import org.apache.hadoop.runc.squashfs.metadata.MetadataBlock;
import org.apache.hadoop.runc.squashfs.metadata.MetadataBlockReader;
import org.apache.hadoop.runc.squashfs.metadata.MetadataReader;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlockFlag;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.DECIMAL;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.UNSIGNED;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.dumpBin;

public class FragmentTable {

  public static final int FRAGMENT_TABLE_RECORD_LENGTH = 8;
  public static final int BYTES_PER_TABLE_ENTRY = 16;
  public static final int ENTRIES_PER_BLOCK =
      MetadataBlock.MAX_SIZE / BYTES_PER_TABLE_ENTRY;

  private static final long[] EMPTY = new long[0];

  private MetadataBlockReader mbReader;

  private boolean available = false;
  private int ourTag = -1;
  private int fragmentCount = 0;
  private long[] tableRef = EMPTY;

  private static int numTables(int inodeCount) {
    return (inodeCount / ENTRIES_PER_BLOCK) + (
        ((inodeCount % ENTRIES_PER_BLOCK) == 0) ? 0 : 1);
  }

  public static FragmentTable read(int tag, TableReader tableReader,
      MetadataBlockReader metaBlockReader)
      throws IOException, SquashFsException {

    FragmentTable table = new FragmentTable();
    table.readData(tag, tableReader, metaBlockReader);
    return table;
  }

  public int getFragmentCount() {
    return fragmentCount;
  }

  public boolean isAvailable() {
    return available;
  }

  public FragmentTableEntry getEntry(int id)
      throws IOException, SquashFsException {
    if (id < 0 || id >= fragmentCount) {
      throw new SquashFsException(String.format("No such fragment %d", id));
    }

    int blockNum = id / ENTRIES_PER_BLOCK;
    short offset =
        (short) (BYTES_PER_TABLE_ENTRY * (id - (blockNum * ENTRIES_PER_BLOCK)));

    MetadataReader reader =
        mbReader.rawReader(ourTag, tableRef[blockNum], offset);

    long start = reader.readLong();
    int size = reader.readInt();
    reader.readInt(); // unused

    return new FragmentTableEntry(start, size);
  }

  public void readData(int tag, TableReader tableReader,
      MetadataBlockReader metaBlockReader)
      throws IOException, SquashFsException {

    this.ourTag = tag;
    SuperBlock sb = tableReader.getSuperBlock();
    if (sb.hasFlag(SuperBlockFlag.NO_FRAGMENTS)) {
      available = false;
      fragmentCount = 0;
      tableRef = EMPTY;
      this.mbReader = null;
      return;
    }

    fragmentCount = sb.getFragmentEntryCount();
    int tableCount = numTables(fragmentCount);
    tableRef = new long[tableCount];

    ByteBuffer tableData = tableReader.read(sb.getFragmentTableStart(),
        tableCount * FRAGMENT_TABLE_RECORD_LENGTH);
    for (int i = 0; i < tableCount; i++) {
      tableRef[i] = tableData.getLong();
    }
    this.mbReader = metaBlockReader;
    available = true;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(String.format("fragment-table: {%n"));
    int width = 18;
    dumpBin(buf, width, "tag", ourTag, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "available", available ? "true" : "false");
    dumpBin(buf, width, "fragmentCount", fragmentCount, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "tableRefs", tableRef.length, DECIMAL);
    for (int i = 0; i < tableRef.length; i++) {
      dumpBin(buf, width, String.format("tableRef[%d]", i), tableRef[i],
          DECIMAL, UNSIGNED);
    }
    buf.append("}");
    return buf.toString();
  }
}
