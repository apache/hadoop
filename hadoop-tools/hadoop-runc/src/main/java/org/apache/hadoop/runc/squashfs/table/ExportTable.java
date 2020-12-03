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
import org.apache.hadoop.runc.squashfs.inode.INodeRef;
import org.apache.hadoop.runc.squashfs.metadata.MetadataBlock;
import org.apache.hadoop.runc.squashfs.metadata.MetadataBlockReader;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlockFlag;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.DECIMAL;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.UNSIGNED;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.dumpBin;

public class ExportTable {

  public static final int EXPORT_TABLE_RECORD_LENGTH = 8;
  public static final int BYTES_PER_TABLE_ENTRY = 8;
  public static final int ENTRIES_PER_BLOCK =
      MetadataBlock.MAX_SIZE / BYTES_PER_TABLE_ENTRY;

  private static final long[] EMPTY = new long[0];

  private MetadataBlockReader mbReader;

  private boolean available = false;
  private int ourTag = -1;
  private int inodeCount = 0;
  private long[] tableRef = EMPTY;

  private static int numTables(int inodeCount) {
    return (inodeCount / ENTRIES_PER_BLOCK) + (
        ((inodeCount % ENTRIES_PER_BLOCK) == 0) ? 0 : 1);
  }

  public static ExportTable read(int tag, TableReader tableReader,
      MetadataBlockReader metaBlockReader)
      throws IOException, SquashFsException {

    ExportTable table = new ExportTable();
    table.readData(tag, tableReader, metaBlockReader);
    return table;
  }

  public int getInodeCount() {
    return inodeCount;
  }

  public boolean isAvailable() {
    return available;
  }

  public INodeRef getInodeRef(int inode) throws IOException, SquashFsException {
    return new INodeRef(getInodeRefRaw(inode));
  }

  public long getInodeRefRaw(int inode) throws IOException, SquashFsException {
    if (inode < 1 || inode > inodeCount) {
      throw new SquashFsException(String.format("No such inode %d", inode));
    }

    int nInode = (inode - 1);
    int blockNum = nInode / ENTRIES_PER_BLOCK;
    short offset = (short) (BYTES_PER_TABLE_ENTRY * (nInode - (blockNum
        * ENTRIES_PER_BLOCK)));

    return mbReader.rawReader(ourTag, tableRef[blockNum], offset)
        .readLong();
  }

  public void readData(int tag, TableReader tableReader,
      MetadataBlockReader metaBlockReader)
      throws IOException, SquashFsException {

    this.ourTag = tag;

    SuperBlock sb = tableReader.getSuperBlock();
    if (!sb.hasFlag(SuperBlockFlag.EXPORTABLE)) {
      available = false;
      inodeCount = 0;
      tableRef = EMPTY;
      this.mbReader = null;
      return;
    }

    inodeCount = sb.getInodeCount();
    int tableCount = numTables(inodeCount);
    tableRef = new long[tableCount];

    ByteBuffer tableData = tableReader.read(sb.getExportTableStart(),
        tableCount * EXPORT_TABLE_RECORD_LENGTH);
    for (int i = 0; i < tableCount; i++) {
      tableRef[i] = tableData.getLong();
    }
    this.mbReader = metaBlockReader;
    available = true;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(String.format("export-table: {%n"));
    int width = 18;
    dumpBin(buf, width, "available", available ? "true" : "false");
    dumpBin(buf, width, "inodeCount", inodeCount, DECIMAL, UNSIGNED);
    dumpBin(buf, width, "tableRefs", tableRef.length, DECIMAL);
    for (int i = 0; i < tableRef.length; i++) {
      dumpBin(buf, width, String.format("tableRef[%d]", i), tableRef[i],
          DECIMAL, UNSIGNED);
    }
    buf.append("}");
    return buf.toString();
  }
}
