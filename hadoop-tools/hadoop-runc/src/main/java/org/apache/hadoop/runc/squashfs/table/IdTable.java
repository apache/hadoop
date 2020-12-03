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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.DECIMAL;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.UNSIGNED;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.dumpBin;

public class IdTable {

  public static final int ID_TABLE_RECORD_LENGTH = 8;
  public static final int BYTES_PER_TABLE_ENTRY = 4;
  public static final int ENTRIES_PER_BLOCK =
      MetadataBlock.MAX_SIZE / BYTES_PER_TABLE_ENTRY;
  private static final int[] EMPTY = new int[0];
  private final SortedMap<Long, Short> reverseMappings = new TreeMap<>();
  private int[] mappings = EMPTY;

  private static int numTables(int idCount) {
    return (idCount / ENTRIES_PER_BLOCK) + (
        ((idCount % ENTRIES_PER_BLOCK) == 0) ? 0 : 1);
  }

  public static IdTable read(int tag, TableReader tableReader,
      MetadataBlockReader metaBlockReader)
      throws IOException, SquashFsException {

    IdTable table = new IdTable();
    table.readData(tag, tableReader, metaBlockReader);
    return table;
  }

  public int getIdCount() {
    return mappings.length;
  }

  public int idFromIndex(short index) throws SquashFsException {
    int iIndex = (index & 0xffff);
    if (iIndex >= mappings.length) {
      throw new SquashFsException(
          String.format("No UID/GID could be found for id ref %d", iIndex));
    }
    return mappings[iIndex];
  }

  public short indexFromId(int id) throws SquashFsException {
    Long key = Long.valueOf(id % 0xFFFFFFFFL);
    Short value = reverseMappings.get(key);
    if (value == null) {
      throw new SquashFsException(
          String.format("No id ref could be found for UID/GID %d", key));
    }
    return value.shortValue();
  }

  public void readData(int tag, TableReader tableReader,
      MetadataBlockReader metaBlockReader)
      throws IOException, SquashFsException {

    reverseMappings.clear();

    SuperBlock sb = tableReader.getSuperBlock();
    int idCount = sb.getIdCount() & 0xffff;
    int tableCount = numTables(idCount);
    long[] tableRef = new long[tableCount];

    mappings = new int[idCount];

    ByteBuffer tableData = tableReader
        .read(sb.getIdTableStart(), tableCount * ID_TABLE_RECORD_LENGTH);
    for (int i = 0; i < tableCount; i++) {
      tableRef[i] = tableData.getLong();
    }

    MetadataReader reader = null;
    int table = 0;
    for (int i = 0; i < idCount; i++) {
      if ((i % ENTRIES_PER_BLOCK) == 0) {
        reader = metaBlockReader.rawReader(tag, tableRef[table++], (short) 0);
      }
      int id = reader.readInt();
      mappings[i] = id;
      Long key = Long.valueOf(id & 0xFFFFFFFFL);
      reverseMappings.put(key, Short.valueOf((short) (i & 0xffff)));
    }
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(String.format("id-table: {%n"));
    int width = 17;
    dumpBin(buf, width, "count", mappings.length, DECIMAL, UNSIGNED);
    for (int i = 0; i < mappings.length; i++) {
      dumpBin(buf, width, String.format("mappings[%d]", i), mappings[i],
          DECIMAL, UNSIGNED);
    }
    buf.append("}");
    return buf.toString();
  }

}
