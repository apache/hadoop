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
import org.apache.hadoop.runc.squashfs.metadata.MemoryMetadataBlockReader;
import org.apache.hadoop.runc.squashfs.metadata.MetadataBlockReader;
import org.apache.hadoop.runc.squashfs.metadata.MetadataBlockRef;
import org.apache.hadoop.runc.squashfs.metadata.MetadataWriter;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestIdTable {

  @Test
  public void readShouldHandleReadingEmptyTable() throws Exception {
    verify(0);
  }

  @Test
  public void readShouldHandleReadingSingleEntry() throws Exception {
    verify(1);
  }

  @Test
  public void readShouldHandleReadingSinglePage() throws Exception {
    verify(2048);
  }

  @Test
  public void readShouldHandleReadingFullTable() throws Exception {
    verify(65535);
  }

  @Test(expected = SquashFsException.class)
  public void idFromIndexShouldFailOnOutOfRangeIndex() throws Exception {
    IdTable table = verify(100);
    table.idFromIndex((short) 100);
  }

  @Test
  public void toStringShouldNotFail() throws Exception {
    IdTable table = verify(10);
    System.out.println(table.toString());
  }

  @Test(expected = SquashFsException.class)
  public void indexFromIdShouldFailOnUnknownValue() throws Exception {
    IdTable table = verify(100);
    table.indexFromId(100_100);
  }

  IdTable verify(int count) throws Exception {
    byte[] tableData;

    List<MetadataBlockRef> refs;
    byte[] metadata;

    SuperBlock sb = new SuperBlock();
    sb.setIdCount((short) (count & 0xffff));
    sb.setIdTableStart(SuperBlock.SIZE);

    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      try (DataOutputStream dos = new DataOutputStream(bos)) {
        refs = createEntries(count, dos);
      }
      metadata = bos.toByteArray();
    }

    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      try (DataOutputStream dos = new DataOutputStream(bos)) {
        sb.writeData(dos);
        for (MetadataBlockRef ref : refs) {
          byte[] buf = new byte[8];
          ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN)
              .putLong(ref.getLocation());
          dos.write(buf);
        }
      }
      tableData = bos.toByteArray();
    }

    int tag = 0;
    TableReader tr = new MemoryTableReader(sb, tableData);
    MetadataBlockReader mbr = new MemoryMetadataBlockReader(tag, sb, metadata);

    IdTable idt = IdTable.read(tag, tr, mbr);
    assertEquals(count, idt.getIdCount());
    for (int i = 0; i < count; i++) {
      assertEquals((short) (i & 0xffff), idt.indexFromId(100000 + i));
      assertEquals(100000 + i, idt.idFromIndex((short) i));
    }
    return idt;
  }

  List<MetadataBlockRef> createEntries(int count, DataOutput out)
      throws IOException {
    List<MetadataBlockRef> refs = new ArrayList<>();

    MetadataWriter writer = new MetadataWriter();
    for (int i = 0; i < count; i++) {
      if (i % 2048 == 0) {
        refs.add(writer.getCurrentReference());
      }
      writer.writeInt(100_000 + i);
    }
    writer.save(out);

    return refs;
  }
}
