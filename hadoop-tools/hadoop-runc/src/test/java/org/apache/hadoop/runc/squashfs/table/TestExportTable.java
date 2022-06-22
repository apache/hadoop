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
import org.apache.hadoop.runc.squashfs.superblock.SuperBlockFlag;
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

public class TestExportTable {

  @Test
  public void readShouldHandleReadingEmptyTable() throws Exception {
    verify(0, true);
  }

  @Test
  public void readShouldHandleReadingSingleEntry() throws Exception {
    verify(1, true);
  }

  @Test
  public void readShouldHandleExportTableBeingDisabled() throws Exception {
    verify(1, false);
  }

  @Test
  public void readShouldHandleReadingSinglePage() throws Exception {
    verify(1024, true);
  }

  @Test
  public void readShouldHandleReadingMultiplePages() throws Exception {
    verify(2048, true);
  }

  @Test
  public void toStringShouldNotFail() throws Exception {
    ExportTable table = verify(10, true);
    System.out.println(table.toString());
  }

  @Test
  public void toStringShouldNotFailOnNotAvailable() throws Exception {
    ExportTable table = verify(10, false);
    System.out.println(table.toString());
  }

  @Test(expected = SquashFsException.class)
  public void getInodeRefRawShouldFailOnTooLargeValue() throws Exception {
    ExportTable table = verify(100, true);
    table.getInodeRefRaw(101);
  }

  @Test(expected = SquashFsException.class)
  public void getInodeRefRawShouldFailOnTooSmallValue() throws Exception {
    ExportTable table = verify(100, true);
    table.getInodeRefRaw(0);
  }

  @Test(expected = SquashFsException.class)
  public void getInodeRefShouldFailOnTooLargeValue() throws Exception {
    ExportTable table = verify(100, true);
    table.getInodeRef(101);
  }

  @Test(expected = SquashFsException.class)
  public void getInodeRefShouldFailOnTooSmallValue() throws Exception {
    ExportTable table = verify(100, true);
    table.getInodeRef(0);
  }

  @Test
  public void getInodeRefShouldReturnSameValueAsRaw() throws Exception {
    ExportTable table = verify(100, true);
    assertEquals("wrong ref value", table.getInodeRefRaw(1),
        table.getInodeRef(1).getRaw());
  }

  ExportTable verify(int count, boolean available) throws Exception {
    byte[] tableData;

    List<MetadataBlockRef> refs;
    byte[] metadata;

    SuperBlock sb = new SuperBlock();
    sb.setInodeCount(count);
    sb.setExportTableStart(SuperBlock.SIZE);
    if (!available) {
      sb.setFlags(SuperBlockFlag.DUPLICATES.mask());
    }

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

    ExportTable et = ExportTable.read(tag, tr, mbr);
    assertEquals("available status", available, et.isAvailable());
    if (available) {
      assertEquals("wrong inode count", count, et.getInodeCount());
      for (int i = 0; i < count; i++) {
        assertEquals(String.format("wrong value of id %d", i + 1),
            (long) (100000 + i), et.getInodeRefRaw(i + 1));
      }
    } else {
      assertEquals("wrong count", 0, et.getInodeCount());
    }
    return et;
  }

  List<MetadataBlockRef> createEntries(int count, DataOutput out)
      throws IOException {
    List<MetadataBlockRef> refs = new ArrayList<>();

    MetadataWriter writer = new MetadataWriter();
    for (int i = 0; i < count; i++) {
      if (i % 1024 == 0) {
        refs.add(writer.getCurrentReference());
      }
      writer.writeLong(100_000 + i);
    }
    writer.save(out);

    return refs;
  }

}
