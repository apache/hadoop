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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestFragmentTable {

  @Test
  public void readShouldHandleReadingEmptyTable() throws Exception {
    verify(0, true);
  }

  @Test
  public void readShouldHandleReadingSingleEntry() throws Exception {
    verify(1, true);
  }

  @Test
  public void readShouldHandleFragmentTableBeingDisabled() throws Exception {
    verify(1, false);
  }

  @Test
  public void readShouldHandleReadingSinglePage() throws Exception {
    verify(512, true);
  }

  @Test
  public void readShouldHandleReadingMultiplePages() throws Exception {
    verify(1024, true);
  }

  @Test
  public void toStringShouldNotFail() throws Exception {
    FragmentTable table = verify(10, true);
    System.out.println(table.toString());
  }

  @Test
  public void toStringShouldNotFailOnNotAvailable() throws Exception {
    FragmentTable table = verify(10, false);
    System.out.println(table.toString());
  }

  @Test(expected = SquashFsException.class)
  public void getEntryShouldFailOnTooLargeValue() throws Exception {
    FragmentTable table = verify(100, true);
    table.getEntry(100);
  }

  @Test(expected = SquashFsException.class)
  public void getEntryShouldFailOnTooSmallValue() throws Exception {
    FragmentTable table = verify(100, true);
    table.getEntry(-1);
  }

  FragmentTable verify(int count, boolean available) throws Exception {
    byte[] tableData;

    List<MetadataBlockRef> refs;
    byte[] metadata;

    SuperBlock sb = new SuperBlock();
    sb.setFragmentEntryCount(count);
    sb.setFragmentTableStart(SuperBlock.SIZE);
    if (!available) {
      sb.setFlags((short) (sb.getFlags() | SuperBlockFlag.NO_FRAGMENTS.mask()));
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

    FragmentTable ft = FragmentTable.read(tag, tr, mbr);
    if (available) {
      assertTrue("not available", ft.isAvailable());
      assertEquals(count, ft.getFragmentCount());
      for (int i = 0; i < count; i++) {
        FragmentTableEntry entry = ft.getEntry(i);
        assertNotNull(String.format("entry %d is null", i), entry);
        assertEquals(String.format("wrong start for entry %d", i),
            (long) (100_000 + i), entry.getStart());
        assertEquals(String.format("wrong size for entry %d", i), 10_000 + i,
            entry.getSize());
      }
    } else {
      assertFalse("available", ft.isAvailable());
      assertEquals(0, ft.getFragmentCount());
    }
    return ft;
  }

  List<MetadataBlockRef> createEntries(int count, DataOutput out)
      throws IOException {
    List<MetadataBlockRef> refs = new ArrayList<>();

    MetadataWriter writer = new MetadataWriter();
    for (int i = 0; i < count; i++) {
      if (i % 512 == 0) {
        writer.flush();
        refs.add(writer.getCurrentReference());
      }
      writer.writeLong(100_000 + i);
      writer.writeInt(10_000 + i);
      writer.writeInt(0);
    }
    writer.save(out);

    return refs;
  }

}
