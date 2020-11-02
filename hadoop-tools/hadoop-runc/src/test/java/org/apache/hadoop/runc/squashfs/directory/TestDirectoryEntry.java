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
import org.apache.hadoop.runc.squashfs.test.DirectoryTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public class TestDirectoryEntry {

  DirectoryEntry entry;

  @Before
  public void setUp() {
    DirectoryHeader hdr = new DirectoryHeader();
    hdr.count = 0;
    hdr.startBlock = 1;
    hdr.inodeNumber = 2;

    entry = new DirectoryEntry();
    entry.header = hdr;
    entry.offset = (short) 3;
    entry.inodeNumberDelta = (short) 4;
    entry.type = (short) 5;
    entry.name = "test".getBytes(StandardCharsets.ISO_8859_1);
    entry.size = (short) (entry.name.length - 1);
  }

  @Test
  public void headerPropertyWorksAsExpected() {
    assertNotNull(entry.getHeader());
    DirectoryHeader hdr2 = new DirectoryHeader();
    entry.header = hdr2;
    assertSame(hdr2, entry.getHeader());
  }

  @Test
  public void offsetPropertyWorksAsExpected() {
    assertEquals((short) 3, entry.getOffset());
    entry.offset = (short) 4;
    assertEquals((short) 4, entry.getOffset());
  }

  @Test
  public void inodeNumberDeltaPropertyWorksAsExpected() {
    assertEquals(4, entry.getInodeNumberDelta());
    entry.inodeNumberDelta = 5;
    assertEquals(5, entry.getInodeNumberDelta());
  }

  @Test
  public void typePropertyWorksAsExpected() {
    assertEquals((short) 5, entry.getType());
    entry.type = (short) 6;
    assertEquals((short) 6, entry.getType());
  }

  @Test
  public void sizePropertyWorksAsExpected() {
    assertEquals((short) 3, entry.getSize());
    entry.size = (short) 4;
    assertEquals((short) 4, entry.getSize());
  }

  @Test
  public void namePropertyWorksAsExpected() {
    assertEquals("test",
        new String(entry.getName(), StandardCharsets.ISO_8859_1));
    entry.name = "test2".getBytes(StandardCharsets.ISO_8859_1);
    assertEquals("test2",
        new String(entry.getName(), StandardCharsets.ISO_8859_1));
  }

  @Test
  public void nameAsStringPropertyWorksAsExpected() {
    assertEquals("test", entry.getNameAsString());
    entry.name = "test2".getBytes(StandardCharsets.ISO_8859_1);
    assertEquals("test2", entry.getNameAsString());
  }

  @Test
  public void getStructureSizeReturnsCorrectValue() {
    assertEquals(12, entry.getStructureSize());
    entry.name = "test2".getBytes(StandardCharsets.ISO_8859_1);
    assertEquals(13, entry.getStructureSize());
  }

  @Test
  public void readShouldSucceed() throws Exception {
    byte[] buf = new byte[12];
    ByteBuffer bb = ByteBuffer.wrap(buf);
    bb.putShort((short) 3); // offset
    bb.putShort((short) 4); // inode number delta
    bb.putShort((short) 5); // type
    bb.putShort((short) 3); // size
    bb.put("test".getBytes(StandardCharsets.ISO_8859_1));

    try (ByteArrayInputStream bis = new ByteArrayInputStream(buf)) {
      try (DataInputStream dis = new DataInputStream(bis)) {
        DirectoryEntry dest = DirectoryEntry.read(entry.header, dis);
        assertSame("wrong header", entry.header, dest.header);
        assertEquals("wrong offset", (short) 3, dest.getOffset());
        assertEquals("wrong inode number delta", (short) 4,
            dest.getInodeNumberDelta());
        assertEquals("wrong offset", (short) 5, dest.getType());
        assertEquals("wrong size", (short) 3, dest.getSize());
        assertEquals("wrong name", "test",
            new String(dest.getName(), StandardCharsets.ISO_8859_1));
      }
    }
  }

  @Test(expected = SquashFsException.class)
  public void readShouldFailIfSizeIsTooLarge() throws Exception {
    byte[] buf = new byte[261];
    ByteBuffer bb = ByteBuffer.wrap(buf);
    bb.putShort((short) 3); // offset
    bb.putShort((short) 4); // inode number delta
    bb.putShort((short) 5); // type
    bb.putShort((short) 256); // size
    bb.put("test".getBytes(StandardCharsets.ISO_8859_1));

    try (ByteArrayInputStream bis = new ByteArrayInputStream(buf)) {
      try (DataInputStream dis = new DataInputStream(bis)) {
        DirectoryEntry.read(entry.header, dis);
      }
    }
  }

  @Test
  public void writeDataAndReadDataShouldBeReflexive() throws IOException {
    byte[] data = DirectoryTestUtils.serializeDirectoryElement(entry);
    DirectoryEntry dest =
        DirectoryTestUtils.deserializeDirectoryEntry(entry.header, data);

    assertSame("wrong header", entry.header, dest.header);
    assertEquals("wrong offset", (short) 3, dest.getOffset());
    assertEquals("wrong inode number delta", (short) 4,
        dest.getInodeNumberDelta());
    assertEquals("wrong offset", (short) 5, dest.getType());
    assertEquals("wrong size", (short) 3, dest.getSize());
    assertEquals("wrong name", "test",
        new String(dest.getName(), StandardCharsets.ISO_8859_1));
  }

  @Test
  public void toStringShouldNotFail() {
    System.out.println(entry.toString());
  }

}
