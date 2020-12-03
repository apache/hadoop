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
import static org.junit.Assert.assertSame;

public class TestDirectoryEntry {

  private DirectoryEntry entry;

  @Before
  public void setUp() {
    DirectoryHeader hdr = new DirectoryHeader(0, 1, 2);
    byte[] name = "test".getBytes(StandardCharsets.ISO_8859_1);
    entry = new DirectoryEntry((short) 3, (short) 4, (short) 5,
        (short) (name.length - 1), name, hdr);
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
        DirectoryEntry dest = DirectoryEntry.read(entry.getHeader(), dis);
        assertSame("wrong header", entry.getHeader(), dest.getHeader());
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
        DirectoryEntry.read(entry.getHeader(), dis);
      }
    }
  }

  @Test
  public void writeDataAndReadDataShouldBeReflexive() throws IOException {
    byte[] data = DirectoryTestUtils.serializeDirectoryElement(entry);
    DirectoryEntry dest =
        DirectoryTestUtils.deserializeDirectoryEntry(entry.getHeader(), data);

    assertSame("wrong header", entry.getHeader(), dest.getHeader());
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
