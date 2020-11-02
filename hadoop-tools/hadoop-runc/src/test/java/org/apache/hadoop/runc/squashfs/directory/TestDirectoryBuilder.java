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

import org.apache.hadoop.runc.squashfs.inode.INodeType;
import org.apache.hadoop.runc.squashfs.test.DirectoryTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class TestDirectoryBuilder {

  DirectoryBuilder db;

  @Before
  public void setUp() {
    db = new DirectoryBuilder();
  }

  @Test
  public void addShouldCreateEntry() {
    db.add("test", 1, 2, (short) 3, INodeType.EXTENDED_FILE);
    assertEquals("wrong entry count", 1, db.entries.size());
    DirectoryBuilder.Entry entry = db.entries.get(0);
    assertEquals("wrong start block", 1, entry.startBlock);
    assertEquals("wrong inode number", 2, entry.inodeNumber);
    assertEquals("wrong offset", (short) 3, entry.offset);
    assertEquals("wrong type", INodeType.BASIC_FILE.value(), entry.type);
    assertEquals("wrong name", "test",
        new String(entry.name, StandardCharsets.ISO_8859_1));
    assertEquals(24, db.getStructureSize());
  }

  @Test
  public void addMultipleShouldCreateOnlyOneDirectoryHeader() {
    db.add("test", 1, 2, (short) 3, INodeType.EXTENDED_FILE);
    db.add("test2", 1, 4, (short) 3, INodeType.EXTENDED_FILE);
    db.build();
    assertEquals("wrong element count", 3, db.elements.size());
    assertSame("wrong class for entry 0", DirectoryHeader.class,
        db.elements.get(0).getClass());
    assertSame("wrong class for entry 1", DirectoryEntry.class,
        db.elements.get(1).getClass());
    assertSame("wrong class for entry 2", DirectoryEntry.class,
        db.elements.get(2).getClass());
  }

  @Test
  public void addMultipleShouldCreateMultipleDirectoryHeadersIfStartBlockChanges() {
    db.add("test", 1, 2, (short) 3, INodeType.EXTENDED_FILE);
    db.add("test2", 4, 5, (short) 3, INodeType.EXTENDED_FILE);
    db.build();
    assertEquals("wrong element count", 4, db.elements.size());
    assertSame("wrong class for entry 0", DirectoryHeader.class,
        db.elements.get(0).getClass());
    assertSame("wrong class for entry 1", DirectoryEntry.class,
        db.elements.get(1).getClass());
    assertSame("wrong class for entry 2", DirectoryHeader.class,
        db.elements.get(2).getClass());
    assertSame("wrong class for entry 3", DirectoryEntry.class,
        db.elements.get(3).getClass());
  }

  @Test
  public void addMultipleShouldCreateMultipleDirectoryHeadersIfInodeNumberGoesBackwards() {
    db.add("test", 1, 2, (short) 3, INodeType.EXTENDED_FILE);
    db.add("test2", 1, 1, (short) 3, INodeType.EXTENDED_FILE);
    db.build();
    assertEquals("wrong element count", 4, db.elements.size());
    assertSame("wrong class for entry 0", DirectoryHeader.class,
        db.elements.get(0).getClass());
    assertSame("wrong class for entry 1", DirectoryEntry.class,
        db.elements.get(1).getClass());
    assertSame("wrong class for entry 2", DirectoryHeader.class,
        db.elements.get(2).getClass());
    assertSame("wrong class for entry 3", DirectoryEntry.class,
        db.elements.get(3).getClass());
  }

  @Test
  public void addMultipleShouldCreateMultipleDirectoryHeadersIfInodeNumberIsTooLarge() {
    db.add("test", 1, 2, (short) 3, INodeType.EXTENDED_FILE);
    db.add("test2", 1, 32770, (short) 3, INodeType.EXTENDED_FILE);
    db.build();
    assertEquals("wrong element count", 4, db.elements.size());
    assertSame("wrong class for entry 0", DirectoryHeader.class,
        db.elements.get(0).getClass());
    assertSame("wrong class for entry 1", DirectoryEntry.class,
        db.elements.get(1).getClass());
    assertSame("wrong class for entry 2", DirectoryHeader.class,
        db.elements.get(2).getClass());
    assertSame("wrong class for entry 3", DirectoryEntry.class,
        db.elements.get(3).getClass());
  }

  @Test
  public void addMultipleShouldCreateOnlyOneDirectoryHeaderIf256Entries() {
    for (int i = 1; i <= 256; i++) {
      db.add("test" + i, 1, i, (short) 3, INodeType.EXTENDED_FILE);
    }
    db.build();
    assertEquals("wrong element count", 257, db.elements.size());
    assertSame("wrong class for entry 0", DirectoryHeader.class,
        db.elements.get(0).getClass());
    for (int i = 1; i <= 256; i++) {
      assertSame("wrong class for entry " + i, DirectoryEntry.class,
          db.elements.get(i).getClass());
    }
  }

  @Test
  public void addMultipleShouldCreateMultipleDirectoryHeadersIf257Entries() {
    for (int i = 1; i <= 257; i++) {
      db.add("test" + i, 1, i, (short) 3, INodeType.EXTENDED_FILE);
    }
    db.build();
    assertEquals("wrong element count", 259, db.elements.size());
    assertSame("wrong class for entry 0", DirectoryHeader.class,
        db.elements.get(0).getClass());
    for (int i = 1; i <= 256; i++) {
      assertSame("wrong class for entry " + i, DirectoryEntry.class,
          db.elements.get(i).getClass());
    }
    assertSame("wrong class for entry 257", DirectoryHeader.class,
        db.elements.get(257).getClass());
    assertSame("wrong class for entry 258", DirectoryEntry.class,
        db.elements.get(258).getClass());
  }

  @Test(expected = IllegalArgumentException.class)
  public void addWithEmptyFilenameShouldFail() {
    db.add("", 1, 2, (short) 3, INodeType.EXTENDED_FILE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void addWithTooLongFilenameShouldFail() {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < 257; i++) {
      buf.append("x");
    }
    db.add(buf.toString(), 1, 2, (short) 3, INodeType.EXTENDED_FILE);
  }

  @Test
  public void getStructureSizeShouldReturnZeroWithNoEntries() {
    assertEquals(0, db.getStructureSize());
  }

  @Test
  public void writeShouldSerializeZeroBytes() throws Exception {
    byte[] data = DirectoryTestUtils.serializeDirectoryBuilder(db);
    assertEquals("wrong length", 0, data.length);
  }

  @Test
  public void writeShouldSerializeData() throws Exception {
    db.add("test", 1, 2, (short) 3, INodeType.EXTENDED_FILE);
    byte[] data = DirectoryTestUtils.serializeDirectoryBuilder(db);

    List<DirectoryElement> elements =
        DirectoryTestUtils.deserializeDirectory(data);
    assertEquals("wrong length", 2, elements.size());
    assertEquals("wrong type for element 0", DirectoryHeader.class,
        elements.get(0).getClass());
    assertEquals("wrong type for element 1", DirectoryEntry.class,
        elements.get(1).getClass());

    DirectoryHeader hdr = (DirectoryHeader) elements.get(0);
    DirectoryEntry entry = (DirectoryEntry) elements.get(1);

    assertEquals("wrong size", 0, hdr.count);
    assertEquals("wrong start block", 1, hdr.startBlock);
    assertEquals("wrong inode number", 2, hdr.inodeNumber);
    assertSame("wrong header", hdr, entry.header);
    assertEquals("wrong offset", (short) 3, entry.offset);
    assertEquals("wrong inode number delta", (short) 0, entry.inodeNumberDelta);
    assertEquals("wrong type", INodeType.BASIC_FILE.value(), entry.type);
    assertEquals("wrong size", (short) 3, entry.size);
    assertEquals("wrong name", "test",
        new String(entry.getName(), StandardCharsets.ISO_8859_1));
  }

}
