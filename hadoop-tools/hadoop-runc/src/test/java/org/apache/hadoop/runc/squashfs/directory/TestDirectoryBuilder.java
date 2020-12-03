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

  private DirectoryBuilder db;

  @Before
  public void setUp() {
    db = new DirectoryBuilder();
  }

  @Test
  public void addShouldCreateEntry() {
    db.add("test", 1, 2, (short) 3, INodeType.EXTENDED_FILE);
    assertEquals("wrong entry count", 1, db.getEntries().size());
    DirectoryBuilder.Entry entry = db.getEntries().get(0);
    assertEquals("wrong start block", 1, entry.getStartBlock());
    assertEquals("wrong inode number", 2, entry.getInodeNumber());
    assertEquals("wrong offset", (short) 3, entry.getOffset());
    assertEquals("wrong type", INodeType.BASIC_FILE.value(), entry.getType());
    assertEquals("wrong name", "test",
        new String(entry.getName(), StandardCharsets.ISO_8859_1));
    assertEquals(24, db.getStructureSize());
  }

  @Test
  public void addMultipleShouldCreateOnlyOneDirectoryHeader() {
    db.add("test", 1, 2, (short) 3, INodeType.EXTENDED_FILE);
    db.add("test2", 1, 4, (short) 3, INodeType.EXTENDED_FILE);
    db.build();
    List<DirectoryElement> elements = db.getElements();
    assertEquals("wrong element count", 3, elements.size());
    assertSame("wrong class for entry 0", DirectoryHeader.class,
        elements.get(0).getClass());
    assertSame("wrong class for entry 1", DirectoryEntry.class,
        elements.get(1).getClass());
    assertSame("wrong class for entry 2", DirectoryEntry.class,
        elements.get(2).getClass());
  }

  @Test
  public void addWithDifferentStartBlockShouldCreateMultipleHeaders() {
    db.add("test", 1, 2, (short) 3, INodeType.EXTENDED_FILE);
    db.add("test2", 4, 5, (short) 3, INodeType.EXTENDED_FILE);
    db.build();
    List<DirectoryElement> elements = db.getElements();
    assertEquals("wrong element count", 4, elements.size());
    assertSame("wrong class for entry 0", DirectoryHeader.class,
        elements.get(0).getClass());
    assertSame("wrong class for entry 1", DirectoryEntry.class,
        elements.get(1).getClass());
    assertSame("wrong class for entry 2", DirectoryHeader.class,
        elements.get(2).getClass());
    assertSame("wrong class for entry 3", DirectoryEntry.class,
        elements.get(3).getClass());
  }

  @Test
  public void addWithDecreasingInodeShouldCreateMultipleHeaders() {
    db.add("test", 1, 2, (short) 3, INodeType.EXTENDED_FILE);
    db.add("test2", 1, 1, (short) 3, INodeType.EXTENDED_FILE);
    db.build();
    List<DirectoryElement> elements = db.getElements();
    assertEquals("wrong element count", 4, elements.size());
    assertSame("wrong class for entry 0", DirectoryHeader.class,
        elements.get(0).getClass());
    assertSame("wrong class for entry 1", DirectoryEntry.class,
        elements.get(1).getClass());
    assertSame("wrong class for entry 2", DirectoryHeader.class,
        elements.get(2).getClass());
    assertSame("wrong class for entry 3", DirectoryEntry.class,
        elements.get(3).getClass());
  }

  @Test
  public void addWithLargeInodeShouldCreateMultipleHeaders() {
    db.add("test", 1, 2, (short) 3, INodeType.EXTENDED_FILE);
    db.add("test2", 1, 32770, (short) 3, INodeType.EXTENDED_FILE);
    db.build();
    List<DirectoryElement> elements = db.getElements();
    assertEquals("wrong element count", 4, elements.size());
    assertSame("wrong class for entry 0", DirectoryHeader.class,
        elements.get(0).getClass());
    assertSame("wrong class for entry 1", DirectoryEntry.class,
        elements.get(1).getClass());
    assertSame("wrong class for entry 2", DirectoryHeader.class,
        elements.get(2).getClass());
    assertSame("wrong class for entry 3", DirectoryEntry.class,
        elements.get(3).getClass());
  }

  @Test
  public void addMultipleShouldCreateOnlyOneDirectoryHeaderIf256Entries() {
    for (int i = 1; i <= 256; i++) {
      db.add("test" + i, 1, i, (short) 3, INodeType.EXTENDED_FILE);
    }
    db.build();
    List<DirectoryElement> elements = db.getElements();
    assertEquals("wrong element count", 257, elements.size());
    assertSame("wrong class for entry 0", DirectoryHeader.class,
        elements.get(0).getClass());
    for (int i = 1; i <= 256; i++) {
      assertSame("wrong class for entry " + i, DirectoryEntry.class,
          elements.get(i).getClass());
    }
  }

  @Test
  public void addMultipleShouldCreateMultipleDirectoryHeadersIf257Entries() {
    for (int i = 1; i <= 257; i++) {
      db.add("test" + i, 1, i, (short) 3, INodeType.EXTENDED_FILE);
    }
    db.build();
    List<DirectoryElement> elements = db.getElements();
    assertEquals("wrong element count", 259, elements.size());
    assertSame("wrong class for entry 0", DirectoryHeader.class,
        elements.get(0).getClass());
    for (int i = 1; i <= 256; i++) {
      assertSame("wrong class for entry " + i, DirectoryEntry.class,
          elements.get(i).getClass());
    }
    assertSame("wrong class for entry 257", DirectoryHeader.class,
        elements.get(257).getClass());
    assertSame("wrong class for entry 258", DirectoryEntry.class,
        elements.get(258).getClass());
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

    assertEquals("wrong size", 0, hdr.getCount());
    assertEquals("wrong start block", 1, hdr.getStartBlock());
    assertEquals("wrong inode number", 2, hdr.getInodeNumber());
    assertSame("wrong header", hdr, entry.getHeader());
    assertEquals("wrong offset", (short) 3, entry.getOffset());
    assertEquals("wrong inode number delta",
        (short) 0, entry.getInodeNumberDelta());
    assertEquals("wrong type", INodeType.BASIC_FILE.value(), entry.getType());
    assertEquals("wrong size", (short) 3, entry.getSize());
    assertEquals("wrong name", "test",
        new String(entry.getName(), StandardCharsets.ISO_8859_1));
  }

}
