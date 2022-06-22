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

package org.apache.hadoop.runc.squashfs.superblock;

import org.apache.hadoop.runc.squashfs.SquashFsException;
import org.apache.hadoop.runc.squashfs.test.SuperBlockTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestSuperBlock {

  private SuperBlock sb;

  @Before
  public void setUp() {
    sb = new SuperBlock();
  }

  @Test
  public void inodeCountPropertyShouldWorkAsExpected() {
    assertEquals(0, sb.getInodeCount());
    sb.setInodeCount(1);
    assertEquals(1, sb.getInodeCount());
  }

  @Test
  public void modificationTimePropertyShouldWorkAsExpected() {
    assertEquals((double) System.currentTimeMillis(),
        (double) (sb.getModificationTime() * 1000L), 5000d);
    sb.setModificationTime(1);
    assertEquals(1, sb.getModificationTime());
  }

  @Test
  public void blockSizePropertyShouldWorkAsExpected() {
    assertEquals(131072, sb.getBlockSize());
    sb.setBlockSize(262144);
    assertEquals(262144, sb.getBlockSize());
  }

  @Test
  public void fragmentEntryCountPropertyShouldWorkAsExpected() {
    assertEquals(0, sb.getFragmentEntryCount());
    sb.setFragmentEntryCount(1);
    assertEquals(1, sb.getFragmentEntryCount());
  }

  @Test
  public void compressionIdPropertyShouldWorkAsExpected() {
    assertSame(CompressionId.ZLIB, sb.getCompressionId());
    sb.setCompressionId(CompressionId.NONE);
    assertSame(CompressionId.NONE, sb.getCompressionId());
  }

  @Test
  public void blockLogPropertyShouldWorkAsExpected() {
    assertEquals((short) 17, sb.getBlockLog());
    sb.setBlockLog((short) 18);
    assertEquals((short) 18, sb.getBlockLog());
  }

  @Test
  public void flagsPropertyShouldWorkAsExpected() {
    assertEquals(SuperBlockFlag
            .flagsFor(SuperBlockFlag.EXPORTABLE, SuperBlockFlag.DUPLICATES),
        sb.getFlags());
    sb.setFlags(SuperBlockFlag.CHECK.mask());
    assertEquals(SuperBlockFlag.CHECK.mask(), sb.getFlags());
  }

  @Test
  public void idCountPropertyShouldWorkAsExpected() {
    assertEquals((short) 0, sb.getIdCount());
    sb.setIdCount((short) 1);
    assertEquals((short) 1, sb.getIdCount());
  }

  @Test
  public void versionMajorPropertyShouldWorkAsExpected() {
    assertEquals((short) 4, sb.getVersionMajor());
    sb.setVersionMajor((short) 5);
    assertEquals((short) 5, sb.getVersionMajor());
  }

  @Test
  public void versionMinorPropertyShouldWorkAsExpected() {
    assertEquals((short) 0, sb.getVersionMinor());
    sb.setVersionMinor((short) 1);
    assertEquals((short) 1, sb.getVersionMinor());
  }

  @Test
  public void rootInodeRefPropertyShouldWorkAsExpected() {
    assertEquals(0L, sb.getRootInodeRef());
    sb.setRootInodeRef(1L);
    assertEquals(1L, sb.getRootInodeRef());
  }

  @Test
  public void bytesUsedPropertyShouldWorkAsExpected() {
    assertEquals(0L, sb.getBytesUsed());
    sb.setBytesUsed(1L);
    assertEquals(1L, sb.getBytesUsed());
  }

  @Test
  public void idTableStartPropertyShouldWorkAsExpected() {
    assertEquals(0L, sb.getIdTableStart());
    sb.setIdTableStart(1L);
    assertEquals(1L, sb.getIdTableStart());
  }

  @Test
  public void xattrIdTableStartPropertyShouldWorkAsExpected() {
    assertEquals(-1L, sb.getXattrIdTableStart());
    sb.setXattrIdTableStart(1L);
    assertEquals(1L, sb.getXattrIdTableStart());
  }

  @Test
  public void inodeTableStartPropertyShouldWorkAsExpected() {
    assertEquals(0L, sb.getInodeTableStart());
    sb.setInodeTableStart(1L);
    assertEquals(1L, sb.getInodeTableStart());
  }

  @Test
  public void directoryTableStartPropertyShouldWorkAsExpected() {
    assertEquals(0L, sb.getDirectoryTableStart());
    sb.setDirectoryTableStart(1L);
    assertEquals(1L, sb.getDirectoryTableStart());
  }

  @Test
  public void fragmentTableStartPropertyShouldWorkAsExpected() {
    assertEquals(0L, sb.getFragmentTableStart());
    sb.setFragmentTableStart(1L);
    assertEquals(1L, sb.getFragmentTableStart());
  }

  @Test
  public void exportTableStartPropertyShouldWorkAsExpected() {
    assertEquals(0L, sb.getExportTableStart());
    sb.setExportTableStart(1L);
    assertEquals(1L, sb.getExportTableStart());
  }

  @Test
  public void hasFlagShouldCorrectlyDetectFlags() {
    assertTrue("Missing exportable flag",
        sb.hasFlag(SuperBlockFlag.EXPORTABLE));
    assertTrue("Missing duplicates flag",
        sb.hasFlag(SuperBlockFlag.DUPLICATES));
    assertFalse("Has always fragments flag",
        sb.hasFlag(SuperBlockFlag.ALWAYS_FRAGMENTS));
  }

  @Test
  public void readShouldSucceedNormally() throws Exception {
    byte[] data = SuperBlockTestUtils.serializeSuperBlock(sb);

    try (ByteArrayInputStream bis = new ByteArrayInputStream(data)) {
      try (DataInputStream dis = new DataInputStream(bis)) {
        assertNotNull(SuperBlock.read(dis));
      }
    }
  }

  @Test(expected = SquashFsException.class)
  public void readDataShouldFailOnInvalidMagic() throws Exception {
    byte[] data = SuperBlockTestUtils.serializeSuperBlock(sb);
    data[0] = 0; // corrupt
    SuperBlockTestUtils.deserializeSuperBlock(data);
  }

  @Test(expected = SquashFsException.class)
  public void readDataShouldFailOnMismatchedBlockSize() throws Exception {
    byte[] data = SuperBlockTestUtils.serializeSuperBlock(sb);
    data[22] = 0; // corrupt
    SuperBlockTestUtils.deserializeSuperBlock(data);
  }

  @Test(expected = SquashFsException.class)
  public void readDataShouldFailOnUnknownVersion() throws Exception {
    byte[] data = SuperBlockTestUtils.serializeSuperBlock(sb);
    data[30] = 1; // corrupt
    SuperBlockTestUtils.deserializeSuperBlock(data);
  }

  @Test
  public void writeDataAndReadDataShouldBeReflexive() throws Exception {
    sb.setInodeCount(1);
    sb.setBlockLog((short) 18);
    sb.setBlockSize(262144);
    sb.setFragmentEntryCount(2);
    sb.setCompressionId(CompressionId.LZO);
    sb.setFlags((short) 3);
    sb.setIdCount((short) 4);
    sb.setRootInodeRef(5L);
    sb.setBytesUsed(6L);
    sb.setIdTableStart(7L);
    sb.setXattrIdTableStart(8L);
    sb.setInodeTableStart(9L);
    sb.setDirectoryTableStart(10L);
    sb.setFragmentTableStart(11L);
    sb.setExportTableStart(12L);

    byte[] data = SuperBlockTestUtils.serializeSuperBlock(sb);

    SuperBlock sb2 = SuperBlockTestUtils.deserializeSuperBlock(data);
    System.out.println(sb2);

    assertEquals("Wrong inode count", sb.getInodeCount(), sb2.getInodeCount());
    assertEquals("Wrong modification time", sb.getModificationTime(),
        sb2.getModificationTime());
    assertEquals("Wrong block size", sb.getBlockSize(), sb2.getBlockSize());
    assertEquals("Wrong fragment entry count", sb.getFragmentEntryCount(),
        sb2.getFragmentEntryCount());
    assertSame("Wrong compression ID", sb.getCompressionId(),
        sb2.getCompressionId());
    assertEquals("Wrong block log", sb.getBlockLog(), sb2.getBlockLog());
    assertEquals("Wrong flags", sb.getFlags(), sb2.getFlags());
    assertEquals("Wrong id count", sb.getIdCount(), sb2.getIdCount());
    assertEquals("Wrong root inode ref", sb.getRootInodeRef(),
        sb2.getRootInodeRef());
    assertEquals("Wrong bytes used", sb.getBytesUsed(), sb2.getBytesUsed());
    assertEquals("Wrong id table start", sb.getIdTableStart(),
        sb2.getIdTableStart());
    assertEquals("Wrong xattr id table start", sb.getXattrIdTableStart(),
        sb2.getXattrIdTableStart());
    assertEquals("Wrong inode table start", sb.getInodeTableStart(),
        sb2.getInodeTableStart());
    assertEquals("Wrong directory table start", sb.getDirectoryTableStart(),
        sb2.getDirectoryTableStart());
    assertEquals("Wrong fragment table start", sb.getFragmentTableStart(),
        sb2.getFragmentTableStart());
    assertEquals("Wrong export table start", sb.getExportTableStart(),
        sb2.getExportTableStart());
  }

  @Test
  public void toStringShouldNotFail() {
    System.out.println(sb.toString());
  }

}
