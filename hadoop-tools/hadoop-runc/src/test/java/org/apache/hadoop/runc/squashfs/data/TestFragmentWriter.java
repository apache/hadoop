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

package org.apache.hadoop.runc.squashfs.data;

import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;
import org.apache.hadoop.runc.squashfs.table.FragmentTableEntry;
import org.apache.hadoop.runc.squashfs.test.DataTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestFragmentWriter {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private File tempFile;
  private RandomAccessFile raf;
  private FragmentWriter writer;

  @Before
  public void setUp() throws Exception {
    tempFile = temp.newFile();
    raf = new RandomAccessFile(tempFile, "rw");
    writer = new FragmentWriter(raf, SuperBlock.DEFAULT_BLOCK_SIZE);
  }

  @After
  public void tearDown() throws Exception {
    writer = null;
    raf.close();
    raf = null;
  }

  @Test
  public void writerMustSaveCompressibleBlockProperly() throws Exception {
    byte[] buf = new byte[SuperBlock.DEFAULT_BLOCK_SIZE];
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) 0xff; // all ones
    }

    writer.write(buf, 0, buf.length);
    writer.flush();

    assertEquals("wrong fragment entry count", 1,
        writer.getFragmentEntryCount());
    FragmentTableEntry fte = writer.getFragmentEntries().get(0);
    assertTrue("Not compressed", fte.isCompressed());

    byte[] compressed = new byte[fte.getDiskSize()];
    raf.seek(0L);
    raf.readFully(compressed, 0, compressed.length);

    byte[] decompressed = DataTestUtils.decompress(compressed);
    assertEquals("Wrong length", buf.length, decompressed.length);
    assertArrayEquals("Wrong buffer", buf, decompressed);
  }

  @Test
  public void writerMustSaveUncompressibleBlockProperly() throws Exception {
    Random random = new Random(0L);

    byte[] buf = new byte[SuperBlock.DEFAULT_BLOCK_SIZE];
    random.nextBytes(buf);

    writer.write(buf, 0, buf.length);
    writer.flush();

    assertEquals("wrong fragment entry count", 1,
        writer.getFragmentEntryCount());
    FragmentTableEntry fte = writer.getFragmentEntries().get(0);
    assertFalse("Compressed", fte.isCompressed());

    byte[] buf2 = new byte[fte.getDiskSize()];
    raf.seek(0L);
    raf.readFully(buf2, 0, buf2.length);

    assertEquals("Wrong length", buf.length, buf2.length);
    assertArrayEquals("Wrong buffer", buf, buf2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void attemptToWriteZeroBytesShouldFail() throws Exception {
    writer.write(new byte[0], 0, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void attemptToWriteMoreThanBlockSizeShouldFail() throws Exception {
    writer.write(new byte[SuperBlock.DEFAULT_BLOCK_SIZE + 1], 0,
        SuperBlock.DEFAULT_BLOCK_SIZE + 1);
  }

  @Test
  public void writingBeyondBlockSizeShouldTriggerFlush() throws Exception {
    writer.write(new byte[SuperBlock.DEFAULT_BLOCK_SIZE], 0,
        SuperBlock.DEFAULT_BLOCK_SIZE);
    assertEquals("wrong fragment entry count (before)", 0,
        writer.getFragmentEntryCount());
    writer.write(new byte[1], 0, 1);
    assertEquals("wrong fragment entry count (after)", 1,
        writer.getFragmentEntryCount());
  }

  @Test
  public void flushWithNoDataShouldNotTriggerFragmentEntryCreation()
      throws Exception {
    writer.flush();
    assertEquals("wrong fragment entry count", 0,
        writer.getFragmentEntryCount());
  }

  @Test
  public void flushWithDataShouldTriggerFragmentEntryCreation()
      throws Exception {
    writer.write(new byte[1], 0, 1);
    writer.flush();
    assertEquals("wrong fragment entry count", 1,
        writer.getFragmentEntryCount());
  }

  @Test
  public void doubleFlushShouldNotTriggerAdditionalFragmentEntryCreation()
      throws Exception {
    writer.write(new byte[1], 0, 1);
    writer.flush();
    assertEquals("wrong fragment entry count (before)", 1,
        writer.getFragmentEntryCount());
    writer.flush();
    assertEquals("wrong fragment entry count (after)", 1,
        writer.getFragmentEntryCount());
  }

  @Test
  public void fragmentTableRefSizeShouldBeZeroIfNoDataWritten()
      throws Exception {
    writer.flush();
    assertEquals("wrong entry count", 0, writer.getFragmentEntryCount());
    assertEquals("wrong table size", 0, writer.getFragmentTableRefSize());
  }

  @Test
  public void fragmentTableRefSizeShouldBeOneIfDataWritten() throws Exception {
    writer.write(new byte[1], 0, 1);
    writer.flush();
    assertEquals("wrong entry count", 1, writer.getFragmentEntryCount());
    assertEquals("wrong table size", 1, writer.getFragmentTableRefSize());
  }

  @Test
  public void fragmentTableRefSizeShouldBeOneIfFullBlockWritten()
      throws Exception {
    for (int i = 0; i < 512; i++) {
      writer.write(new byte[1], 0, 1);
      writer.flush();
    }
    assertEquals("wrong entry count", 512, writer.getFragmentEntryCount());
    assertEquals("wrong table size", 1, writer.getFragmentTableRefSize());
  }

  @Test
  public void fragmentTableRefSizeShouldBeTwoIfFullBlockPlusOneWritten()
      throws Exception {
    for (int i = 0; i < 513; i++) {
      writer.write(new byte[1], 0, 1);
      writer.flush();
    }
    assertEquals("wrong entry count", 513, writer.getFragmentEntryCount());
    assertEquals("wrong table size", 2, writer.getFragmentTableRefSize());
  }

  @Test
  public void saveShouldSerializeEmptyMetadataIfNoFragmentsPresent()
      throws Exception {
    byte[] data = DataTestUtils.saveFragmentMetadata(writer);
    assertEquals("wrong length", 0, data.length);
  }

  @Test
  public void saveShouldSerializeOneEntryIfOneFragmentPresent()
      throws Exception {
    writer.write(new byte[1], 0, 1);
    writer.flush();

    byte[] data = DataTestUtils.saveFragmentMetadata(writer);
    byte[] decoded = DataTestUtils.decodeMetadataBlock(data);
    assertEquals("wrong data length", 16, decoded.length);
    ByteBuffer bb = ByteBuffer.wrap(decoded).order(ByteOrder.LITTLE_ENDIAN);
    long start = bb.getLong();
    int size = bb.getInt();
    int unused = bb.getInt();
    assertEquals("wrong start", 0L, start);
    assertEquals("wrong size", 0x1000001, size);
    assertEquals("wrong unused value", 0, unused);
  }

  @Test
  public void saveShouldSerializeOneEntryIfTwoFragmentsPresent()
      throws Exception {
    writer.write(new byte[1], 0, 1);
    writer.flush();
    writer.write(new byte[1], 0, 1);
    writer.flush();

    byte[] data = DataTestUtils.saveFragmentMetadata(writer);
    byte[] decoded = DataTestUtils.decodeMetadataBlock(data);
    assertEquals("wrong data length", 32, decoded.length);
    ByteBuffer bb = ByteBuffer.wrap(decoded).order(ByteOrder.LITTLE_ENDIAN);

    assertEquals("wrong start 0", 0L, bb.getLong());
    assertEquals("wrong size 0", 0x1000001, bb.getInt());
    assertEquals("wrong unused value 0", 0, bb.getInt());

    assertEquals("wrong start 1", 1L, bb.getLong());
    assertEquals("wrong size 1", 0x1000001, bb.getInt());
    assertEquals("wrong unused value 1", 0, bb.getInt());
  }
}
