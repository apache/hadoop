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

package org.apache.hadoop.runc.squashfs.metadata;

import org.apache.hadoop.runc.squashfs.SquashFsException;
import org.apache.hadoop.runc.squashfs.superblock.CompressionId;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlockFlag;
import org.apache.hadoop.runc.squashfs.test.DataTestUtils;
import org.apache.hadoop.runc.squashfs.test.MetadataTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestMetadataBlock {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void readingCompressedBlockShouldSucceed() throws Exception {

    byte[] buf = new byte[8192];
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) 0xff;
    }
    byte[] blockData = MetadataTestUtils.saveMetadataBlock(buf);

    SuperBlock sb = new SuperBlock();
    try (DataInputStream dis = new DataInputStream(
        new ByteArrayInputStream(blockData))) {
      MetadataBlock block = MetadataBlock.read(dis, sb);
      assertTrue("not compressed", block.isCompressed());
      assertArrayEquals(buf, block.getData());
    }

  }

  @Test(expected = SquashFsException.class)
  public void readingCompressedBlockShouldFailIfCompressionNotInUse()
      throws Exception {

    byte[] buf = new byte[8192];
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) 0xff;
    }
    byte[] blockData = MetadataTestUtils.saveMetadataBlock(buf);

    SuperBlock sb = new SuperBlock();
    sb.setCompressionId(CompressionId.NONE);
    try (DataInputStream dis = new DataInputStream(
        new ByteArrayInputStream(blockData))) {
      MetadataBlock.read(dis, sb);
    }

  }

  @Test(expected = UnsupportedOperationException.class)
  public void readingCompressedBlockShouldFailIfUnsupportedAlgorithm()
      throws Exception {

    byte[] buf = new byte[8192];
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) 0xff;
    }
    byte[] blockData = MetadataTestUtils.saveMetadataBlock(buf);

    SuperBlock sb = new SuperBlock();
    sb.setCompressionId(CompressionId.XZ);
    try (DataInputStream dis = new DataInputStream(
        new ByteArrayInputStream(blockData))) {
      MetadataBlock.read(dis, sb);
    }

  }

  @Test(expected = UnsupportedOperationException.class)
  public void readingCompressedBlockShouldFailIfCompressionFlagsPresent()
      throws Exception {

    byte[] buf = new byte[8192];
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) 0xff;
    }
    byte[] blockData = MetadataTestUtils.saveMetadataBlock(buf);

    SuperBlock sb = new SuperBlock();
    sb.setFlags(
        (short) (sb.getFlags() | SuperBlockFlag.COMPRESSOR_OPTIONS.mask()));
    try (DataInputStream dis = new DataInputStream(
        new ByteArrayInputStream(blockData))) {
      MetadataBlock.read(dis, sb);
    }

  }

  @Test
  public void toStringShouldNotFailWhenCompressed() throws Exception {
    byte[] buf = new byte[64];
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte) 0xff;
    }
    byte[] blockData = MetadataTestUtils.saveMetadataBlock(buf);

    SuperBlock sb = new SuperBlock();
    try (DataInputStream dis = new DataInputStream(
        new ByteArrayInputStream(blockData))) {
      MetadataBlock block = MetadataBlock.read(dis, sb);
      System.out.println(block);
    }
  }

  @Test
  public void toStringShouldNotFailWhenUncompressed() throws Exception {
    Random r = new Random(0L);

    byte[] buf = new byte[64];
    r.nextBytes(buf);

    byte[] blockData = MetadataTestUtils.saveMetadataBlock(buf);

    SuperBlock sb = new SuperBlock();
    try (DataInputStream dis = new DataInputStream(
        new ByteArrayInputStream(blockData))) {
      MetadataBlock block = MetadataBlock.read(dis, sb);
      System.out.println(block);
    }
  }

  @Test
  public void readingUncompressedBlockShouldSucceed() throws Exception {
    Random r = new Random(0L);

    byte[] buf = new byte[8192];
    r.nextBytes(buf);

    byte[] blockData = MetadataTestUtils.saveMetadataBlock(buf);

    SuperBlock sb = new SuperBlock();
    try (DataInputStream dis = new DataInputStream(
        new ByteArrayInputStream(blockData))) {
      MetadataBlock block = MetadataBlock.read(dis, sb);
      assertFalse("compressed", block.isCompressed());
      assertArrayEquals(buf, block.getData());
    }

  }

  @Test(expected = SquashFsException.class)
  public void readingUncompressedDataThatIsTooLargeShouldFail()
      throws Exception {
    byte[] buf = new byte[8196];
    ShortBuffer sbuf =
        ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer();
    sbuf.put((short) (8194 | 0x8000)); // 8194 bytes, uncompressed

    SuperBlock sb = new SuperBlock();
    try (DataInputStream dis = new DataInputStream(
        new ByteArrayInputStream(buf))) {
      MetadataBlock.read(dis, sb);
    }
  }

  @Test(expected = SquashFsException.class)
  public void readingCompressedDataThatIsTooLargeShouldFail() throws Exception {

    byte[] buf = new byte[8194];
    byte[] compressed = DataTestUtils.compress(buf);
    byte[] encoded = new byte[compressed.length + 2];
    System.arraycopy(compressed, 0, encoded, 2, compressed.length);
    ShortBuffer sbuf =
        ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer();
    sbuf.put((short) (compressed.length & 0x7fff));

    SuperBlock sb = new SuperBlock();
    try (DataInputStream dis = new DataInputStream(
        new ByteArrayInputStream(encoded))) {
      MetadataBlock.read(dis, sb);
    }
  }

}
