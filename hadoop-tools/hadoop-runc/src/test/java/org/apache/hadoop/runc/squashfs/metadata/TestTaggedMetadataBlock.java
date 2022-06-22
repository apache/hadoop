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

import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;
import org.apache.hadoop.runc.squashfs.test.MetadataTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.EOFException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class TestTaggedMetadataBlock {

  private TaggedMetadataBlockReader taggedReader;
  private MemoryMetadataBlockReader reader;
  private SuperBlock sb;
  private byte[] block;
  private byte[] block2;
  private byte[] encoded;
  private int offset2;

  @Before
  public void setUp() throws Exception {
    sb = new SuperBlock();
    // write a block
    block = new byte[1024];
    for (int i = 0; i < block.length; i++) {
      block[i] = (byte) (i & 0xff);
    }
    block2 = new byte[1024];
    for (int i = 0; i < block2.length; i++) {
      block2[i] = (byte) ((i + 128) & 0xff);
    }
    byte[] data1 = MetadataTestUtils.saveMetadataBlock(block);
    byte[] data2 = MetadataTestUtils.saveMetadataBlock(block2);
    offset2 = data1.length;
    encoded = new byte[data1.length + data2.length];
    System.arraycopy(data1, 0, encoded, 0, data1.length);
    System.arraycopy(data2, 0, encoded, data1.length, data2.length);
    reader =
        new MemoryMetadataBlockReader(10101, sb, encoded, 0, encoded.length);
    taggedReader = new TaggedMetadataBlockReader(true);
    taggedReader.add(10101, reader);
  }

  @After
  public void tearDown() throws Exception {
    taggedReader.close();
    taggedReader = null;
    reader = null;
    encoded = null;
    block = null;
    block2 = null;
    sb = null;
  }

  @Test(expected = IllegalArgumentException.class)
  public void addOfExistingTagShouldFail() {
    taggedReader.add(10101, reader);
  }

  @Test
  public void addOfNewTagShouldSucceed() {
    taggedReader.add(10102, reader);
  }

  @Test
  public void getSuperBlockShouldReturnConstructedInstance() {
    assertSame(sb, taggedReader.getSuperBlock(10101));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getSuperBlockShouldFailOnIncorrectTag() {
    taggedReader.getSuperBlock(10102);
  }

  @Test
  public void readFirstBlockShouldSucceed() throws Exception {
    MetadataBlock mb = taggedReader.read(10101, 0L);
    assertEquals(1024, mb.getData().length);
    assertArrayEquals(block, mb.getData());
  }

  @Test
  public void readSecondBlockShouldSucceed() throws Exception {
    MetadataBlock mb = taggedReader.read(10101, offset2);
    assertEquals(1024, mb.getData().length);
    assertArrayEquals(block2, mb.getData());
  }

  @Test(expected = EOFException.class)
  public void readPastEofShouldFail() throws Exception {
    taggedReader.read(10101, encoded.length);
  }

}
