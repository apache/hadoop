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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class TestFileMetadataBlockReader {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private int tag;
  private File tempFile;
  private FileMetadataBlockReader reader;
  private SuperBlock sb;
  private byte[] block;
  private byte[] encoded;

  @Before
  public void setUp() throws Exception {
    tag = 1;
    tempFile = temp.newFile();
    sb = new SuperBlock();
    try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw")) {
      sb.writeData(raf);

      // write a block
      block = new byte[1024];
      for (int i = 0; i < block.length; i++) {
        block[i] = (byte) (i & 0xff);
      }
      encoded = MetadataTestUtils.saveMetadataBlock(block);
      raf.write(encoded);
    }

    reader = new FileMetadataBlockReader(tag, tempFile);
  }

  @After
  public void tearDown() throws Exception {
    reader.close();
    reader = null;
    encoded = null;
    block = null;
    sb = null;
  }

  @Test
  public void getSuperBlockShouldReturnVersionReadFromFile() {
    assertEquals(sb.getModificationTime(),
        reader.getSuperBlock(tag).getModificationTime());
  }

  @Test
  public void getSuperBlockShouldReturnConstructedVersionIfApplicable()
      throws Exception {
    try (RandomAccessFile raf = new RandomAccessFile(tempFile, "r")) {
      reader = new FileMetadataBlockReader(tag, raf, sb, true);
    }
    assertSame(sb, reader.getSuperBlock(tag));
  }

  @Test
  public void readFromFileOffsetShouldSucceed() throws Exception {
    MetadataBlock mb = reader.read(tag, SuperBlock.SIZE);
    assertEquals(1024, mb.getData().length);
    assertArrayEquals(block, mb.getData());
  }

  @Test
  public void readFromFileOffsetOnRandomAccessFileBackedReaderShouldSucceed()
      throws Exception {
    try (RandomAccessFile raf = new RandomAccessFile(tempFile, "r")) {
      reader = new FileMetadataBlockReader(tag, raf, sb, true);
      MetadataBlock mb = reader.read(tag, SuperBlock.SIZE);
      assertEquals(1024, mb.getData().length);
      assertArrayEquals(block, mb.getData());
    }
  }

  @Test
  public void closeShouldCloseUnderlyingReaderIfRequested() throws Exception {
    try (RandomAccessFile raf = new RandomAccessFile(tempFile, "r")) {
      reader = new FileMetadataBlockReader(tag, raf, sb, true);
      reader.close();

      try {
        raf.seek(0L);
        fail("exception not thrown");
      } catch (IOException e) {
        System.out.println("EOF");
      }
    }
  }

  @Test
  public void closeShouldNotCloseUnderlyingReaderIfNotRequested()
      throws Exception {
    try (RandomAccessFile raf = new RandomAccessFile(tempFile, "r")) {
      reader = new FileMetadataBlockReader(tag, raf, sb, false);
      reader.close();
      raf.seek(0L);
    }
  }

}
