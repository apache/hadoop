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
import org.apache.hadoop.runc.squashfs.test.DataTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestDataBlockWriter {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private File tempFile;
  private RandomAccessFile raf;
  private DataBlockWriter writer;

  @Before
  public void setUp() throws Exception {
    tempFile = temp.newFile();
    raf = new RandomAccessFile(tempFile, "rw");
    writer = new DataBlockWriter(raf, SuperBlock.DEFAULT_BLOCK_SIZE);
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

    DataBlockRef ref = writer.write(buf, 0, buf.length);
    System.out.println(ref);
    assertEquals("wrong location", 0L, ref.getLocation());
    assertEquals("wrong logical size", SuperBlock.DEFAULT_BLOCK_SIZE,
        ref.getLogicalSize());
    assertTrue("not compressed", ref.isCompressed());
    assertFalse("sparse", ref.isSparse());

    byte[] compressed = new byte[ref.getPhysicalSize()];
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

    DataBlockRef ref = writer.write(buf, 0, buf.length);
    System.out.println(ref);
    assertEquals("wrong location", 0L, ref.getLocation());
    assertEquals("wrong logical size", SuperBlock.DEFAULT_BLOCK_SIZE,
        ref.getLogicalSize());
    assertFalse("compressed", ref.isCompressed());
    assertFalse("sparse", ref.isSparse());

    byte[] buf2 = new byte[ref.getPhysicalSize()];
    raf.seek(0L);
    raf.readFully(buf2, 0, buf2.length);

    assertEquals("Wrong length", buf.length, buf2.length);
    assertArrayEquals("Wrong buffer", buf, buf2);
  }

  @Test
  public void writerMustSaveSparseBlockProperly() throws Exception {
    byte[] buf = new byte[SuperBlock.DEFAULT_BLOCK_SIZE];

    DataBlockRef ref = writer.write(buf, 0, buf.length);
    System.out.println(ref);
    assertEquals("wrong location", 0L, ref.getLocation());
    assertEquals("wrong logical size", SuperBlock.DEFAULT_BLOCK_SIZE,
        ref.getLogicalSize());
    assertEquals("wrong physical size", 0L, ref.getPhysicalSize());
    assertFalse("compressed", ref.isCompressed());
    assertTrue("sparse", ref.isSparse());
  }

  @Test(expected = IllegalArgumentException.class)
  public void writeOfShortBlockMustFail() throws Exception {
    writer.write(new byte[SuperBlock.DEFAULT_BLOCK_SIZE - 1], 0,
        SuperBlock.DEFAULT_BLOCK_SIZE - 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void writeOfEmptyBlockMustFail() throws Exception {
    writer.write(new byte[SuperBlock.DEFAULT_BLOCK_SIZE], 0, 0);
  }

}
