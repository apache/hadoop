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

import org.apache.hadoop.runc.squashfs.SquashFsException;
import org.apache.hadoop.runc.squashfs.inode.BasicFileINode;
import org.apache.hadoop.runc.squashfs.superblock.CompressionId;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlockFlag;
import org.apache.hadoop.runc.squashfs.table.FragmentTable;
import org.apache.hadoop.runc.squashfs.table.FragmentTableEntry;
import org.apache.hadoop.runc.squashfs.test.InMemoryFragmentTable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestDataBlockReader {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private File tempFile;
  private RandomAccessFile raf;
  private SuperBlock sb;
  private int tag;

  @Before
  public void setUp() throws Exception {
    tempFile = temp.newFile();
    tag = 10101;
    raf = new RandomAccessFile(tempFile, "rw");
    sb = new SuperBlock();
    sb.writeData(raf);
  }

  DataBlockRef writeBlock(byte[] data, int offset, int length)
      throws IOException {
    DataBlockWriter writer =
        new DataBlockWriter(raf, SuperBlock.DEFAULT_BLOCK_SIZE);
    return writer.write(data, offset, length);
  }

  FragmentRef writeFragment(FragmentWriter writer, byte[] data, int offset,
      int length) throws IOException {
    FragmentRef ref = writer.write(data, offset, length);
    return ref;
  }

  @Test
  public void readOfSingleFragmentShouldSucceed() throws Exception {
    byte[] data = new byte[SuperBlock.DEFAULT_BLOCK_SIZE - 1];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) 0xff;
    }

    FragmentWriter fw = new FragmentWriter(raf, SuperBlock.DEFAULT_BLOCK_SIZE);
    FragmentRef ref = writeFragment(fw, data, 0, data.length);
    fw.flush();
    System.out.println(ref);
    FragmentTableEntry entry = fw.getFragmentEntries().get(0);
    System.out.println(entry);

    BasicFileINode inode = new BasicFileINode();
    inode.setFileSize(data.length);
    inode.setFragmentOffset(ref.getOffset());
    inode.setFragmentBlockIndex(ref.getFragmentIndex());

    FragmentTable ft = new InMemoryFragmentTable(entry);

    DataBlock block =
        DataBlockReader.readFragment(tag, raf, sb, inode, ft, data.length);
    assertEquals("wrong logical size", data.length, block.getLogicalSize());
    assertEquals("wrong physical size", data.length, block.getPhysicalSize());
    assertArrayEquals("wrong data", data, block.getData());
  }

  @Test(expected = SquashFsException.class)
  public void readOfSingleFragmentShouldFailIfReadTooManyBytes()
      throws Exception {
    byte[] data = new byte[SuperBlock.DEFAULT_BLOCK_SIZE - 2];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) 0xff;
    }

    FragmentWriter fw = new FragmentWriter(raf, SuperBlock.DEFAULT_BLOCK_SIZE);
    FragmentRef ref = writeFragment(fw, data, 0, data.length);
    fw.flush();
    System.out.println(ref);
    FragmentTableEntry entry = fw.getFragmentEntries().get(0);
    System.out.println(entry);

    BasicFileINode inode = new BasicFileINode();
    inode.setFileSize(data.length);
    inode.setFragmentOffset(ref.getOffset());
    inode.setFragmentBlockIndex(ref.getFragmentIndex());

    FragmentTable ft = new InMemoryFragmentTable(entry);

    DataBlock block =
        DataBlockReader.readFragment(tag, raf, sb, inode, ft, data.length + 1);
    assertEquals("wrong logical size", data.length, block.getLogicalSize());
    assertEquals("wrong physical size", data.length, block.getPhysicalSize());
    assertArrayEquals("wrong data", data, block.getData());
  }

  @Test
  public void readOfSingleCompressedBlockShouldSucceed() throws Exception {
    byte[] data = new byte[SuperBlock.DEFAULT_BLOCK_SIZE];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) 0xff;
    }
    DataBlockRef ref = writeBlock(data, 0, data.length);
    System.out.println(ref);

    BasicFileINode inode = new BasicFileINode();
    inode.setFileSize(data.length);
    inode.setBlocksStart(ref.getLocation());
    inode.setBlockSizes(new int[] {ref.getInodeSize()});
    DataBlock block = DataBlockReader.readBlock(tag, raf, sb, inode, 0);
    assertEquals("wrong logical size", data.length, block.getLogicalSize());
    assertEquals("wrong physical size", data.length, block.getPhysicalSize());
    assertArrayEquals("wrong data", data, block.getData());
  }

  @Test(expected = SquashFsException.class)
  public void readOfCompressedBlockShouldFailIfCompressionIdIsNotSet()
      throws Exception {
    sb.setCompressionId(CompressionId.NONE);
    byte[] data = new byte[SuperBlock.DEFAULT_BLOCK_SIZE];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) 0xff;
    }
    DataBlockRef ref = writeBlock(data, 0, data.length);
    System.out.println(ref);

    BasicFileINode inode = new BasicFileINode();
    inode.setFileSize(data.length);
    inode.setBlocksStart(ref.getLocation());
    inode.setBlockSizes(new int[] {ref.getInodeSize()});

    DataBlockReader.readBlock(tag, raf, sb, inode, 0);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void readOfCompressedBlockShouldFailIfCompressionOptionsSet()
      throws Exception {
    sb.setFlags(
        (short) (sb.getFlags() | SuperBlockFlag.COMPRESSOR_OPTIONS.mask()));
    byte[] data = new byte[SuperBlock.DEFAULT_BLOCK_SIZE];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) 0xff;
    }
    DataBlockRef ref = writeBlock(data, 0, data.length);
    System.out.println(ref);

    BasicFileINode inode = new BasicFileINode();
    inode.setFileSize(data.length);
    inode.setBlocksStart(ref.getLocation());
    inode.setBlockSizes(new int[] {ref.getInodeSize()});

    DataBlockReader.readBlock(tag, raf, sb, inode, 0);
  }

  @Test(expected = SquashFsException.class)
  public void readOfCompressedBlockShouldFailIfDecompressedTooLarge()
      throws Exception {
    byte[] data = new byte[SuperBlock.DEFAULT_BLOCK_SIZE];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) 0xff;
    }
    DataBlockRef ref = writeBlock(data, 0, data.length);
    System.out.println(ref);

    sb.setBlockSize(SuperBlock.DEFAULT_BLOCK_SIZE / 2);
    BasicFileINode inode = new BasicFileINode();
    inode.setFileSize(data.length / 2);
    inode.setBlocksStart(ref.getLocation());
    inode.setBlockSizes(new int[] {ref.getInodeSize()});

    DataBlockReader.readBlock(tag, raf, sb, inode, 0);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void readOfCompressedBlockWithUnsupportedAlgShouldFail()
      throws Exception {
    sb.setCompressionId(CompressionId.XZ);
    byte[] data = new byte[SuperBlock.DEFAULT_BLOCK_SIZE];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) 0xff;
    }
    DataBlockRef ref = writeBlock(data, 0, data.length);
    System.out.println(ref);

    BasicFileINode inode = new BasicFileINode();
    inode.setFileSize(data.length);
    inode.setBlocksStart(ref.getLocation());
    inode.setBlockSizes(new int[] {ref.getInodeSize()});

    DataBlockReader.readBlock(tag, raf, sb, inode, 0);
  }

  @Test
  public void readOfMlutipleBlocksShouldSucceed() throws Exception {
    byte[] data = new byte[SuperBlock.DEFAULT_BLOCK_SIZE];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) 0xff;
    }
    DataBlockRef ref = writeBlock(data, 0, data.length);
    System.out.println(ref);
    DataBlockRef ref2 = writeBlock(data, 0, data.length);
    System.out.println(ref);

    BasicFileINode inode = new BasicFileINode();
    inode.setFileSize(data.length * 2);
    inode.setBlocksStart(ref.getLocation());
    inode.setBlockSizes(new int[] {ref.getInodeSize(), ref2.getInodeSize()});
    DataBlock block = DataBlockReader.readBlock(tag, raf, sb, inode, 0);
    assertEquals("wrong logical size", data.length, block.getLogicalSize());
    assertEquals("wrong physical size", data.length, block.getPhysicalSize());
    assertArrayEquals("wrong data", data, block.getData());

    block = DataBlockReader.readBlock(tag, raf, sb, inode, 1);
    assertEquals("wrong logical size", data.length, block.getLogicalSize());
    assertEquals("wrong physical size", data.length, block.getPhysicalSize());
    assertArrayEquals("wrong data", data, block.getData());
  }

  @Test
  public void readOfSingleCompressedBlockShouldSucceedWhenFragmentPresent()
      throws Exception {
    byte[] data = new byte[SuperBlock.DEFAULT_BLOCK_SIZE];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) 0xff;
    }
    DataBlockRef ref = writeBlock(data, 0, data.length);
    System.out.println(ref);

    BasicFileINode inode = new BasicFileINode();
    inode.setFileSize(data.length + 1);
    inode.setFragmentBlockIndex(1);
    inode.setFragmentOffset(1);
    inode.setBlocksStart(ref.getLocation());
    inode.setBlockSizes(new int[] {ref.getInodeSize()});
    DataBlock block = DataBlockReader.readBlock(tag, raf, sb, inode, 0);
    assertEquals("wrong logical size", data.length, block.getLogicalSize());
    assertEquals("wrong physical size", data.length, block.getPhysicalSize());
    assertArrayEquals("wrong data", data, block.getData());
  }

  @Test
  public void readOfPartialBlockShouldSucceed() throws Exception {
    byte[] data = new byte[SuperBlock.DEFAULT_BLOCK_SIZE];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) 0xff;
    }
    DataBlockRef ref = writeBlock(data, 0, data.length);
    System.out.println(ref);

    BasicFileINode inode = new BasicFileINode();
    inode.setFileSize(data.length - 1);
    inode.setBlocksStart(ref.getLocation());
    inode.setBlockSizes(new int[] {ref.getInodeSize()});

    DataBlock block = DataBlockReader.readBlock(tag, raf, sb, inode, 0);
    assertEquals("wrong logical size", data.length - 1, block.getLogicalSize());
    assertEquals("wrong physical size", data.length, block.getPhysicalSize());
    assertArrayEquals("wrong data", data, block.getData());
  }

  @Test
  public void readOfSparseBlockShouldSucceed() throws Exception {
    byte[] data = new byte[SuperBlock.DEFAULT_BLOCK_SIZE];
    DataBlockRef ref = writeBlock(data, 0, data.length);
    System.out.println(ref);

    BasicFileINode inode = new BasicFileINode();
    inode.setFileSize(data.length);
    inode.setBlocksStart(ref.getLocation());
    inode.setBlockSizes(new int[] {ref.getInodeSize()});

    DataBlock block = DataBlockReader.readBlock(tag, raf, sb, inode, 0);
    assertEquals("wrong logical size", data.length, block.getLogicalSize());
    assertEquals("wrong physical size", 0, block.getPhysicalSize());
  }

  @Test
  public void readOfSingleUncompressedBlockShouldSucceed() throws Exception {
    Random r = new Random(0L);

    byte[] data = new byte[SuperBlock.DEFAULT_BLOCK_SIZE];
    r.nextBytes(data);

    DataBlockRef ref = writeBlock(data, 0, data.length);
    System.out.println(ref);

    BasicFileINode inode = new BasicFileINode();
    inode.setFileSize(data.length);
    inode.setBlocksStart(ref.getLocation());
    inode.setBlockSizes(new int[] {ref.getInodeSize()});

    DataBlock block = DataBlockReader.readBlock(tag, raf, sb, inode, 0);
    assertEquals("wrong logical size", data.length, block.getLogicalSize());
    assertEquals("wrong physical size", data.length, block.getPhysicalSize());
    assertArrayEquals("wrong data", data, block.getData());
  }

  @Test(expected = SquashFsException.class)
  public void readOfOutOfBoundsBlockShouldFail() throws Exception {
    Random r = new Random(0L);

    byte[] data = new byte[SuperBlock.DEFAULT_BLOCK_SIZE];
    r.nextBytes(data);

    DataBlockRef ref = writeBlock(data, 0, data.length);
    System.out.println(ref);

    BasicFileINode inode = new BasicFileINode();
    inode.setFileSize(data.length);
    inode.setBlocksStart(ref.getLocation());
    inode.setBlockSizes(new int[] {});

    DataBlock block = DataBlockReader.readBlock(tag, raf, sb, inode, 0);
    assertEquals("wrong logical size", data.length, block.getLogicalSize());
    assertEquals("wrong physical size", data.length, block.getPhysicalSize());
    assertArrayEquals("wrong data", data, block.getData());
  }
}
