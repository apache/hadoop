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

package org.apache.hadoop.runc.squashfs.inode;

import org.apache.hadoop.runc.squashfs.test.INodeTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestExtendedFileINode {

  private ExtendedFileINode inode;

  @Before
  public void setUp() {
    inode = new ExtendedFileINode();
    inode.setBlocksStart(1L);
    inode.setFragmentBlockIndex(2);
    inode.setFragmentOffset(3);
    inode.setFileSize(131073L);
    inode.setBlockSizes(new int[] {5});
    inode.setSparse(6L);
    inode.setNlink(7);
    inode.setXattrIndex(8);
  }

  @Test
  public void getNameShouldReturnCorrectValue() {
    assertEquals("extended-file-inode", inode.getName());
  }

  @Test
  public void getInodeTypeShouldReturnCorrectValue() {
    assertSame(INodeType.EXTENDED_FILE, inode.getInodeType());
  }

  @Test
  public void blocksStartPropertyShouldWorkAsExpected() {
    assertEquals(1L, inode.getBlocksStart());
    inode.setBlocksStart(2L);
    assertEquals(2L, inode.getBlocksStart());
  }

  @Test
  public void fragmentBlockIndexPropertyShouldWorkAsExpected() {
    assertEquals(2, inode.getFragmentBlockIndex());
    inode.setFragmentBlockIndex(3);
    assertEquals(3, inode.getFragmentBlockIndex());
  }

  @Test
  public void isFragmentPresentShouldReturnTrueIfFragmentBlockIndexSet() {
    assertTrue(inode.isFragmentPresent());
    inode.setFragmentBlockIndex(-1);
    assertFalse(inode.isFragmentPresent());
  }

  @Test
  public void fragmentOffsetPropertyShouldWorkAsExpected() {
    assertEquals(3, inode.getFragmentOffset());
    inode.setFragmentOffset(4);
    assertEquals(4, inode.getFragmentOffset());
  }

  @Test
  public void fileSizePropertyShouldWorkAsExpected() {
    assertEquals(131073L, inode.getFileSize());
    inode.setFileSize(131074L);
    assertEquals(131074L, inode.getFileSize());
  }

  @Test
  public void sparsePropertyShouldWorkAsExpected() {
    assertEquals(6L, inode.getSparse());
    inode.setSparse(7L);
    assertEquals(7L, inode.getSparse());
  }

  @Test
  public void isSparseBlockPresentShouldReturnTrueIfSparseNonZero() {
    assertTrue(inode.isSparseBlockPresent());
    inode.setSparse(0L);
    assertFalse(inode.isSparseBlockPresent());
  }

  @Test
  public void getChildSerializedSizeShouldReturnCorrectValue() {
    assertEquals(44, inode.getChildSerializedSize());
    inode.setBlockSizes(new int[] {1, 2, 3});
    assertEquals(52, inode.getChildSerializedSize());
  }

  @Test
  public void xattrIndexPropertyShouldWorkAsExpected() {
    assertEquals(8, inode.getXattrIndex());
    inode.setXattrIndex(9);
    assertEquals(9, inode.getXattrIndex());
  }

  @Test
  public void isXattrPresentShouldReturnTrueIfPresent() {
    assertTrue(inode.isXattrPresent());
    inode.setXattrIndex(-1);
    assertFalse(inode.isXattrPresent());
  }

  @Test
  public void writeDataAndReadDataWithFragmentsShouldBeReflexive()
      throws IOException {
    byte[] data = INodeTestUtils.serializeINode(inode);
    INode dest = INodeTestUtils.deserializeINode(data);

    assertSame("Wrong class", inode.getClass(), dest.getClass());
    ExtendedFileINode bDest = (ExtendedFileINode) dest;

    assertEquals("wrong blocks start", 1L, bDest.getBlocksStart());
    assertEquals("wrong fragment block index", 2,
        bDest.getFragmentBlockIndex());
    assertEquals("wrong fragment offset", 3, bDest.getFragmentOffset());
    assertEquals("wrong file size", 131073L, bDest.getFileSize());
    assertArrayEquals("wrong block sizes", new int[] {5},
        bDest.getBlockSizes());
    assertEquals("wrong sparse", 6L, bDest.getSparse());
    assertEquals("wrong nlink count", 7, bDest.getNlink());
    assertEquals("wrong xattr index", 8, bDest.getXattrIndex());
  }

  @Test
  public void writeDataAndReadDataWithoutFragmentsShouldBeReflexive()
      throws IOException {
    inode.setFragmentOffset(0);
    inode.setFragmentBlockIndex(-1);
    inode.setFileSize(131072L);

    byte[] data = INodeTestUtils.serializeINode(inode);
    INode dest = INodeTestUtils.deserializeINode(data);

    assertSame("Wrong class", inode.getClass(), dest.getClass());
    ExtendedFileINode bDest = (ExtendedFileINode) dest;

    assertEquals("wrong blocks start", 1L, bDest.getBlocksStart());
    assertEquals("wrong fragment block index", -1,
        bDest.getFragmentBlockIndex());
    assertEquals("wrong fragment offset", 0, bDest.getFragmentOffset());
    assertEquals("wrong file size", 131072L, bDest.getFileSize());
    assertArrayEquals("wrong block sizes", new int[] {5},
        bDest.getBlockSizes());
    assertEquals("wrong sparse", 6L, bDest.getSparse());
    assertEquals("wrong nlink count", 7, bDest.getNlink());
    assertEquals("wrong xattr index", 8, bDest.getXattrIndex());
  }

  @Test
  public void writeDataAndReadDataWithShortEndBlockShouldBeReflexive()
      throws IOException {
    inode.setFragmentOffset(0);
    inode.setFragmentBlockIndex(-1);
    inode.setFileSize(131071L);

    byte[] data = INodeTestUtils.serializeINode(inode);
    INode dest = INodeTestUtils.deserializeINode(data);

    assertSame("Wrong class", inode.getClass(), dest.getClass());
    ExtendedFileINode bDest = (ExtendedFileINode) dest;

    assertEquals("wrong blocks start", 1L, bDest.getBlocksStart());
    assertEquals("wrong fragment block index", -1,
        bDest.getFragmentBlockIndex());
    assertEquals("wrong fragment offset", 0, bDest.getFragmentOffset());
    assertEquals("wrong file size", 131071L, bDest.getFileSize());
    assertArrayEquals("wrong block sizes", new int[] {5},
        bDest.getBlockSizes());
    assertEquals("wrong file size", 131071L, bDest.getFileSize());
    assertEquals("wrong sparse", 6L, bDest.getSparse());
    assertEquals("wrong nlink count", 7, bDest.getNlink());
    assertEquals("wrong xattr index", 8, bDest.getXattrIndex());
  }

  @Test
  public void simplifyShouldReturnBasicIfExtendedAttributesNotNeeded() {
    FileINode inode2 = new ExtendedFileINode();
    inode2.setBlocksStart(1L);
    inode2.setNlink(1);
    inode2.setFragmentBlockIndex(2);
    inode2.setFragmentOffset(3);
    inode2.setFileSize(131073L);
    inode2.setBlockSizes(new int[] {5});

    FileINode result = inode2.simplify();
    assertSame("wrong class", BasicFileINode.class, result.getClass());

    assertEquals("wrong block start", inode2.getBlocksStart(),
        result.getBlocksStart());
    assertEquals("wrong fragment block index", inode2.getFragmentBlockIndex(),
        result.getFragmentBlockIndex());
    assertEquals("wrong fragment offset", inode2.getFragmentOffset(),
        result.getFragmentOffset());
    assertEquals("wrong file size", inode2.getFileSize(), result.getFileSize());
    assertArrayEquals("wrong block sizes", inode2.getBlockSizes(),
        result.getBlockSizes());
  }

  @Test
  public void simplifyShouldReturnOriginalIfLinkCountGreaterThanOne() {
    FileINode inode2 = new ExtendedFileINode();
    inode2.setBlocksStart(1L);
    inode2.setNlink(2);
    inode2.setFragmentBlockIndex(2);
    inode2.setFragmentOffset(3);
    inode2.setFileSize(131073L);
    inode2.setBlockSizes(new int[] {5});
    assertSame(inode2, inode2.simplify());
  }

  @Test
  public void simplifyShouldReturnOriginalIfExtendedAttributes() {
    FileINode inode2 = new ExtendedFileINode();
    inode2.setBlocksStart(1L);
    inode2.setNlink(1);
    inode2.setXattrIndex(1);
    inode2.setFragmentBlockIndex(2);
    inode2.setFragmentOffset(3);
    inode2.setFileSize(131073L);
    inode2.setBlockSizes(new int[] {5});
    assertSame(inode2, inode2.simplify());
  }

  @Test
  public void simplifyShouldReturnOriginalIfSparse() {
    FileINode inode2 = new ExtendedFileINode();
    inode2.setBlocksStart(1L);
    inode2.setNlink(1);
    inode2.setSparse(3L);
    inode2.setFragmentBlockIndex(2);
    inode2.setFragmentOffset(3);
    inode2.setFileSize(131073L);
    inode2.setBlockSizes(new int[] {5});
    assertSame(inode2, inode2.simplify());
  }

  @Test
  public void simplifyShouldReturnOriginalIfBlocksStartTooLarge() {
    FileINode inode2 = new ExtendedFileINode();
    inode2.setBlocksStart(0x1_0000_0000L);
    inode2.setNlink(1);
    inode2.setFragmentBlockIndex(2);
    inode2.setFragmentOffset(3);
    inode2.setFileSize(131073L);
    inode2.setBlockSizes(new int[] {5});
    assertSame(inode2, inode2.simplify());
  }

  @Test
  public void simplifyShouldReturnOriginalIfFileSizeTooLarge() {
    FileINode inode2 = new ExtendedFileINode();
    inode2.setBlocksStart(1L);
    inode2.setNlink(1);
    inode2.setFragmentBlockIndex(2);
    inode2.setFragmentOffset(3);
    inode2.setFileSize(0x1_0000_0000L);
    inode2.setBlockSizes(new int[] {5});
    assertSame(inode2, inode2.simplify());
  }

  @Test
  public void toStringShouldNotFail() {
    System.out.println(inode.toString());
  }

}
