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

public class TestBasicFileINode {

  private BasicFileINode inode;

  @Before
  public void setUp() {
    inode = new BasicFileINode();
    inode.setBlocksStart(1L);
    inode.setFragmentBlockIndex(2);
    inode.setFragmentOffset(3);
    inode.setFileSize(131073L);
    inode.setBlockSizes(new int[] {5});
  }

  @Test
  public void getNameShouldReturnCorrectValue() {
    assertEquals("basic-file-inode", inode.getName());
  }

  @Test
  public void getInodeTypeShouldReturnCorrectValue() {
    assertSame(INodeType.BASIC_FILE, inode.getInodeType());
  }

  @Test
  public void blocksStartPropertyShouldWorkAsExpected() {
    assertEquals(1L, inode.getBlocksStart());
    inode.setBlocksStart(2L);
    assertEquals(2L, inode.getBlocksStart());
  }

  @Test(expected = IllegalArgumentException.class)
  public void blocksStartPropertyShouldNotAllowMoreThanFourGigabytes() {
    inode.setBlocksStart(0x1_0000_0000L);
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
  public void getSparseShouldReturnZero() {
    assertEquals(0L, inode.getSparse());
  }

  @Test
  public void setSparseShouldAllowSettingZero() {
    inode.setSparse(0L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void setSparseShouldNotAllowSettingMoreThanZero() {
    inode.setSparse(1L);
  }

  @Test
  public void isSparseBlockPresentShouldReturnFalse() {
    assertFalse(inode.isSparseBlockPresent());
  }

  @Test
  public void getNlinkShouldReturnOne() {
    assertEquals(1, inode.getNlink());
  }

  @Test
  public void setNlinkShouldAllowSettingOne() {
    inode.setNlink(1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void setNlinkShouldNotAllowSettingMoreThanOne() {
    inode.setNlink(2);
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

  @Test(expected = IllegalArgumentException.class)
  public void fileSizePropertyShouldNotAllowMoreThanFourGigabytes() {
    inode.setFileSize(0x1_0000_0000L);
  }

  @Test
  public void getXattrIndexShouldReturnNotPresent() {
    assertEquals(-1, inode.getXattrIndex());
  }

  @Test
  public void setXattrIndexWithNotPresentValueShouldSucceed() {
    inode.setXattrIndex(-1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void setXattrIndexWithInvalidValueShouldFail() {
    inode.setXattrIndex(1);
  }

  @Test
  public void isXattrPresentShouldReturnFalse() {
    assertFalse(inode.isXattrPresent());
  }

  @Test
  public void getChildSerializedSizeShouldReturnCorrectValue() {
    assertEquals(20, inode.getChildSerializedSize());
    inode.setBlockSizes(new int[] {1, 2, 3});
    assertEquals(28, inode.getChildSerializedSize());
  }

  @Test
  public void simplifyShouldReturnSelf() {
    assertSame(inode, inode.simplify());
  }

  @Test
  public void writeDataAndReadDataWithFragmentsShouldBeReflexive()
      throws IOException {
    byte[] data = INodeTestUtils.serializeINode(inode);
    INode dest = INodeTestUtils.deserializeINode(data);

    assertSame("Wrong class", inode.getClass(), dest.getClass());
    BasicFileINode bDest = (BasicFileINode) dest;

    assertEquals("wrong blocks start", 1L, bDest.getBlocksStart());
    assertEquals("wrong fragment block index", 2,
        bDest.getFragmentBlockIndex());
    assertEquals("wrong fragment offset", 3, bDest.getFragmentOffset());
    assertEquals("wrong file size", 131073L, bDest.getFileSize());
    assertArrayEquals("wrong block sizes", new int[] {5},
        bDest.getBlockSizes());
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
    BasicFileINode bDest = (BasicFileINode) dest;

    assertEquals("wrong blocks start", 1L, bDest.getBlocksStart());
    assertEquals("wrong fragment block index", -1,
        bDest.getFragmentBlockIndex());
    assertEquals("wrong fragment offset", 0, bDest.getFragmentOffset());
    assertEquals("wrong file size", 131072L, bDest.getFileSize());
    assertArrayEquals("wrong block sizes", new int[] {5},
        bDest.getBlockSizes());
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
    BasicFileINode bDest = (BasicFileINode) dest;

    assertEquals("wrong blocks start", 1L, bDest.getBlocksStart());
    assertEquals("wrong fragment block index", -1,
        bDest.getFragmentBlockIndex());
    assertEquals("wrong fragment offset", 0, bDest.getFragmentOffset());
    assertEquals("wrong file size", 131071L, bDest.getFileSize());
    assertArrayEquals("wrong block sizes", new int[] {5},
        bDest.getBlockSizes());
  }

  @Test
  public void staticSimplifyMethodShouldReturnOriginalIfBasic() {
    assertSame(inode, BasicFileINode.simplify(inode));
  }

  @Test
  public void staticSimplifyMethodReturnsBasicWhenNoExtendedAttributes() {
    FileINode inode2 = new ExtendedFileINode();
    inode2.setBlocksStart(1L);
    inode2.setNlink(1);
    inode2.setFragmentBlockIndex(2);
    inode2.setFragmentOffset(3);
    inode2.setFileSize(131073L);
    inode2.setBlockSizes(new int[] {5});

    FileINode result = BasicFileINode.simplify(inode2);
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
  public void staticSimplifyMethodReturnsOriginalIfLinkCountGreaterThanOne() {
    FileINode inode2 = new ExtendedFileINode();
    inode2.setBlocksStart(1L);
    inode2.setNlink(2);
    inode2.setFragmentBlockIndex(2);
    inode2.setFragmentOffset(3);
    inode2.setFileSize(131073L);
    inode2.setBlockSizes(new int[] {5});
    assertSame(inode2, BasicFileINode.simplify(inode2));
  }

  @Test
  public void staticSimplifyMethodShouldReturnOriginalIfExtendedAttributes() {
    FileINode inode2 = new ExtendedFileINode();
    inode2.setBlocksStart(1L);
    inode2.setNlink(1);
    inode2.setXattrIndex(1);
    inode2.setFragmentBlockIndex(2);
    inode2.setFragmentOffset(3);
    inode2.setFileSize(131073L);
    inode2.setBlockSizes(new int[] {5});
    assertSame(inode2, BasicFileINode.simplify(inode2));
  }

  @Test
  public void staticSimplifyMethodShouldReturnOriginalIfSparse() {
    FileINode inode2 = new ExtendedFileINode();
    inode2.setBlocksStart(1L);
    inode2.setNlink(1);
    inode2.setSparse(3L);
    inode2.setFragmentBlockIndex(2);
    inode2.setFragmentOffset(3);
    inode2.setFileSize(131073L);
    inode2.setBlockSizes(new int[] {5});
    assertSame(inode2, BasicFileINode.simplify(inode2));
  }

  @Test
  public void staticSimplifyMethodShouldReturnOriginalIfBlocksStartTooLarge() {
    FileINode inode2 = new ExtendedFileINode();
    inode2.setBlocksStart(0x1_0000_0000L);
    inode2.setNlink(1);
    inode2.setFragmentBlockIndex(2);
    inode2.setFragmentOffset(3);
    inode2.setFileSize(131073L);
    inode2.setBlockSizes(new int[] {5});
    assertSame(inode2, BasicFileINode.simplify(inode2));
  }

  @Test
  public void staticSimplifyMethodShouldReturnOriginalIfFileSizeTooLarge() {
    FileINode inode2 = new ExtendedFileINode();
    inode2.setBlocksStart(1L);
    inode2.setNlink(1);
    inode2.setFragmentBlockIndex(2);
    inode2.setFragmentOffset(3);
    inode2.setFileSize(0x1_0000_0000L);
    inode2.setBlockSizes(new int[] {5});
    assertSame(inode2, BasicFileINode.simplify(inode2));
  }

  @Test
  public void toStringShouldNotFail() {
    System.out.println(inode.toString());
  }

}
