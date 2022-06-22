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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

public class TestBasicDirectoryINode {

  private BasicDirectoryINode inode;

  @Before
  public void setUp() {
    inode = new BasicDirectoryINode();
    inode.setStartBlock(1);
    inode.setNlink(2);
    inode.setFileSize(3);
    inode.setOffset((short) 4);
    inode.setParentInodeNumber(5);
  }

  @Test
  public void getNameShouldReturnCorrectValue() {
    assertEquals("basic-directory-inode", inode.getName());
  }

  @Test
  public void getInodeTypeShouldReturnCorrectValue() {
    assertSame(INodeType.BASIC_DIRECTORY, inode.getInodeType());
  }

  @Test
  public void startBlockPropertyShouldWorkAsExpected() {
    assertEquals(1, inode.getStartBlock());
    inode.setStartBlock(2);
    assertEquals(2, inode.getStartBlock());
  }

  @Test
  public void nlinkPropertyShouldWorkAsExpected() {
    assertEquals(2, inode.getNlink());
    inode.setNlink(3);
    assertEquals(3, inode.getNlink());
  }

  @Test
  public void fileSizePropertyShouldWorkAsExpected() {
    assertEquals(3, inode.getFileSize());
    inode.setFileSize(4);
    assertEquals(4, inode.getFileSize());
  }

  @Test(expected = IllegalArgumentException.class)
  public void fileSizePropertyShouldNotAllowMoreThanSixyFourKilobytes() {
    inode.setFileSize(65536);
  }

  @Test
  public void offsetPropertyShouldWorkAsExpected() {
    assertEquals((short) 4, inode.getOffset());
    inode.setOffset((short) 5);
    assertEquals((short) 5, inode.getOffset());
  }

  @Test
  public void parentInodeNumberPropertyShouldWorkAsExpected() {
    assertEquals(5, inode.getParentInodeNumber());
    inode.setParentInodeNumber(6);
    assertEquals(6, inode.getParentInodeNumber());
  }

  @Test
  public void getIndexCountShouldReturnZero() {
    assertEquals((short) 0, inode.getIndexCount());
  }

  @Test
  public void setSetIndexCountWithValidValueShouldSucceed() {
    inode.setIndexCount((short) 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void setSetIndexCountWithInvalidValueShouldFail() {
    inode.setIndexCount((short) 1);
  }

  @Test
  public void isIndexPresentShouldReturnFalse() {
    assertFalse(inode.isIndexPresent());
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
    assertEquals(16, inode.getChildSerializedSize());
  }

  @Test
  public void simplifyShouldReturnSelf() {
    assertSame(inode, inode.simplify());
  }

  @Test
  public void writeDataAndReadDataShouldBeReflexive() throws IOException {
    byte[] data = INodeTestUtils.serializeINode(inode);
    INode dest = INodeTestUtils.deserializeINode(data);

    assertSame("Wrong class", inode.getClass(), dest.getClass());
    BasicDirectoryINode bDest = (BasicDirectoryINode) dest;

    assertEquals("wrong start block", 1, bDest.getStartBlock());
    assertEquals("wrong nlink count", 2, bDest.getNlink());
    assertEquals("wrong file size", 3, bDest.getFileSize());
    assertEquals("wrong offset", (short) 4, bDest.getOffset());
    assertEquals("wrong parent inode number", 5, bDest.getParentInodeNumber());
  }

  @Test
  public void staticSimplifyMethodShouldReturnOriginalIfBasic() {
    assertSame(inode, BasicDirectoryINode.simplify(inode));
  }

  @Test
  public void staticSimplifyMethodReturnsBasicWhenNoExtendedAttributes() {
    DirectoryINode inode2 = new ExtendedDirectoryINode();
    inode2.setStartBlock(1);
    inode2.setNlink(2);
    inode2.setFileSize(3);
    inode2.setOffset((short) 4);
    inode2.setParentInodeNumber(5);
    inode2.setIndexCount((short) 0);
    inode2.setXattrIndex(-1);

    DirectoryINode result = BasicDirectoryINode.simplify(inode2);
    assertSame("wrong class", BasicDirectoryINode.class, result.getClass());

    assertEquals("wrong start block", inode2.getStartBlock(),
        result.getStartBlock());
    assertEquals("wrong nlink count", inode2.getNlink(), result.getNlink());
    assertEquals("wrong file size", inode2.getFileSize(), result.getFileSize());
    assertEquals("wrong offset", inode2.getOffset(), result.getOffset());
    assertEquals("wrong parent inode number", inode2.getParentInodeNumber(),
        result.getParentInodeNumber());
  }

  @Test
  public void staticSimplifyMethodReturnsOriginalWhenFileSizeTooLarge() {
    DirectoryINode inode2 = new ExtendedDirectoryINode();
    inode2.setStartBlock(1);
    inode2.setNlink(2);
    inode2.setFileSize(65536);
    inode2.setOffset((short) 4);
    inode2.setParentInodeNumber(5);
    inode2.setIndexCount((short) 0);
    inode2.setXattrIndex(-1);
    assertSame(inode2, BasicDirectoryINode.simplify(inode2));
  }

  @Test
  public void staticSimplifyMethodShouldReturnOriginalIfIndexPresent() {
    DirectoryINode inode2 = new ExtendedDirectoryINode();
    inode2.setStartBlock(1);
    inode2.setNlink(2);
    inode2.setFileSize(3);
    inode2.setOffset((short) 4);
    inode2.setParentInodeNumber(5);
    inode2.setIndexCount((short) 1);
    inode2.setXattrIndex(-1);
    assertSame(inode2, BasicDirectoryINode.simplify(inode2));
  }

  @Test
  public void staticSimplifyMethodReturnsOriginalWhenExtendedAttributes() {
    DirectoryINode inode2 = new ExtendedDirectoryINode();
    inode2.setStartBlock(1);
    inode2.setNlink(2);
    inode2.setFileSize(3);
    inode2.setOffset((short) 4);
    inode2.setParentInodeNumber(5);
    inode2.setIndexCount((short) 0);
    inode2.setXattrIndex(1);
    assertSame(inode2, BasicDirectoryINode.simplify(inode2));
  }

  @Test
  public void toStringShouldNotFail() {
    System.out.println(inode.toString());
  }

}
