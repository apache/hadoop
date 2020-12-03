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
import static org.junit.Assert.assertTrue;

public class TestExtendedDirectoryINode {

  private ExtendedDirectoryINode inode;

  @Before
  public void setUp() {
    inode = new ExtendedDirectoryINode();
    inode.setStartBlock(1);
    inode.setNlink(2);
    inode.setFileSize(3);
    inode.setOffset((short) 4);
    inode.setParentInodeNumber(5);
    inode.setIndexCount((short) 6);
    inode.setXattrIndex(7);
  }

  @Test
  public void getNameShouldReturnCorrectValue() {
    assertEquals("extended-directory-inode", inode.getName());
  }

  @Test
  public void getInodeTypeShouldReturnCorrectValue() {
    assertSame(INodeType.EXTENDED_DIRECTORY, inode.getInodeType());
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
  public void indexCountPropertyShouldWorkAsExpected() {
    assertEquals((short) 6, inode.getIndexCount());
    inode.setIndexCount((short) 7);
    assertEquals((short) 7, inode.getIndexCount());
  }

  @Test
  public void isIndexPresentShouldReturnCorrectValue() {
    assertTrue(inode.isIndexPresent());
    inode.setIndexCount((short) 0);
    assertFalse(inode.isIndexPresent());
  }

  @Test
  public void xattrIndexPropertyShouldWorkAsExpected() {
    assertEquals(7, inode.getXattrIndex());
    inode.setXattrIndex(8);
    assertEquals(8, inode.getXattrIndex());
  }

  @Test
  public void isXattrPresentShouldReturnTrueIfPresent() {
    assertTrue(inode.isXattrPresent());
    inode.setXattrIndex(-1);
    assertFalse(inode.isXattrPresent());
  }

  @Test
  public void getChildSerializedSizeShouldReturnCorrectValue() {
    assertEquals(24, inode.getChildSerializedSize());
  }

  @Test
  public void writeDataAndReadDataShouldBeReflexive() throws IOException {
    byte[] data = INodeTestUtils.serializeINode(inode);
    INode dest = INodeTestUtils.deserializeINode(data);

    assertSame("Wrong class", inode.getClass(), dest.getClass());
    ExtendedDirectoryINode bDest = (ExtendedDirectoryINode) dest;

    assertEquals("wrong start block", 1, bDest.getStartBlock());
    assertEquals("wrong nlink count", 2, bDest.getNlink());
    assertEquals("wrong file size", 3, bDest.getFileSize());
    assertEquals("wrong offset", (short) 4, bDest.getOffset());
    assertEquals("wrong parent inode number", 5, bDest.getParentInodeNumber());
    assertEquals("wrong index count", (short) 6, bDest.getIndexCount());
    assertEquals("wrong xattr index", 7, bDest.getXattrIndex());
  }

  @Test
  public void simplifyShouldReturnBasicIfExtendedAttributesNotNeeded() {
    DirectoryINode inode2 = new ExtendedDirectoryINode();
    inode2.setStartBlock(1);
    inode2.setNlink(2);
    inode2.setFileSize(3);
    inode2.setOffset((short) 4);
    inode2.setParentInodeNumber(5);
    inode2.setIndexCount((short) 0);
    inode2.setXattrIndex(-1);

    DirectoryINode result = inode2.simplify();
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
  public void simplifyShouldReturnOriginalIfFileSizeTooLarge() {
    DirectoryINode inode2 = new ExtendedDirectoryINode();
    inode2.setStartBlock(1);
    inode2.setNlink(2);
    inode2.setFileSize(65536);
    inode2.setOffset((short) 4);
    inode2.setParentInodeNumber(5);
    inode2.setIndexCount((short) 0);
    inode2.setXattrIndex(-1);
    assertSame(inode2, inode2.simplify());
  }

  @Test
  public void simplifyShouldReturnOriginalIfIndexPresent() {
    DirectoryINode inode2 = new ExtendedDirectoryINode();
    inode2.setStartBlock(1);
    inode2.setNlink(2);
    inode2.setFileSize(3);
    inode2.setOffset((short) 4);
    inode2.setParentInodeNumber(5);
    inode2.setIndexCount((short) 1);
    inode2.setXattrIndex(-1);
    assertSame(inode2, inode2.simplify());
  }

  @Test
  public void simplifyShouldReturnOriginalIfExtendedAttributesPresent() {
    DirectoryINode inode2 = new ExtendedDirectoryINode();
    inode2.setStartBlock(1);
    inode2.setNlink(2);
    inode2.setFileSize(3);
    inode2.setOffset((short) 4);
    inode2.setParentInodeNumber(5);
    inode2.setIndexCount((short) 0);
    inode2.setXattrIndex(1);
    assertSame(inode2, inode2.simplify());
  }

  @Test
  public void toStringShouldNotFail() {
    System.out.println(inode.toString());
  }

}
