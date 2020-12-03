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
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

public class TestBasicSymlinkINode {

  private BasicSymlinkINode inode;

  @Before
  public void setUp() {
    inode = new BasicSymlinkINode();
    inode.setNlink(2);
    inode.setTargetPath("/test".getBytes(StandardCharsets.ISO_8859_1));
  }

  @Test
  public void getNameShouldReturnCorrectValue() {
    assertEquals("basic-symlink-inode", inode.getName());
  }

  @Test
  public void getInodeTypeShouldReturnCorrectValue() {
    assertSame(INodeType.BASIC_SYMLINK, inode.getInodeType());
  }

  @Test
  public void nlinkPropertyShouldWorkAsExpected() {
    assertEquals(2, inode.getNlink());
    inode.setNlink(3);
    assertEquals(3, inode.getNlink());
  }

  @Test
  public void targetPathPropertyShouldWorkAsExpected() {
    assertEquals("/test",
        new String(inode.getTargetPath(), StandardCharsets.ISO_8859_1));
    inode.setTargetPath("/test2".getBytes(StandardCharsets.ISO_8859_1));
    assertEquals("/test2",
        new String(inode.getTargetPath(), StandardCharsets.ISO_8859_1));
  }

  @Test
  public void targetPathPropertyShouldConvertNullToEmptyString() {
    inode.setTargetPath(null);
    assertEquals("",
        new String(inode.getTargetPath(), StandardCharsets.ISO_8859_1));
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
  public void simplifyShouldReturnSelf() {
    assertSame(inode, inode.simplify());
  }

  @Test
  public void staticSimplifyMethodShouldReturnOriginalIfBasic() {
    assertSame(inode, BasicSymlinkINode.simplify(inode));
  }

  @Test
  public void getChildSerializedSizeShouldReturnCorrectValue() {
    assertEquals(13, inode.getChildSerializedSize());
    inode.setTargetPath("/test2".getBytes(StandardCharsets.ISO_8859_1));
    assertEquals(14, inode.getChildSerializedSize());
  }

  @Test
  public void writeDataAndReadDataShouldBeReflexive() throws IOException {
    byte[] data = INodeTestUtils.serializeINode(inode);
    INode dest = INodeTestUtils.deserializeINode(data);

    assertSame("Wrong class", inode.getClass(), dest.getClass());
    BasicSymlinkINode bDest = (BasicSymlinkINode) dest;

    assertEquals("Wrong nlink count", 2, bDest.getNlink());
    assertEquals("Wrong target path",
        new String(inode.getTargetPath(), StandardCharsets.ISO_8859_1),
        new String(bDest.getTargetPath(), StandardCharsets.ISO_8859_1));
  }

  @Test
  public void staticSimplifyMethodReturnsOriginalWhenExtendedAttributes() {
    SymlinkINode inode2 = new ExtendedSymlinkINode();
    inode2.setNlink(2);
    inode2.setXattrIndex(3);
    assertSame(inode2, BasicSymlinkINode.simplify(inode2));
  }

  @Test
  public void staticSimplifyMethodReturnsBasicWhenNoExtendedAttributes() {
    SymlinkINode inode2 = new ExtendedSymlinkINode();
    inode2.setNlink(2);
    inode2.setXattrIndex(-1);

    SymlinkINode result = BasicSymlinkINode.simplify(inode2);
    assertSame("wrong class", BasicSymlinkINode.class, result.getClass());
    assertSame("wrong nlink count", 2, result.getNlink());
  }

  @Test
  public void toStringShouldNotFail() {
    System.out.println(inode.toString());
  }

}
