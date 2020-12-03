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

public class TestAbstractBasicIpcINode {

  private AbstractBasicIpcINode inode;

  @Before
  public void setUp() {
    inode = new BasicSocketINode();
    inode.setNlink(2);
  }

  @Test
  public void nlinkPropertyShouldWorkAsExpected() {
    assertEquals(2, inode.getNlink());
    inode.setNlink(3);
    assertEquals(3, inode.getNlink());
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
    assertEquals(4, inode.getChildSerializedSize());
  }

  @Test
  public void writeDataAndReadDataShouldBeReflexive() throws IOException {
    byte[] data = INodeTestUtils.serializeINode(inode);
    INode dest = INodeTestUtils.deserializeINode(data);

    assertSame("Wrong class", inode.getClass(), dest.getClass());
    AbstractBasicIpcINode bDest = (AbstractBasicIpcINode) dest;

    assertEquals("Wrong nlink count", 2, bDest.getNlink());
  }

  @Test
  public void toStringShouldNotFail() {
    System.out.println(inode.toString());
  }
}
