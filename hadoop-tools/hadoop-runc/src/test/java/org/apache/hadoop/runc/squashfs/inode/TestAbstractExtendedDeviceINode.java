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

public class TestAbstractExtendedDeviceINode {

  private AbstractExtendedDeviceINode inode;

  @Before
  public void setUp() {
    inode = new ExtendedBlockDeviceINode();
    inode.setDevice(1);
    inode.setNlink(2);
    inode.setXattrIndex(3);
  }

  @Test
  public void devicePropertyShouldWorkAsExpected() {
    assertEquals(1, inode.getDevice());
    inode.setDevice(2);
    assertEquals(2, inode.getDevice());
  }

  @Test
  public void nlinkPropertyShouldWorkAsExpected() {
    assertEquals(2, inode.getNlink());
    inode.setNlink(3);
    assertEquals(3, inode.getNlink());
  }

  @Test
  public void xattrIndexPropertyShouldWorkAsExpected() {
    assertEquals(3, inode.getXattrIndex());
    inode.setXattrIndex(4);
    assertEquals(4, inode.getXattrIndex());
  }

  @Test
  public void isXattrPresentShouldReturnTrueIfPresent() {
    assertTrue(inode.isXattrPresent());
    inode.setXattrIndex(-1);
    assertFalse(inode.isXattrPresent());
  }

  @Test
  public void getChildSerializedSizeShouldReturnCorrectValue() {
    assertEquals(12, inode.getChildSerializedSize());
  }

  @Test
  public void writeDataAndReadDataShouldBeReflexive() throws IOException {
    byte[] data = INodeTestUtils.serializeINode(inode);
    INode dest = INodeTestUtils.deserializeINode(data);

    assertSame("Wrong class", inode.getClass(), dest.getClass());
    AbstractExtendedDeviceINode bDest = (AbstractExtendedDeviceINode) dest;

    assertEquals("Wrong device", 1, bDest.getDevice());
    assertEquals("Wrong nlink count", 2, bDest.getNlink());
    assertEquals("Wrong xattr index", 3, bDest.getXattrIndex());
  }

  @Test
  public void toStringShouldNotFail() {
    System.out.println(inode.toString());
  }
}
