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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class TestExtendedBlockDeviceINode {

  private ExtendedBlockDeviceINode inode;

  @Before
  public void setUp() {
    inode = new ExtendedBlockDeviceINode();
    inode.setDevice(1);
    inode.setNlink(2);
    inode.setXattrIndex(3);
  }

  @Test
  public void getNameShouldReturnCorrectValue() {
    assertEquals("extended-block-dev-inode", inode.getName());
  }

  @Test
  public void getInodeTypeShouldReturnCorrectValue() {
    assertSame(INodeType.EXTENDED_BLOCK_DEVICE, inode.getInodeType());
  }

  @Test
  public void simplifyShouldReturnOriginalIfExtendedAttributesPresent() {
    BlockDeviceINode inode2 = new ExtendedBlockDeviceINode();
    inode2.setDevice(1);
    inode2.setNlink(2);
    inode2.setXattrIndex(3);
    assertSame(inode2, inode2.simplify());
  }

  @Test
  public void simplifyShouldReturnBasicIfExtendedAttributesNotPresent() {
    BlockDeviceINode inode2 = new ExtendedBlockDeviceINode();
    inode2.setDevice(1);
    inode2.setNlink(2);
    inode2.setXattrIndex(-1);

    BlockDeviceINode result = inode2.simplify();
    assertSame("wrong class", BasicBlockDeviceINode.class, result.getClass());
    assertSame("wrong device", 1, result.getDevice());
    assertSame("wrong nlink count", 2, result.getNlink());
  }

  @Test
  public void toStringShouldNotFail() {
    System.out.println(inode.toString());
  }

}
