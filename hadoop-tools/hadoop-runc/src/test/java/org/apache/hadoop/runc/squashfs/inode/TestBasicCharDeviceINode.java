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

public class TestBasicCharDeviceINode {

  private BasicCharDeviceINode inode;

  @Before
  public void setUp() {
    inode = new BasicCharDeviceINode();
    inode.setDevice(1);
    inode.setNlink(2);
  }

  @Test
  public void getNameShouldReturnCorrectValue() {
    assertEquals("basic-char-dev-inode", inode.getName());
  }

  @Test
  public void getInodeTypeShouldReturnCorrectValue() {
    assertSame(INodeType.BASIC_CHAR_DEVICE, inode.getInodeType());
  }

  @Test
  public void simplifyShouldReturnSelf() {
    assertSame(inode, inode.simplify());
  }

  @Test
  public void staticSimplifyMethodShouldReturnOriginalIfBasic() {
    assertSame(inode, BasicCharDeviceINode.simplify(inode));
  }

  @Test
  public void staticSimplifyMethodReturnsOriginalWhenExtendedAttributes() {
    CharDeviceINode inode2 = new ExtendedCharDeviceINode();
    inode2.setDevice(1);
    inode2.setNlink(2);
    inode2.setXattrIndex(3);
    assertSame(inode2, BasicCharDeviceINode.simplify(inode2));
  }

  @Test
  public void staticSimplifyMethodReturnsBasicWhenNoExtendedAttributes() {
    CharDeviceINode inode2 = new ExtendedCharDeviceINode();
    inode2.setDevice(1);
    inode2.setNlink(2);
    inode2.setXattrIndex(-1);

    CharDeviceINode result = BasicCharDeviceINode.simplify(inode2);
    assertSame("wrong class", BasicCharDeviceINode.class, result.getClass());
    assertSame("wrong device", 1, result.getDevice());
    assertSame("wrong nlink count", 2, result.getNlink());
  }

  @Test
  public void toStringShouldNotFail() {
    System.out.println(inode.toString());
  }

}
