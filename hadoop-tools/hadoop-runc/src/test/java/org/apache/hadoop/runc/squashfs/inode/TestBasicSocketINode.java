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

public class TestBasicSocketINode {

  private BasicSocketINode inode;

  @Before
  public void setUp() {
    inode = new BasicSocketINode();
    inode.setNlink(2);
  }

  @Test
  public void getNameShouldReturnCorrectValue() {
    assertEquals("basic-socket-inode", inode.getName());
  }

  @Test
  public void getInodeTypeShouldReturnCorrectValue() {
    assertSame(INodeType.BASIC_SOCKET, inode.getInodeType());
  }

  @Test
  public void simplifyShouldReturnSelf() {
    assertSame(inode, inode.simplify());
  }

  @Test
  public void staticSimplifyMethodShouldReturnOriginalIfBasic() {
    assertSame(inode, BasicSocketINode.simplify(inode));
  }

  @Test
  public void staticSimplifyMethodReturnsOriginalWhentendedAttributes() {
    SocketINode inode2 = new ExtendedSocketINode();
    inode2.setNlink(2);
    inode2.setXattrIndex(3);
    assertSame(inode2, BasicSocketINode.simplify(inode2));
  }

  @Test
  public void staticSimplifyMethodReturnsBasicWhenNoExtendedAttributes() {
    SocketINode inode2 = new ExtendedSocketINode();
    inode2.setNlink(2);
    inode2.setXattrIndex(-1);

    SocketINode result = BasicSocketINode.simplify(inode2);
    assertSame("wrong class", BasicSocketINode.class, result.getClass());
    assertSame("wrong nlink count", 2, result.getNlink());
  }

  @Test
  public void toStringShouldNotFail() {
    System.out.println(inode.toString());
  }

}
