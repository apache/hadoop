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
import static org.junit.Assert.assertSame;

public class TestAbstractINode {

  private AbstractINode inode;
  private int modifiedTime;

  @Before
  public void setUp() {
    inode = new BasicFifoINode(); // pick a simple one
    inode.setUidIdx((short) 1);
    inode.setGidIdx((short) 2);
    inode.setInodeNumber(3);
    inode.setPermissions((short) 0755);
    modifiedTime = (int) (System.currentTimeMillis() / 1000L);
    inode.setModifiedTime(modifiedTime);
  }

  @Test
  public void copyToShouldDuplicateCommonINodeProperties() {
    INode dest = new BasicSymlinkINode();
    inode.copyTo(dest);
    assertEquals("Wrong uidIdx", (short) 1, dest.getUidIdx());
    assertEquals("Wrong gidIdx", (short) 2, dest.getGidIdx());
    assertEquals("Wrong inodeNumber", 3, dest.getInodeNumber());
    assertEquals("Wrong permissions", (short) 0755, dest.getPermissions());
    assertEquals("Wrong modifiedTime", modifiedTime, dest.getModifiedTime());
  }

  @Test
  public void getSerializedSizeShouldReturnSixteenPlusWhateverChildNeeds() {
    assertEquals(16 + inode.getChildSerializedSize(),
        inode.getSerializedSize());
  }

  @Test
  public void uidIdxPropertyShouldWorkAsExpected() {
    assertEquals((short) 1, inode.getUidIdx());
    inode.setUidIdx((short) 2);
    assertEquals((short) 2, inode.getUidIdx());
  }

  @Test
  public void gidIdxPropertyShouldWorkAsExpected() {
    assertEquals((short) 2, inode.getGidIdx());
    inode.setGidIdx((short) 3);
    assertEquals((short) 3, inode.getGidIdx());
  }

  @Test
  public void inodeNumberPropertyShouldWorkAsExpected() {
    assertEquals(3, inode.getInodeNumber());
    inode.setInodeNumber(4);
    assertEquals(4, inode.getInodeNumber());
  }

  @Test
  public void permissionsPropertyShouldWorkAsExpected() {
    assertEquals((short) 0755, inode.getPermissions());
    inode.setPermissions((short) 0644);
    assertEquals((short) 0644, inode.getPermissions());
  }

  @Test
  public void modifiedTimePropertyShouldWorkAsExpected() {
    assertEquals(modifiedTime, inode.getModifiedTime());
    inode.setModifiedTime(modifiedTime + 1);
    assertEquals(modifiedTime + 1, inode.getModifiedTime());
  }

  @Test
  public void writeDataAndReadDataShouldBeReflexive() throws IOException {
    byte[] data = INodeTestUtils.serializeINode(inode);
    INode dest = INodeTestUtils.deserializeINode(data);

    assertSame("Wrong class", inode.getClass(), dest.getClass());
    assertEquals("Wrong uidIdx", (short) 1, dest.getUidIdx());
    assertEquals("Wrong gidIdx", (short) 2, dest.getGidIdx());
    assertEquals("Wrong inodeNumber", 3, dest.getInodeNumber());
    assertEquals("Wrong permissions", (short) 0755, dest.getPermissions());
    assertEquals("Wrong modifiedTime", modifiedTime, dest.getModifiedTime());
  }

  @Test
  public void toStringShouldNotFail() {
    System.out.println(inode.toString());
  }

}
