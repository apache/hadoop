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

package org.apache.hadoop.runc.squashfs.metadata;

import org.apache.hadoop.runc.squashfs.SquashFsException;
import org.apache.hadoop.runc.squashfs.directory.DirectoryEntry;
import org.apache.hadoop.runc.squashfs.directory.DirectoryTestAccessor;
import org.apache.hadoop.runc.squashfs.directory.DirectoryHeader;
import org.apache.hadoop.runc.squashfs.inode.BasicDirectoryINode;
import org.apache.hadoop.runc.squashfs.inode.INodeRef;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestMetadataReference {

  @Test
  public void inodeWithInodeRefShouldBeRelativeToInodeTable() throws Exception {
    SuperBlock sb = new SuperBlock();
    sb.setInodeTableStart(12345L);

    INodeRef inodeRef = new INodeRef(54321, (short) 1234);
    MetadataReference ref =
        MetadataReference.inode(10101, sb, inodeRef.getRaw());
    System.out.println(ref);
    assertEquals(10101, ref.getTag());
    assertEquals(66666L, ref.getBlockLocation());
    assertEquals((short) 1234, ref.getOffset());
    assertEquals(Integer.MAX_VALUE, ref.getMaxLength());
  }

  @Test(expected = SquashFsException.class)
  public void inodeWithInodeRefShouldFailIfOffsetIsTooLarge() throws Exception {
    SuperBlock sb = new SuperBlock();
    sb.setInodeTableStart(12345L);

    INodeRef inodeRef = new INodeRef(54321, (short) 8192);
    MetadataReference.inode(1234, sb, inodeRef.getRaw());
  }

  @Test
  public void inodeWithDirectoryEntryShouldBeRelativeToInodeTable()
      throws Exception {
    SuperBlock sb = new SuperBlock();
    sb.setInodeTableStart(12345L);

    DirectoryHeader dh = DirectoryTestAccessor.createDirectoryHeader(
        0, 54321, 0);
    DirectoryEntry de = DirectoryTestAccessor.createDirectoryEntry(
        (short) 1234, (short) 0, (short) 0, (short) 0, new byte[0], dh);

    MetadataReference ref = MetadataReference.inode(10101, sb, de);
    System.out.println(ref);
    assertEquals(10101, ref.getTag());
    assertEquals(66666L, ref.getBlockLocation());
    assertEquals((short) 1234, ref.getOffset());
    assertEquals(Integer.MAX_VALUE, ref.getMaxLength());
  }

  @Test(expected = SquashFsException.class)
  public void inodeWithDirectoryEntryShouldFailIfOffsetIsTooLarge()
      throws Exception {
    SuperBlock sb = new SuperBlock();
    sb.setInodeTableStart(12345L);

    DirectoryHeader dh = DirectoryTestAccessor.createDirectoryHeader(
        0, 54321, 0);
    DirectoryEntry de = DirectoryTestAccessor.createDirectoryEntry(
        (short) 8192, (short) 0, (short) 0, (short) 0, new byte[0], dh);
    MetadataReference.inode(10101, sb, de);
  }

  @Test
  public void rawShouldBeRelativeToInodeTable() throws Exception {
    MetadataReference ref = MetadataReference.raw(10101, 12345L, (short) 6789);
    System.out.println(ref);
    assertEquals(10101, ref.getTag());
    assertEquals(12345L, ref.getBlockLocation());
    assertEquals((short) 6789, ref.getOffset());
    assertEquals(Integer.MAX_VALUE, ref.getMaxLength());
  }

  @Test(expected = SquashFsException.class)
  public void rawShouldFailIfOffsetIsTooLarge() throws Exception {
    MetadataReference.raw(10101, 12345L, (short) 8192);
  }

  @Test
  public void directoryShouldBeRelativeToDirectoryTable() throws Exception {
    SuperBlock sb = new SuperBlock();
    sb.setDirectoryTableStart(12345L);

    BasicDirectoryINode inode = new BasicDirectoryINode();
    inode.setFileSize(50);
    inode.setStartBlock(54321);
    inode.setOffset((short) 1234);

    MetadataReference ref = MetadataReference.directory(10101, sb, inode);
    System.out.println(ref);
    assertEquals(10101, ref.getTag());
    assertEquals(66666L, ref.getBlockLocation());
    assertEquals((short) 1234, ref.getOffset());
    assertEquals(47, ref.getMaxLength());
  }

  @Test(expected = SquashFsException.class)
  public void directoryShouldFailIfOffsetIsTooLarge() throws Exception {
    SuperBlock sb = new SuperBlock();
    sb.setDirectoryTableStart(12345L);

    BasicDirectoryINode inode = new BasicDirectoryINode();
    inode.setFileSize(50);
    inode.setStartBlock(54321);
    inode.setOffset((short) 8192);

    MetadataReference.directory(10101, sb, inode);
  }

}
