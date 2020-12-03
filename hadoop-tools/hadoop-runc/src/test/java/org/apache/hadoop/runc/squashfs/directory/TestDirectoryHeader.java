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

package org.apache.hadoop.runc.squashfs.directory;

import org.apache.hadoop.runc.squashfs.SquashFsException;
import org.apache.hadoop.runc.squashfs.test.DirectoryTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class TestDirectoryHeader {

  private DirectoryHeader hdr;

  @Before
  public void setUp() {
    hdr = new DirectoryHeader(0, 1, 2);
  }

  @Test
  public void getStructureSizeReturnsCorrectValue() {
    assertEquals(12, hdr.getStructureSize());
  }

  @Test
  public void readShouldSucceed() throws Exception {
    byte[] buf = new byte[12];
    ByteBuffer bb = ByteBuffer.wrap(buf);
    bb.putInt(0);
    bb.putInt(1);
    bb.putInt(2);
    try (ByteArrayInputStream bis = new ByteArrayInputStream(buf)) {
      try (DataInputStream dis = new DataInputStream(bis)) {
        DirectoryHeader dest = DirectoryHeader.read(dis);
        assertEquals("wrong count", 0, dest.getCount());
        assertEquals("wrong start block", 1, dest.getStartBlock());
        assertEquals("wrong inode number", 2, dest.getInodeNumber());
      }
    }
  }

  @Test(expected = SquashFsException.class)
  public void readShouldFailIfCountIsTooLarge() throws Exception {
    byte[] buf = new byte[12];
    ByteBuffer bb = ByteBuffer.wrap(buf);
    bb.putInt(256);
    bb.putInt(1);
    bb.putInt(2);
    try (ByteArrayInputStream bis = new ByteArrayInputStream(buf)) {
      try (DataInputStream dis = new DataInputStream(bis)) {
        DirectoryHeader.read(dis);
      }
    }
  }

  @Test
  public void writeDataAndReadDataShouldBeReflexive() throws IOException {
    byte[] data = DirectoryTestUtils.serializeDirectoryElement(hdr);
    DirectoryHeader dest = DirectoryTestUtils.deserializeDirectoryHeader(data);

    assertEquals("wrong count", 0, dest.getCount());
    assertEquals("wrong start block", 1, dest.getStartBlock());
    assertEquals("wrong inode number", 2, dest.getInodeNumber());
  }

  @Test
  public void toStringShouldNotFail() {
    System.out.println(hdr.toString());
  }

}
