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

import org.apache.hadoop.runc.squashfs.SquashFsException;
import org.junit.Test;

import java.util.EnumSet;

import static org.apache.hadoop.runc.squashfs.inode.INodeType.BASIC_BLOCK_DEVICE;
import static org.apache.hadoop.runc.squashfs.inode.INodeType.BASIC_CHAR_DEVICE;
import static org.apache.hadoop.runc.squashfs.inode.INodeType.BASIC_DIRECTORY;
import static org.apache.hadoop.runc.squashfs.inode.INodeType.BASIC_FIFO;
import static org.apache.hadoop.runc.squashfs.inode.INodeType.BASIC_FILE;
import static org.apache.hadoop.runc.squashfs.inode.INodeType.BASIC_SOCKET;
import static org.apache.hadoop.runc.squashfs.inode.INodeType.BASIC_SYMLINK;
import static org.apache.hadoop.runc.squashfs.inode.INodeType.EXTENDED_BLOCK_DEVICE;
import static org.apache.hadoop.runc.squashfs.inode.INodeType.EXTENDED_CHAR_DEVICE;
import static org.apache.hadoop.runc.squashfs.inode.INodeType.EXTENDED_DIRECTORY;
import static org.apache.hadoop.runc.squashfs.inode.INodeType.EXTENDED_FIFO;
import static org.apache.hadoop.runc.squashfs.inode.INodeType.EXTENDED_FILE;
import static org.apache.hadoop.runc.squashfs.inode.INodeType.EXTENDED_SOCKET;
import static org.apache.hadoop.runc.squashfs.inode.INodeType.EXTENDED_SYMLINK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestINodeType {

  @Test
  public void basicShouldReturnTrueOnlyForBasicSubtypes() {
    EnumSet<INodeType> basics = EnumSet.of(
        BASIC_BLOCK_DEVICE,
        BASIC_CHAR_DEVICE,
        BASIC_DIRECTORY,
        BASIC_FIFO,
        BASIC_FILE,
        BASIC_SOCKET,
        BASIC_SYMLINK);

    for (INodeType type : INodeType.values()) {
      assertEquals(String.format("Wrong basic() result for %s", type),
          basics.contains(type), type.basic());
    }
  }

  @Test
  public void directoryShouldReturnTrueOnlyForDirectorySubtypes() {
    for (INodeType type : INodeType.values()) {
      assertEquals(String.format("Wrong directory() result for %s", type),
          type == BASIC_DIRECTORY || type == EXTENDED_DIRECTORY,
          type.directory());
    }
  }

  @Test
  public void fileShouldReturnTrueOnlyForFileSubtypes() {
    for (INodeType type : INodeType.values()) {
      assertEquals(String.format("Wrong file() result for %s", type),
          type == BASIC_FILE || type == EXTENDED_FILE, type.file());
    }
  }

  @Test
  public void symlinkShouldReturnTrueOnlyForSymlinkSubtypes() {
    for (INodeType type : INodeType.values()) {
      assertEquals(String.format("Wrong symlink() result for %s", type),
          type == BASIC_SYMLINK || type == EXTENDED_SYMLINK, type.symlink());
    }
  }

  @Test
  public void blockDeviceShouldReturnTrueOnlyForBlockDeviceSubtypes() {
    for (INodeType type : INodeType.values()) {
      assertEquals(String.format("Wrong blockDevice() result for %s", type),
          type == BASIC_BLOCK_DEVICE || type == EXTENDED_BLOCK_DEVICE,
          type.blockDevice());
    }
  }

  @Test
  public void charDeviceShouldReturnTrueOnlyForCharDeviceSubtypes() {
    for (INodeType type : INodeType.values()) {
      assertEquals(String.format("Wrong blockDevice() result for %s", type),
          type == BASIC_CHAR_DEVICE || type == EXTENDED_CHAR_DEVICE,
          type.charDevice());
    }
  }

  @Test
  public void deviceShouldReturnTrueOnlyForDeviceSubtypes() {
    for (INodeType type : INodeType.values()) {
      assertEquals(String.format("Wrong device() result for %s", type),
          type.charDevice() || type.blockDevice(), type.device());
    }
  }

  @Test
  public void fifoShouldReturnTrueOnlyForFifoSubtypes() {
    for (INodeType type : INodeType.values()) {
      assertEquals(String.format("Wrong fifo() result for %s", type),
          type == BASIC_FIFO || type == EXTENDED_FIFO, type.fifo());
    }
  }

  @Test
  public void socketShouldReturnTrueOnlyForSocketSubtypes() {
    for (INodeType type : INodeType.values()) {
      assertEquals(String.format("Wrong socket() result for %s", type),
          type == BASIC_SOCKET || type == EXTENDED_SOCKET, type.socket());
    }
  }

  @Test
  public void ipcShouldReturnTrueOnlyForIpcSubtypes() {
    for (INodeType type : INodeType.values()) {
      assertEquals(String.format("Wrong ipc() result for %s", type),
          type.fifo() || type.socket(), type.ipc());
    }
  }

  @Test
  public void valueShouldReturnIncrementingValues() {
    short value = 1;
    for (INodeType type : INodeType.values()) {
      assertEquals(String.format("Wrong value() result for %s", type), value,
          type.value());
      value++;
    }
  }

  @Test
  public void dirValueShouldAlwaysResolveToABasicTypeWithSameMode()
      throws SquashFsException {
    for (INodeType type : INodeType.values()) {
      INodeType other = INodeType.fromValue(type.dirValue());
      assertEquals(String.format("Wrong mode() result for %s", type),
          other.mode(), type.mode());
    }
  }

  @Test
  public void createShouldReturnAnINodeWithProperType() {
    for (INodeType type : INodeType.values()) {
      INode inode = type.create();
      assertSame(String.format("Wrong create() result for %s", type),
          inode.getInodeType(), type);
    }
  }

  @Test
  public void fromValueShouldProperlyFindAllValuesFromOneToFourteen()
      throws SquashFsException {
    EnumSet<INodeType> pending = EnumSet.allOf(INodeType.class);
    for (short i = 1; i <= 14; i++) {
      INodeType it = INodeType.fromValue(i);
      assertEquals(String.format("Wrong fromValue() result for %d", i), i,
          it.value());
      assertTrue(String.format("Duplicate entry found for %s", it),
          pending.remove(it));
    }
    assertEquals(String.format("Didn't find all values: %s", pending), 0,
        pending.size());
  }

  @Test(expected = SquashFsException.class)
  public void fromValueShouldThrowExceptionForOutOfRangeValue()
      throws SquashFsException {
    INodeType.fromValue((short) 15);
  }

  @Test
  public void fromDirectoryValueShouldProperlyFindAllValuesFromOneToSeven()
      throws SquashFsException {
    EnumSet<INodeType> pending = EnumSet.allOf(INodeType.class);
    pending.removeIf(it -> !it.basic());

    for (short i = 1; i <= 7; i++) {
      INodeType it = INodeType.fromDirectoryValue(i);
      assertEquals(String.format("Wrong fromDirectoryValue() result for %d", i),
          i, it.value());
      assertTrue(String.format("Duplicate entry found for %s", it),
          pending.remove(it));
    }
    assertEquals(String.format("Didn't find all values: %s", pending), 0,
        pending.size());
  }

  @Test(expected = SquashFsException.class)
  public void fromDirectoryValueShouldThrowExceptionForOutOfRangeValue()
      throws SquashFsException {
    INodeType.fromDirectoryValue((short) 8);
  }

}
