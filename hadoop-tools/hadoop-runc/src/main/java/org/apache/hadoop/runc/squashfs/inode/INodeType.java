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

import java.util.function.Supplier;

public enum INodeType {

  BASIC_DIRECTORY(1, 1, 'd', BasicDirectoryINode::new),
  BASIC_FILE(2, 2, '-', BasicFileINode::new),
  BASIC_SYMLINK(3, 3, 'l', BasicSymlinkINode::new),
  BASIC_BLOCK_DEVICE(4, 4, 'b', BasicBlockDeviceINode::new),
  BASIC_CHAR_DEVICE(5, 5, 'c', BasicCharDeviceINode::new),
  BASIC_FIFO(6, 6, 'p', BasicFifoINode::new),
  BASIC_SOCKET(7, 7, 's', BasicSocketINode::new),
  EXTENDED_DIRECTORY(8, 1, 'd', ExtendedDirectoryINode::new),
  EXTENDED_FILE(9, 2, '-', ExtendedFileINode::new),
  EXTENDED_SYMLINK(10, 3, 'l', ExtendedSymlinkINode::new),
  EXTENDED_BLOCK_DEVICE(11, 4, 'b', ExtendedBlockDeviceINode::new),
  EXTENDED_CHAR_DEVICE(12, 5, 'c', ExtendedCharDeviceINode::new),
  EXTENDED_FIFO(13, 6, 'p', ExtendedFifoINode::new),
  EXTENDED_SOCKET(14, 7, 's', ExtendedSocketINode::new);

  private final short value;
  private final short dirValue;
  private final char mode;
  private final Supplier<INode> creator;

  INodeType(int value, int dirValue, char mode, Supplier<INode> creator) {
    this.value = (short) value;
    this.dirValue = (short) dirValue;
    this.mode = mode;
    this.creator = creator;
  }

  public static INodeType fromValue(short value) throws SquashFsException {
    for (INodeType nt : values()) {
      if (nt.value == value) {
        return nt;
      }
    }
    throw new SquashFsException(
        String.format("Unknown inode type 0x%x (%d)", value, value));
  }

  public static INodeType fromDirectoryValue(short value)
      throws SquashFsException {
    for (INodeType nt : values()) {
      if (nt.value == value && nt.basic()) {
        return nt;
      }
    }
    throw new SquashFsException(
        String.format("Unknown inode type 0x%x (%d)", value, value));
  }

  public boolean basic() {
    return value <= (short) 7;
  }

  public boolean directory() {
    return mode == 'd';
  }

  public boolean file() {
    return mode == '-';
  }

  public boolean symlink() {
    return mode == 'l';
  }

  public boolean blockDevice() {
    return mode == 'b';
  }

  public boolean charDevice() {
    return mode == 'c';
  }

  public boolean device() {
    return blockDevice() || charDevice();
  }

  public boolean fifo() {
    return mode == 'p';
  }

  public boolean socket() {
    return mode == 's';
  }

  public boolean ipc() {
    return fifo() || socket();
  }

  public short value() {
    return value;
  }

  public short dirValue() {
    return dirValue;
  }

  public char mode() {
    return mode;
  }

  public INode create() {
    return creator.get();
  }

}
