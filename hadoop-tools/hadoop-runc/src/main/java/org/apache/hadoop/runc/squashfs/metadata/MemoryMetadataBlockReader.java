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
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

public class MemoryMetadataBlockReader implements MetadataBlockReader {

  private final int ourTag;
  private final SuperBlock sb;
  private final byte[] data;
  private final int offset;
  private final int length;

  public MemoryMetadataBlockReader(int tag, SuperBlock sb, byte[] data) {
    this(tag, sb, data, 0, data.length);
  }

  public MemoryMetadataBlockReader(int tag, SuperBlock sb, byte[] data,
      int offset, int length) {
    this.ourTag = tag;
    this.sb = sb;
    this.data = data;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public SuperBlock getSuperBlock(int tag) {
    if (ourTag != tag) {
      throw new IllegalArgumentException(String.format("Invalid tag: %d", tag));
    }
    return sb;
  }

  @Override
  public MetadataBlock read(int tag, long fileOffset)
      throws IOException, SquashFsException {
    if (ourTag != tag) {
      throw new IllegalArgumentException(String.format("Invalid tag: %d", tag));
    }
    if (fileOffset >= length) {
      throw new EOFException("Read past end of buffer");
    }
    int localOffset = (int) fileOffset;

    try (ByteArrayInputStream bis = new ByteArrayInputStream(data,
        offset + localOffset, length - localOffset)) {
      try (DataInputStream dis = new DataInputStream(bis)) {
        return MetadataBlock.read(dis, sb);
      }
    }
  }

  @Override
  public void close() {
    // nothing to do
  }

}
