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

package org.apache.hadoop.runc.squashfs.table;

import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class MemoryTableReader implements TableReader {

  private final SuperBlock sb;
  private final byte[] data;
  private final int offset;
  private final int leh;

  public MemoryTableReader(SuperBlock sb, byte[] data) {
    this(sb, data, 0, data.length);
  }

  public MemoryTableReader(SuperBlock sb, byte[] data, int offset, int length) {
    this.sb = sb;
    this.data = data;
    this.offset = offset;
    this.leh = length;
  }

  @Override
  public SuperBlock getSuperBlock() {
    return sb;
  }

  @Override
  public ByteBuffer read(long fileOffset, int length) throws IOException {
    if ((fileOffset + length) > (long) this.leh) {
      throw new EOFException(String.format(
          "Read past end of table (offset = %d, length = %d, available = %d)",
          fileOffset, length, this.leh - fileOffset));
    }
    int localOffset = ((int) fileOffset) + offset;
    return ByteBuffer.wrap(data, localOffset, length)
        .order(ByteOrder.LITTLE_ENDIAN);
  }

  @Override
  public void close() {
  }

}
