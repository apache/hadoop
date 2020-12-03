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

package org.apache.hadoop.runc.squashfs.superblock;

import org.apache.hadoop.runc.squashfs.SquashFsException;

public enum CompressionId {
  NONE(0),
  ZLIB(1),
  LZMA(2),
  LZO(3),
  XZ(4),
  LZ4(5),
  ZSTD(6);

  private final short value;

  CompressionId(int value) {
    this.value = (short) value;
  }

  public static CompressionId fromValue(short value) throws SquashFsException {
    for (CompressionId id : values()) {
      if (id.value == value) {
        return id;
      }
    }
    throw new SquashFsException(
        String.format("Unknown compression id %d", value));
  }

  public short value() {
    return value;
  }
}
