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

import java.util.Collection;
import java.util.EnumSet;

public enum SuperBlockFlag {
  UNCOMPRESSED_INODES(0),
  UNCOMPRESSED_DATA(1),
  CHECK(2),
  UNCOMPRESSED_FRAGMENTS(3),
  NO_FRAGMENTS(4),
  ALWAYS_FRAGMENTS(5),
  DUPLICATES(6),
  EXPORTABLE(7),
  UNCOMPRESSED_XATTRS(8),
  NO_XATTRS(9),
  COMPRESSOR_OPTIONS(10),
  UNCOMPRESSED_IDS(11);

  private final short mask;

  SuperBlockFlag(int bit) {
    mask = (short) (1 << bit);
  }

  public static EnumSet<SuperBlockFlag> flagsPresent(short value) {
    EnumSet<SuperBlockFlag> values = EnumSet.noneOf(SuperBlockFlag.class);
    for (SuperBlockFlag flag : values()) {
      if (flag.isSet(value)) {
        values.add(flag);
      }
    }
    return values;
  }

  public static short flagsFor(SuperBlockFlag... flags) {
    short value = 0;
    for (SuperBlockFlag flag : flags) {
      value |= flag.mask;
    }
    return value;
  }

  public static short flagsFor(Collection<SuperBlockFlag> flags) {
    short value = 0;
    for (SuperBlockFlag flag : flags) {
      value |= flag.mask;
    }
    return value;
  }

  public short mask() {
    return mask;
  }

  public boolean isSet(short value) {
    return (value & mask) != 0;
  }
}
