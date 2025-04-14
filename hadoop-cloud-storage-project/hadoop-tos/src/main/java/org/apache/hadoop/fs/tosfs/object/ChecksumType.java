/*
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

package org.apache.hadoop.fs.tosfs.object;

public enum ChecksumType {
  CRC32C((byte) 2, 4),
  CRC64ECMA((byte) 3, 8),
  MD5((byte) 4, 128);

  private final byte value;
  private final int bytes;

  ChecksumType(byte value, int bytes) {
    this.value = value;
    this.bytes = bytes;
  }

  public byte value() {
    return value;
  }

  public int bytes() {
    return bytes;
  }
}
