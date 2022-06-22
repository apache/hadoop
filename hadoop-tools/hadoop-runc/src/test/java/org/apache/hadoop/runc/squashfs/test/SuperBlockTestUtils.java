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

package org.apache.hadoop.runc.squashfs.test;

import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public final class SuperBlockTestUtils {

  private SuperBlockTestUtils() {
  }

  public static byte[] serializeSuperBlock(SuperBlock sb) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      try (DataOutputStream dos = new DataOutputStream(bos)) {
        sb.writeData(dos);
      }
      return bos.toByteArray();
    }
  }

  public static SuperBlock deserializeSuperBlock(byte[] data)
      throws IOException {
    SuperBlock sb = new SuperBlock();
    try (ByteArrayInputStream bis = new ByteArrayInputStream(data)) {
      try (DataInputStream dis = new DataInputStream(bis)) {
        sb.readData(dis);
      }
    }
    return sb;
  }

}
