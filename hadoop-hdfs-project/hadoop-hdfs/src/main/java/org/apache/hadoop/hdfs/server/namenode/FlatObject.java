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
package org.apache.hadoop.hdfs.server.namenode;

import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

abstract class FlatObject {
  protected final ByteBuffer data;

  protected FlatObject(ByteBuffer data) {
    this.data = data.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
  }

  protected FlatObject(byte[] b) {
    this.data = ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN);
  }

  protected FlatObject(ByteString data) {
    this.data = data.asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
  }

  public ByteBuffer asReadOnlyByteBuffer() { return data.duplicate(); }

  public byte[] toByteArray() {
    byte[] b = new byte[data.remaining()];
    System.arraycopy(data.array(), data.arrayOffset() + data.position(),
      b, 0, data.remaining());
    return b;
  }
}
