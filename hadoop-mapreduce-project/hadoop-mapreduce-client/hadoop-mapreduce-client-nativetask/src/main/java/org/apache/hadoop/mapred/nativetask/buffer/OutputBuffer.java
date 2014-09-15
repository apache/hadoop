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

package org.apache.hadoop.mapred.nativetask.buffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class OutputBuffer {
  protected ByteBuffer byteBuffer;
  private final BufferType type;

  public OutputBuffer(BufferType type, int outputBufferCapacity) {

    this.type = type;
    if (outputBufferCapacity > 0) {
      switch (type) {
      case DIRECT_BUFFER:
        this.byteBuffer = ByteBuffer.allocateDirect(outputBufferCapacity);
        this.byteBuffer.order(ByteOrder.BIG_ENDIAN);
        break;
      case HEAP_BUFFER:
        this.byteBuffer = ByteBuffer.allocate(outputBufferCapacity);
        this.byteBuffer.order(ByteOrder.BIG_ENDIAN);
        break;
      }
    }
  }

  public OutputBuffer(byte[] bytes) {
    this.type = BufferType.HEAP_BUFFER;
    final int outputBufferCapacity = bytes.length;
    if (outputBufferCapacity > 0) {
      this.byteBuffer = ByteBuffer.wrap(bytes);
      this.byteBuffer.order(ByteOrder.BIG_ENDIAN);
      this.byteBuffer.position(0);
    }
  }

  public BufferType getType() {
    return this.type;
  }

  public ByteBuffer getByteBuffer() {
    return this.byteBuffer;
  }

  public int length() {
    return byteBuffer.position();
  }

  public void rewind() {
    byteBuffer.position(0);
  }

  public int limit() {
    return byteBuffer.limit();
  }
}