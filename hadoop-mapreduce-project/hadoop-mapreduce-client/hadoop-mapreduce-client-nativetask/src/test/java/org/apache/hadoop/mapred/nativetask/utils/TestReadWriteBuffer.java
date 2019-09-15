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
package org.apache.hadoop.mapred.nativetask.utils;

import org.junit.Test;

import org.apache.hadoop.mapred.nativetask.util.ReadWriteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class TestReadWriteBuffer {

  private static byte[] bytes = new byte[] { '0', 'a', 'b', 'c', 'd', '9' };

  @Test
  public void testReadWriteBuffer() {

    final ReadWriteBuffer buffer = new ReadWriteBuffer();

    assertThat(buffer.getBuff()).isNotNull();

    assertThat(buffer.getWritePoint()).isZero();
    assertThat(buffer.getReadPoint()).isZero();

    buffer.writeInt(3);

    buffer.writeString("goodboy");

    buffer.writeLong(10L);
    buffer.writeBytes(bytes, 0, bytes.length);
    buffer.writeLong(100L);

    assertThat(buffer.getWritePoint()).isEqualTo(41);
    assertThat(buffer.getReadPoint()).isZero();
    assertThat(buffer.getBuff().length).isEqualTo(41);

    assertThat(buffer.readInt()).isEqualTo(3);
    assertThat(buffer.readString()).isEqualTo("goodboy");
    assertThat(buffer.readLong()).isEqualTo(10L);

    final byte[] read = buffer.readBytes();
    for (int i = 0; i < bytes.length; i++) {
      assertThat(read[i]).isEqualTo(bytes[i]);
    }

    assertThat(buffer.readLong()).isEqualTo(100L);
    assertThat(buffer.getReadPoint()).isEqualTo(41);
  }
}
