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

import java.io.IOException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestInputBuffer {

  @Test
  public void testInputBuffer() throws IOException {
    final int size = 100;
    final InputBuffer input1 = new InputBuffer(BufferType.DIRECT_BUFFER, size);
    assertThat(input1.getType()).isEqualTo(BufferType.DIRECT_BUFFER);

    assertThat(input1.position()).isZero();
    assertThat(input1.length()).isZero();
    assertThat(input1.remaining()).isZero();
    assertThat(input1.capacity()).isEqualTo(size);

    final InputBuffer input2 = new InputBuffer(BufferType.HEAP_BUFFER, size);
    assertThat(input2.getType()).isEqualTo(BufferType.HEAP_BUFFER);

    assertThat(input2.position()).isZero();
    assertThat(input2.length()).isZero();
    assertThat(input2.remaining()).isZero();
    assertThat(input2.capacity()).isEqualTo(size);

    final InputBuffer input3 = new InputBuffer(new byte[size]);
    assertThat(input3.getType()).isEqualTo(BufferType.HEAP_BUFFER);

    assertThat(input3.position()).isZero();
    assertThat(input3.length()).isZero();
    assertThat(input3.remaining()).isZero();
    assertThat(input3.capacity()).isEqualTo(size);
  }
}
