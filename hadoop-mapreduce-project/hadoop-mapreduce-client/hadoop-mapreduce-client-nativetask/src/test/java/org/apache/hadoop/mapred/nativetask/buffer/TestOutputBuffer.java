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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestOutputBuffer {

  @Test
  void testOutputBuffer() {
    final int size = 100;
    final OutputBuffer output1 = new OutputBuffer(BufferType.DIRECT_BUFFER, size);
    assertThat(output1.getType()).isEqualTo(BufferType.DIRECT_BUFFER);

    assertThat(output1.length()).isZero();
    assertThat(output1.limit()).isEqualTo(size);

    final OutputBuffer output2 = new OutputBuffer(BufferType.HEAP_BUFFER, size);
    assertThat(output2.getType()).isEqualTo(BufferType.HEAP_BUFFER);

    assertThat(output2.length()).isZero();
    assertThat(output2.limit()).isEqualTo(size);

    final OutputBuffer output3 = new OutputBuffer(new byte[size]);
    assertThat(output3.getType()).isEqualTo(BufferType.HEAP_BUFFER);

    assertThat(output3.length()).isZero();
    assertThat(output3.limit()).isEqualTo(size);
  }
}
