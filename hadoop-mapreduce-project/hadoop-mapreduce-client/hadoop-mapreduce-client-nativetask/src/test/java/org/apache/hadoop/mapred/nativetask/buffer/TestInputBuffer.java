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

import junit.framework.TestCase;

import org.junit.Assert;

public class TestInputBuffer extends TestCase {
  public void testInputBuffer() throws IOException {
    final int size = 100;
    final InputBuffer input1 = new InputBuffer(BufferType.DIRECT_BUFFER, size);
    Assert.assertEquals(input1.getType(), BufferType.DIRECT_BUFFER);

    Assert.assertTrue(input1.position() == 0);
    Assert.assertTrue(input1.length() == 0);
    Assert.assertTrue(input1.remaining() == 0);
    Assert.assertTrue(input1.capacity() == size);

    final InputBuffer input2 = new InputBuffer(BufferType.HEAP_BUFFER, size);
    Assert.assertEquals(input2.getType(), BufferType.HEAP_BUFFER);

    Assert.assertTrue(input2.position() == 0);
    Assert.assertTrue(input2.length() == 0);
    Assert.assertTrue(input2.remaining() == 0);
    Assert.assertTrue(input2.capacity() == size);

    final InputBuffer input3 = new InputBuffer(new byte[size]);
    Assert.assertEquals(input3.getType(), BufferType.HEAP_BUFFER);

    Assert.assertTrue(input3.position() == 0);
    Assert.assertTrue(input3.length() == 0);
    Assert.assertTrue(input3.remaining() == 0);
    Assert.assertEquals(input3.capacity(), size);
  }
}
