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

package org.apache.hadoop.io;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.test.HadoopTestBase;

/**
 * Unit tests for {@code WeakReferencedElasticByteBufferPool}.
 */
@RunWith(Parameterized.class)
public class TestWeakReferencedElasticByteBufferPool
        extends HadoopTestBase {

  private final boolean isDirect;

  private final String type;

  @Parameterized.Parameters(name = "Buffer type : {0}")
  public static List<String> params() {
    return Arrays.asList("direct", "array");
  }

  public TestWeakReferencedElasticByteBufferPool(String type) {
    this.type = type;
    this.isDirect = !"array".equals(type);
  }

  @Test
  public void testGetAndPutBasic() {
    WeakReferencedElasticByteBufferPool pool = new WeakReferencedElasticByteBufferPool();
    int bufferSize = 5;
    ByteBuffer buffer = pool.getBuffer(isDirect, bufferSize);
    Assertions.assertThat(buffer.isDirect())
            .describedAs("Buffered returned should be of correct type {}", type)
            .isEqualTo(isDirect);
    Assertions.assertThat(buffer.capacity())
            .describedAs("Initial capacity of returned buffer from pool")
            .isEqualTo(bufferSize);
    Assertions.assertThat(buffer.position())
            .describedAs("Initial position of returned buffer from pool")
            .isEqualTo(0);

    byte[] arr = createByteArray(bufferSize);
    buffer.put(arr, 0, arr.length);
    buffer.flip();
    validateBufferContent(buffer, arr);
    Assertions.assertThat(buffer.position())
            .describedAs("Buffer's position after filling bytes in it")
            .isEqualTo(bufferSize);
    // releasing buffer to the pool.
    pool.putBuffer(buffer);
    Assertions.assertThat(buffer.position())
            .describedAs("Position should be reset to 0 after returning buffer to the pool")
            .isEqualTo(0);

  }

  @Test
  public void testPoolingWithDifferentSizes() {
    WeakReferencedElasticByteBufferPool pool = new WeakReferencedElasticByteBufferPool();
    ByteBuffer buffer = pool.getBuffer(isDirect, 5);
    ByteBuffer buffer1 = pool.getBuffer(isDirect, 10);
    ByteBuffer buffer2 = pool.getBuffer(isDirect, 15);

    Assertions.assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool")
            .isEqualTo(0);

    pool.putBuffer(buffer1);
    pool.putBuffer(buffer2);
    Assertions.assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool")
            .isEqualTo(2);
    ByteBuffer buffer3 = pool.getBuffer(isDirect, 12);
    Assertions.assertThat(buffer3.capacity())
            .describedAs("Pooled buffer should have older capacity")
            .isEqualTo(15);
    Assertions.assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool")
            .isEqualTo(1);
    pool.putBuffer(buffer);
    ByteBuffer buffer4 = pool.getBuffer(isDirect, 6);
    Assertions.assertThat(buffer4.capacity())
            .describedAs("Pooled buffer should have older capacity")
            .isEqualTo(10);
    Assertions.assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool")
            .isEqualTo(1);

    pool.release();
    Assertions.assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool post release")
            .isEqualTo(0);
  }

  @Test
  public void testPoolingWithDifferentInsertionTime() {
    WeakReferencedElasticByteBufferPool pool = new WeakReferencedElasticByteBufferPool();
    ByteBuffer buffer = pool.getBuffer(isDirect, 10);
    ByteBuffer buffer1 = pool.getBuffer(isDirect, 10);
    ByteBuffer buffer2 = pool.getBuffer(isDirect, 10);

    Assertions.assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool")
            .isEqualTo(0);

    pool.putBuffer(buffer1);
    pool.putBuffer(buffer2);
    Assertions.assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool")
            .isEqualTo(2);
    ByteBuffer buffer3 = pool.getBuffer(isDirect, 10);
    // As buffer1 is returned to the pool before buffer2, it should
    // be returned when buffer of same size is asked again from
    // the pool. Memory references must match not just content
    // that is why {@code Assertions.isSameAs} is used here rather
    // than usual {@code Assertions.isEqualTo}.
    Assertions.assertThat(buffer3)
            .describedAs("Buffers should be returned in order of their " +
                    "insertion time")
            .isSameAs(buffer1);
    pool.putBuffer(buffer);
    ByteBuffer buffer4 = pool.getBuffer(isDirect, 10);
    Assertions.assertThat(buffer4)
            .describedAs("Buffers should be returned in order of their " +
                    "insertion time")
            .isSameAs(buffer2);
  }

  @Test
  public void testGarbageCollection() {
    WeakReferencedElasticByteBufferPool pool = new WeakReferencedElasticByteBufferPool();
    ByteBuffer buffer = pool.getBuffer(isDirect, 5);
    ByteBuffer buffer1 = pool.getBuffer(isDirect, 10);
    ByteBuffer buffer2 = pool.getBuffer(isDirect, 15);
    Assertions.assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool")
            .isEqualTo(0);
    pool.putBuffer(buffer1);
    pool.putBuffer(buffer2);
    Assertions.assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool")
            .isEqualTo(2);
    // Before GC.
    ByteBuffer buffer4 = pool.getBuffer(isDirect, 12);
    Assertions.assertThat(buffer4.capacity())
            .describedAs("Pooled buffer should have older capacity")
            .isEqualTo(15);
    pool.putBuffer(buffer4);
    // Removing the references
    buffer1 = null;
    buffer2 = null;
    buffer4 = null;
    System.gc();
    ByteBuffer buffer3 = pool.getBuffer(isDirect, 12);
    Assertions.assertThat(buffer3.capacity())
            .describedAs("After garbage collection new buffer should be " +
                    "returned with fixed capacity")
            .isEqualTo(12);
  }

  @Test
  public void testWeakReferencesPruning() {
    WeakReferencedElasticByteBufferPool pool = new WeakReferencedElasticByteBufferPool();
    ByteBuffer buffer1 = pool.getBuffer(isDirect, 5);
    ByteBuffer buffer2 = pool.getBuffer(isDirect, 10);
    ByteBuffer buffer3 = pool.getBuffer(isDirect, 15);

    pool.putBuffer(buffer2);
    pool.putBuffer(buffer3);
    Assertions.assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool")
            .isEqualTo(2);

    // marking only buffer2 to be garbage collected.
    buffer2 = null;
    System.gc();
    ByteBuffer buffer4 = pool.getBuffer(isDirect, 10);
    // Number of buffers in the pool is 0 as one got garbage
    // collected and other got returned in above call.
    Assertions.assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool")
            .isEqualTo(0);
    Assertions.assertThat(buffer4.capacity())
            .describedAs("After gc, pool should return next greater than " +
                    "available buffer")
            .isEqualTo(15);

  }

  private void validateBufferContent(ByteBuffer buffer, byte[] arr) {
    for (int i=0; i<arr.length; i++) {
      Assertions.assertThat(buffer.get())
              .describedAs("Content of buffer at index {} should match " +
                      "with content of byte array", i)
              .isEqualTo(arr[i]);
    }
  }

  private byte[] createByteArray(int length) {
    byte[] arr = new byte[length];
    Random r = new Random();
    r.nextBytes(arr);
    return arr;
  }
}
