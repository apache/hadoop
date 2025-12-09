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

import org.apache.hadoop.test.HadoopTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@code WeakReferencedElasticByteBufferPool}.
 */
public class TestWeakReferencedElasticByteBufferPool
        extends HadoopTestBase {

  private boolean isDirect;

  private String type;

  public static List<String> params() {
    return Arrays.asList("direct", "array");
  }

  public void initTestWeakReferencedElasticByteBufferPool(String pType) {
    this.type = pType;
    this.isDirect = !"array".equals(pType);
  }

  @ParameterizedTest(name = "Buffer type : {0}")
  @MethodSource("params")
  public void testGetAndPutBasic(String pType) {
    initTestWeakReferencedElasticByteBufferPool(pType);
    WeakReferencedElasticByteBufferPool pool = new WeakReferencedElasticByteBufferPool();
    int bufferSize = 5;
    ByteBuffer buffer = pool.getBuffer(isDirect, bufferSize);
    assertThat(buffer.isDirect())
        .describedAs("Buffered returned should be of correct type {}", type)
        .isEqualTo(isDirect);
    assertThat(buffer.capacity())
        .describedAs("Initial capacity of returned buffer from pool")
        .isEqualTo(bufferSize);
    assertThat(buffer.position())
        .describedAs("Initial position of returned buffer from pool")
        .isEqualTo(0);

    byte[] arr = createByteArray(bufferSize);
    buffer.put(arr, 0, arr.length);
    buffer.flip();
    validateBufferContent(buffer, arr);
    assertThat(buffer.position())
        .describedAs("Buffer's position after filling bytes in it")
        .isEqualTo(bufferSize);
    // releasing buffer to the pool.
    pool.putBuffer(buffer);
    assertThat(buffer.position())
        .describedAs("Position should be reset to 0 after returning buffer to the pool")
        .isEqualTo(0);
  }

  @ParameterizedTest(name = "Buffer type : {0}")
  @MethodSource("params")
  public void testPoolingWithDifferentSizes(String pType) {
    initTestWeakReferencedElasticByteBufferPool(pType);
    WeakReferencedElasticByteBufferPool pool = new WeakReferencedElasticByteBufferPool();
    ByteBuffer buffer = pool.getBuffer(isDirect, 5);
    ByteBuffer buffer1 = pool.getBuffer(isDirect, 10);
    ByteBuffer buffer2 = pool.getBuffer(isDirect, 15);

    assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool")
            .isEqualTo(0);

    pool.putBuffer(buffer1);
    pool.putBuffer(buffer2);
    assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool")
            .isEqualTo(2);
    ByteBuffer buffer3 = pool.getBuffer(isDirect, 12);
    assertThat(buffer3.capacity())
            .describedAs("Pooled buffer should have older capacity")
            .isEqualTo(15);
    assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool")
            .isEqualTo(1);
    pool.putBuffer(buffer);
    ByteBuffer buffer4 = pool.getBuffer(isDirect, 6);
    assertThat(buffer4.capacity())
            .describedAs("Pooled buffer should have older capacity")
            .isEqualTo(10);
    assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool")
            .isEqualTo(1);

    pool.release();
    assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool post release")
            .isEqualTo(0);
  }

  @ParameterizedTest(name = "Buffer type : {0}")
  @MethodSource("params")
  public void testPoolingWithDifferentInsertionTime(String pType) {
    initTestWeakReferencedElasticByteBufferPool(pType);
    WeakReferencedElasticByteBufferPool pool = new WeakReferencedElasticByteBufferPool();
    ByteBuffer buffer = pool.getBuffer(isDirect, 10);
    ByteBuffer buffer1 = pool.getBuffer(isDirect, 10);
    ByteBuffer buffer2 = pool.getBuffer(isDirect, 10);

    assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool")
            .isEqualTo(0);

    pool.putBuffer(buffer1);
    pool.putBuffer(buffer2);
    assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool")
            .isEqualTo(2);
    ByteBuffer buffer3 = pool.getBuffer(isDirect, 10);
    // As buffer1 is returned to the pool before buffer2, it should
    // be returned when buffer of same size is asked again from
    // the pool. Memory references must match not just content
    // that is why {@code isSameAs} is used here rather
    // than usual {@code isEqualTo}.
    assertThat(buffer3)
            .describedAs("Buffers should be returned in order of their " +
                    "insertion time")
            .isSameAs(buffer1);
    pool.putBuffer(buffer);
    ByteBuffer buffer4 = pool.getBuffer(isDirect, 10);
    assertThat(buffer4)
            .describedAs("Buffers should be returned in order of their " +
                    "insertion time")
            .isSameAs(buffer2);
  }

  @ParameterizedTest(name = "Buffer type : {0}")
  @MethodSource("params")
  public void testGarbageCollection(String pType) {
    initTestWeakReferencedElasticByteBufferPool(pType);
    WeakReferencedElasticByteBufferPool pool = new WeakReferencedElasticByteBufferPool();
    ByteBuffer buffer = pool.getBuffer(isDirect, 5);
    ByteBuffer buffer1 = pool.getBuffer(isDirect, 10);
    ByteBuffer buffer2 = pool.getBuffer(isDirect, 15);
    assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool")
            .isEqualTo(0);
    pool.putBuffer(buffer1);
    pool.putBuffer(buffer2);
    assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool")
            .isEqualTo(2);
    // Before GC.
    ByteBuffer buffer4 = pool.getBuffer(isDirect, 12);
    assertThat(buffer4.capacity())
            .describedAs("Pooled buffer should have older capacity")
            .isEqualTo(15);
    pool.putBuffer(buffer4);
    // Removing the references
    buffer1 = null;
    buffer2 = null;
    buffer4 = null;
    System.gc();
    ByteBuffer buffer3 = pool.getBuffer(isDirect, 12);
    assertThat(buffer3.capacity())
            .describedAs("After garbage collection new buffer should be " +
                    "returned with fixed capacity")
            .isEqualTo(12);
  }

  @ParameterizedTest(name = "Buffer type : {0}")
  @MethodSource("params")
  public void testWeakReferencesPruning(String pType) {
    initTestWeakReferencedElasticByteBufferPool(pType);
    WeakReferencedElasticByteBufferPool pool = new WeakReferencedElasticByteBufferPool();
    ByteBuffer buffer1 = pool.getBuffer(isDirect, 5);
    ByteBuffer buffer2 = pool.getBuffer(isDirect, 10);
    ByteBuffer buffer3 = pool.getBuffer(isDirect, 15);

    pool.putBuffer(buffer2);
    pool.putBuffer(buffer3);
    assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool")
            .isEqualTo(2);

    // marking only buffer2 to be garbage collected.
    buffer2 = null;
    System.gc();
    ByteBuffer buffer4 = pool.getBuffer(isDirect, 10);
    // Number of buffers in the pool is 0 as one got garbage
    // collected and other got returned in above call.
    assertThat(pool.getCurrentBuffersCount(isDirect))
            .describedAs("Number of buffers in the pool")
            .isEqualTo(0);
    assertThat(buffer4.capacity())
            .describedAs("After gc, pool should return next greater than " +
                    "available buffer")
            .isEqualTo(15);

  }

  private void validateBufferContent(ByteBuffer buffer, byte[] arr) {
    for (int i=0; i<arr.length; i++) {
      assertThat(buffer.get())
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
