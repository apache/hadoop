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

import org.assertj.core.api.Assertions;
import org.junit.Test;

/**
 * Non parameterized tests for {@code WeakReferencedElasticByteBufferPool}.
 */
public class TestMoreWeakReferencedElasticByteBufferPool {

  @Test
  public void testMixedBuffersInPool() {
    WeakReferencedElasticByteBufferPool pool = new WeakReferencedElasticByteBufferPool();
    ByteBuffer buffer1 = pool.getBuffer(true, 5);
    ByteBuffer buffer2 = pool.getBuffer(true, 10);
    ByteBuffer buffer3 = pool.getBuffer(false, 5);
    ByteBuffer buffer4 = pool.getBuffer(false, 10);
    ByteBuffer buffer5 = pool.getBuffer(true, 15);

    assertBufferCounts(pool, 0, 0);
    pool.putBuffer(buffer1);
    pool.putBuffer(buffer2);
    assertBufferCounts(pool, 2, 0);
    pool.putBuffer(buffer3);
    assertBufferCounts(pool, 2, 1);
    pool.putBuffer(buffer5);
    assertBufferCounts(pool, 3, 1);
    pool.putBuffer(buffer4);
    assertBufferCounts(pool, 3, 2);
    pool.release();
    assertBufferCounts(pool, 0, 0);

  }

  private void assertBufferCounts(WeakReferencedElasticByteBufferPool pool,
                                  int numDirectBuffersExpected,
                                  int numHeapBuffersExpected) {
    Assertions.assertThat(pool.getCurrentBuffersCount(true))
            .describedAs("Number of direct buffers in pool")
            .isEqualTo(numDirectBuffersExpected);
    Assertions.assertThat(pool.getCurrentBuffersCount(false))
            .describedAs("Number of heap buffers in pool")
            .isEqualTo(numHeapBuffersExpected);
  }
}
