/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.impl.prefetch;

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_TMP_DIR;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class TestBlockCache extends AbstractHadoopTestBase {

  private static final int BUFFER_SIZE = 16;

  private static final Configuration CONF = new Configuration();

  @Test
  public void testArgChecks() throws Exception {
    // Should not throw.
    BlockCache cache =
        new SingleFilePerBlockCache(EmptyPrefetchingStatistics.getInstance(), 2);

    ByteBuffer buffer = ByteBuffer.allocate(16);

    // Verify it throws correctly.
    intercept(IllegalArgumentException.class, "'buffer' must not be null",
        () -> cache.put(42, null, null, null));


    intercept(NullPointerException.class, null,
        () -> new SingleFilePerBlockCache(null, 2));

  }


  @Test
  public void testPutAndGet() throws Exception {
    BlockCache cache =
        new SingleFilePerBlockCache(EmptyPrefetchingStatistics.getInstance(), 2);

    ByteBuffer buffer1 = ByteBuffer.allocate(BUFFER_SIZE);
    for (byte i = 0; i < BUFFER_SIZE; i++) {
      buffer1.put(i);
    }

    assertEquals(0, cache.size());
    assertFalse(cache.containsBlock(0));
    cache.put(0, buffer1, CONF, new LocalDirAllocator(HADOOP_TMP_DIR));
    assertEquals(1, cache.size());
    assertTrue(cache.containsBlock(0));
    ByteBuffer buffer2 = ByteBuffer.allocate(BUFFER_SIZE);
    cache.get(0, buffer2);
    assertNotSame(buffer1, buffer2);
    assertBuffersEqual(buffer1, buffer2);

    assertEquals(1, cache.size());
    assertFalse(cache.containsBlock(1));
    cache.put(1, buffer1, CONF, new LocalDirAllocator(HADOOP_TMP_DIR));
    assertEquals(2, cache.size());
    assertTrue(cache.containsBlock(1));
    ByteBuffer buffer3 = ByteBuffer.allocate(BUFFER_SIZE);
    cache.get(1, buffer3);
    assertNotSame(buffer1, buffer3);
    assertBuffersEqual(buffer1, buffer3);
  }

  private void assertBuffersEqual(ByteBuffer buffer1, ByteBuffer buffer2) {
    assertNotNull(buffer1);
    assertNotNull(buffer2);
    assertEquals(buffer1.limit(), buffer2.limit());
    assertEquals(BUFFER_SIZE, buffer1.limit());
    for (int i = 0; i < BUFFER_SIZE; i++) {
      assertEquals(buffer1.get(i), buffer2.get(i));
    }
  }
}
