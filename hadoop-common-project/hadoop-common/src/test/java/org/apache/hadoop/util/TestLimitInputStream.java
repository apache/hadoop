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

package org.apache.hadoop.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.test.HadoopTestBase;

public class TestLimitInputStream extends HadoopTestBase {
  static class RandomInputStream extends InputStream {
    private Random rn = new Random(0);

    @Override
    public int read() { return rn.nextInt(); }
  }

  @Test
  public void testRead() throws IOException {
    try (LimitInputStream limitInputStream =
      new LimitInputStream(new RandomInputStream(), 0)) {
      assertEquals(-1, limitInputStream.read(),
          "Reading byte after reaching limit should return -1");
    }
    try (LimitInputStream limitInputStream =
      new LimitInputStream(new RandomInputStream(), 4)) {
      assertEquals(new Random(0).nextInt(),
          limitInputStream.read(), "Incorrect byte returned");
    }
  }

  @Test
  public void testResetWithoutMark() throws IOException {
    assertThrows(IOException.class, () -> {
      try (LimitInputStream limitInputStream =
          new LimitInputStream(new RandomInputStream(), 128)) {
        limitInputStream.reset();
      }
    });
  }

  @Test
  public void testReadBytes() throws IOException {
    try (LimitInputStream limitInputStream =
      new LimitInputStream(new RandomInputStream(), 128)) {
      Random r = new Random(0);
      byte[] data = new byte[4];
      byte[] expected = { (byte) r.nextInt(), (byte) r.nextInt(),
                          (byte) r.nextInt(), (byte) r.nextInt() };
      limitInputStream.read(data, 0, 4);
      assertArrayEquals(expected, data, "Incorrect bytes returned");
    }
  }
}
