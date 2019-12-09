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

package org.apache.hadoop.ipc;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ipc.WeightedRoundRobinMultiplexer.IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY;

public class TestWeightedRoundRobinMultiplexer {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestWeightedRoundRobinMultiplexer.class);

  private WeightedRoundRobinMultiplexer mux;

  @Test(expected=IllegalArgumentException.class)
  public void testInstantiateNegativeMux() {
    mux = new WeightedRoundRobinMultiplexer(-1, "", new Configuration());
  }

  @Test(expected=IllegalArgumentException.class)
  public void testInstantiateZeroMux() {
    mux = new WeightedRoundRobinMultiplexer(0, "", new Configuration());
  }

  @Test(expected=IllegalArgumentException.class)
  public void testInstantiateIllegalMux() {
    Configuration conf = new Configuration();
    conf.setStrings("namespace." + IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY,
      "1", "2", "3");

    // ask for 3 weights with 2 queues
    mux = new WeightedRoundRobinMultiplexer(2, "namespace", conf);
  }

  @Test
  public void testLegalInstantiation() {
    Configuration conf = new Configuration();
    conf.setStrings("namespace." + IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY,
      "1", "2", "3");

    // ask for 3 weights with 3 queues
    mux = new WeightedRoundRobinMultiplexer(3, "namespace.", conf);
  }

  @Test
  public void testDefaultPattern() {
    // Mux of size 1: 0 0 0 0 0, etc
    mux = new WeightedRoundRobinMultiplexer(1, "", new Configuration());
    for(int i = 0; i < 10; i++) {
      assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
    }

    // Mux of size 2: 0 0 1 0 0 1 0 0 1, etc
    mux = new WeightedRoundRobinMultiplexer(2, "", new Configuration());
    assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 1);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 1);

    // Size 3: 4x0 2x1 1x2, etc
    mux = new WeightedRoundRobinMultiplexer(3, "", new Configuration());
    assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 1);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 1);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 2);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 0);

    // Size 4: 8x0 4x1 2x2 1x3
    mux = new WeightedRoundRobinMultiplexer(4, "", new Configuration());
    assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 1);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 1);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 1);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 1);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 2);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 2);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 3);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
  }

  @Test
  public void testCustomPattern() {
    // 1x0 1x1
    Configuration conf = new Configuration();
    conf.setStrings("test.custom." + IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY,
      "1", "1");

    mux = new WeightedRoundRobinMultiplexer(2, "test.custom", conf);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 1);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
    assertEquals(mux.getAndAdvanceCurrentIndex(), 1);

    // 1x0 3x1 2x2
    conf.setStrings("test.custom." + IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY,
      "1", "3", "2");

    mux = new WeightedRoundRobinMultiplexer(3, "test.custom", conf);

    for(int i = 0; i < 5; i++) {
      assertEquals(mux.getAndAdvanceCurrentIndex(), 0);
      assertEquals(mux.getAndAdvanceCurrentIndex(), 1);
      assertEquals(mux.getAndAdvanceCurrentIndex(), 1);
      assertEquals(mux.getAndAdvanceCurrentIndex(), 1);
      assertEquals(mux.getAndAdvanceCurrentIndex(), 2);
      assertEquals(mux.getAndAdvanceCurrentIndex(), 2);
    } // Ensure pattern repeats

  }
}