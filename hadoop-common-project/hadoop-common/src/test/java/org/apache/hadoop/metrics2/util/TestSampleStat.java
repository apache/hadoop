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

package org.apache.hadoop.metrics2.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test the running sample stat computation
 */
public class TestSampleStat {
  private static final double EPSILON = 1e-42;

  /**
   * Some simple use cases
   */
  @Test public void testSimple() {
    SampleStat stat = new SampleStat();
    assertEquals(0, stat.numSamples(), "num samples");
    assertEquals(0.0, stat.mean(), EPSILON, "mean");
    assertEquals(0.0, stat.variance(), EPSILON, "variance");
    assertEquals(0.0, stat.stddev(), EPSILON, "stddev");
    assertEquals(SampleStat.MinMax.DEFAULT_MIN_VALUE, stat.min(), EPSILON, "min");
    assertEquals(SampleStat.MinMax.DEFAULT_MAX_VALUE, stat.max(), EPSILON, "max");

    stat.add(3);
    assertEquals(1L, stat.numSamples(), "num samples");
    assertEquals(3.0, stat.mean(), EPSILON, "mean");
    assertEquals(0.0, stat.variance(), EPSILON, "variance");
    assertEquals(0.0, stat.stddev(), EPSILON, "stddev");
    assertEquals(3.0, stat.min(), EPSILON, "min");
    assertEquals(3.0, stat.max(), EPSILON, "max");

    stat.add(2).add(1);
    assertEquals(3L, stat.numSamples(), "num samples");
    assertEquals(2.0, stat.mean(), EPSILON, "mean");
    assertEquals(1.0, stat.variance(), EPSILON, "variance");
    assertEquals(1.0, stat.stddev(), EPSILON, "stddev");
    assertEquals(1.0, stat.min(), EPSILON, "min");
    assertEquals(3.0, stat.max(), EPSILON, "max");

    stat.reset();
    assertEquals(0, stat.numSamples(), "num samples");
    assertEquals(0.0, stat.mean(), EPSILON, "mean");
    assertEquals(0.0, stat.variance(), EPSILON, "variance");
    assertEquals(0.0, stat.stddev(), EPSILON, "stddev");
    assertEquals(SampleStat.MinMax.DEFAULT_MIN_VALUE, stat.min(), EPSILON, "min");
    assertEquals(SampleStat.MinMax.DEFAULT_MAX_VALUE, stat.max(), EPSILON, "max");
  }

}
