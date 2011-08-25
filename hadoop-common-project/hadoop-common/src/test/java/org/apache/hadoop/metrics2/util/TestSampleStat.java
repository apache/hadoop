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

import org.junit.Test;
import static org.junit.Assert.*;

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
    assertEquals("num samples", 0, stat.numSamples());
    assertEquals("mean", 0.0, stat.mean(), EPSILON);
    assertEquals("variance", 0.0, stat.variance(), EPSILON);
    assertEquals("stddev", 0.0, stat.stddev(), EPSILON);
    assertEquals("min", Double.MAX_VALUE, stat.min(), EPSILON);
    assertEquals("max", Double.MIN_VALUE, stat.max(), EPSILON);

    stat.add(3);
    assertEquals("num samples", 1L, stat.numSamples());
    assertEquals("mean", 3.0, stat.mean(), EPSILON);
    assertEquals("variance", 0.0, stat.variance(), EPSILON);
    assertEquals("stddev", 0.0, stat.stddev(), EPSILON);
    assertEquals("min", 3.0, stat.min(), EPSILON);
    assertEquals("max", 3.0, stat.max(), EPSILON);

    stat.add(2).add(1);
    assertEquals("num samples", 3L, stat.numSamples());
    assertEquals("mean", 2.0, stat.mean(), EPSILON);
    assertEquals("variance", 1.0, stat.variance(), EPSILON);
    assertEquals("stddev", 1.0, stat.stddev(), EPSILON);
    assertEquals("min", 1.0, stat.min(), EPSILON);
    assertEquals("max", 3.0, stat.max(), EPSILON);

    stat.reset();
    assertEquals("num samples", 0, stat.numSamples());
    assertEquals("mean", 0.0, stat.mean(), EPSILON);
    assertEquals("variance", 0.0, stat.variance(), EPSILON);
    assertEquals("stddev", 0.0, stat.stddev(), EPSILON);
    assertEquals("min", Double.MAX_VALUE, stat.min(), EPSILON);
    assertEquals("max", Double.MIN_VALUE, stat.max(), EPSILON);
  }

}
