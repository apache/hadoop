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

package org.apache.hadoop.fs.azure.metrics;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class TestRollingWindowAverage {
  /**
   * Tests the basic functionality of the class.
   */
  @Test
  public void testBasicFunctionality() throws Exception {
    RollingWindowAverage average = new RollingWindowAverage(100);
    assertEquals(0, average.getCurrentAverage()); // Nothing there yet.
    average.addPoint(5);
    assertEquals(5, average.getCurrentAverage()); // One point in there.
    Thread.sleep(50);
    average.addPoint(15);
    assertEquals(10, average.getCurrentAverage()); // Two points in there.
    Thread.sleep(60);
    assertEquals(15, average.getCurrentAverage()); // One point retired.
    Thread.sleep(50);
    assertEquals(0, average.getCurrentAverage()); // Both points retired.
  }
}
