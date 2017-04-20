/*
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

package org.apache.slider.server.management;

import org.apache.slider.server.appmaster.management.LongGauge;
import org.apache.slider.utils.SliderTestBase;
import org.junit.Test;

/**
 * Test gauges.
 */
public class TestGauges extends SliderTestBase {

  @Test
  public void testLongGaugeOperations() throws Throwable {
    LongGauge gauge = new LongGauge();
    assertEquals(0, gauge.get());
    gauge.inc();
    assertEquals(1, gauge.get());
    gauge.inc();
    assertEquals(2, gauge.get());
    gauge.inc();
    assertEquals(3, gauge.get());
    assertEquals(gauge.getValue().longValue(), gauge.get());
    assertEquals(gauge.getCount().longValue(), gauge.get());

    gauge.dec();
    assertEquals(2, gauge.get());
    assertEquals(1, gauge.decToFloor(1));
    assertEquals(1, gauge.get());
    assertEquals(0, gauge.decToFloor(1));
    assertEquals(0, gauge.decToFloor(1));
    assertEquals(0, gauge.decToFloor(0));

    gauge.set(4);
    assertEquals(0, gauge.decToFloor(8));

  }
}
