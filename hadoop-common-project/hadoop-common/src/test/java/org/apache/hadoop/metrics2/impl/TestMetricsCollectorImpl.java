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

package org.apache.hadoop.metrics2.impl;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.commons.configuration2.SubsetConfiguration;
import static org.apache.hadoop.metrics2.filter.TestPatternFilter.*;
import static org.apache.hadoop.metrics2.lib.Interns.*;

public class TestMetricsCollectorImpl {

  @Test public void recordBuilderShouldNoOpIfFiltered() {
    SubsetConfiguration fc = new ConfigBuilder()
        .add("p.exclude", "foo").subset("p");
    MetricsCollectorImpl mb = new MetricsCollectorImpl();
    mb.setRecordFilter(newGlobFilter(fc));
    MetricsRecordBuilderImpl rb = mb.addRecord("foo");
    rb.tag(info("foo", ""), "value").addGauge(info("g0", ""), 1);
    assertEquals(0, rb.tags().size(), "no tags");
    assertEquals(0, rb.metrics().size(), "no metrics");
    assertNull(rb.getRecord(), "null record");
    assertEquals(0, mb.getRecords().size(), "no records");
  }

  @Test public void testPerMetricFiltering() {
    SubsetConfiguration fc = new ConfigBuilder()
        .add("p.exclude", "foo").subset("p");
    MetricsCollectorImpl mb = new MetricsCollectorImpl();
    mb.setMetricFilter(newGlobFilter(fc));
    MetricsRecordBuilderImpl rb = mb.addRecord("foo");
    rb.tag(info("foo", ""), "").addCounter(info("c0", ""), 0)
      .addGauge(info("foo", ""), 1);
    assertEquals(1, rb.tags().size(), "1 tag");
    assertEquals(1, rb.metrics().size(), "1 metric");
    assertEquals("foo", rb.tags().get(0).name(), "expect foo tag");
    assertEquals("c0", rb.metrics().get(0).name(), "expect c0");
  }
}
