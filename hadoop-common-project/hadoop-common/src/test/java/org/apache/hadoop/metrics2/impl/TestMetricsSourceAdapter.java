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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsAnnotations;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MetricsSourceBuilder;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import static org.apache.hadoop.metrics2.lib.Interns.info;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;

public class TestMetricsSourceAdapter {


  @Test
  public void testPurgeOldMetrics() throws Exception {
    // create test source with a single metric counter of value 1
    PurgableSource source = new PurgableSource();
    MetricsSourceBuilder sb = MetricsAnnotations.newSourceBuilder(source);
    final MetricsSource s = sb.build();

    List<MetricsTag> injectedTags = new ArrayList<MetricsTag>();
    MetricsSourceAdapter sa = new MetricsSourceAdapter(
        "tst", "tst", "testdesc", s, injectedTags, null, null, 1, false);

    MBeanInfo info = sa.getMBeanInfo();
    boolean sawIt = false;
    for (MBeanAttributeInfo mBeanAttributeInfo : info.getAttributes()) {
      sawIt |= mBeanAttributeInfo.getName().equals(source.lastKeyName);
    };
    assertTrue("The last generated metric is not exported to jmx", sawIt);

    Thread.sleep(1000); // skip JMX cache TTL

    info = sa.getMBeanInfo();
    sawIt = false;
    for (MBeanAttributeInfo mBeanAttributeInfo : info.getAttributes()) {
      sawIt |= mBeanAttributeInfo.getName().equals(source.lastKeyName);
    };
    assertTrue("The last generated metric is not exported to jmx", sawIt);
  }

  //generate a new key per each call
  class PurgableSource implements MetricsSource {
    int nextKey = 0;
    String lastKeyName = null;
    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
      MetricsRecordBuilder rb =
          collector.addRecord("purgablesource")
              .setContext("test");
      lastKeyName = "key" + nextKey++;
      rb.addGauge(info(lastKeyName, "desc"), 1);
    }
  }

  @Test
  public void testGetMetricsAndJmx() throws Exception {
    // create test source with a single metric counter of value 0
    TestSource source = new TestSource("test");
    MetricsSourceBuilder sb = MetricsAnnotations.newSourceBuilder(source);
    final MetricsSource s = sb.build();

    List<MetricsTag> injectedTags = new ArrayList<MetricsTag>();
    MetricsSourceAdapter sa = new MetricsSourceAdapter(
        "test", "test", "test desc", s, injectedTags, null, null, 1, false);

    // all metrics are initially assumed to have changed
    MetricsCollectorImpl builder = new MetricsCollectorImpl();
    Iterable<MetricsRecordImpl> metricsRecords = sa.getMetrics(builder, true);

    // Validate getMetrics and JMX initial values
    MetricsRecordImpl metricsRecord = metricsRecords.iterator().next();
    assertEquals(0L,
        metricsRecord.metrics().iterator().next().value().longValue());

    Thread.sleep(100); // skip JMX cache TTL
    assertEquals(0L, (Number)sa.getAttribute("C1"));

    // change metric value
    source.incrementCnt();

    // validate getMetrics and JMX
    builder = new MetricsCollectorImpl();
    metricsRecords = sa.getMetrics(builder, true);
    metricsRecord = metricsRecords.iterator().next();
    assertTrue(metricsRecord.metrics().iterator().hasNext());
    Thread.sleep(100); // skip JMX cache TTL
    assertEquals(1L, (Number)sa.getAttribute("C1"));
  }

  @SuppressWarnings("unused")
  @Metrics(context="test")
  private static class TestSource {
    @Metric("C1 desc") MutableCounterLong c1;
    final MetricsRegistry registry;

    TestSource(String recName) {
      registry = new MetricsRegistry(recName);
    }

    public void incrementCnt() {
      c1.incr();
    }
  }
}
