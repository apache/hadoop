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

package org.apache.hadoop.metrics2.util;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.metrics2.lib.Interns.*;

public class TestMetricsCache {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMetricsCache.class);

  @SuppressWarnings("deprecation")
  @Test public void testUpdate() {
    MetricsCache cache = new MetricsCache();
    MetricsRecord mr = makeRecord("r",
        Arrays.asList(makeTag("t", "tv")),
        Arrays.asList(makeMetric("m", 0), makeMetric("m1", 1)));

    MetricsCache.Record cr = cache.update(mr);
    verify(mr).name();
    verify(mr).tags();
    verify(mr).metrics();
    assertEquals("same record size", cr.metrics().size(),
                 ((Collection<AbstractMetric>)mr.metrics()).size());
    assertEquals("same metric value", 0, cr.getMetric("m"));

    MetricsRecord mr2 = makeRecord("r",
        Arrays.asList(makeTag("t", "tv")),
        Arrays.asList(makeMetric("m", 2), makeMetric("m2", 42)));
    cr = cache.update(mr2);
    assertEquals("contains 3 metric", 3, cr.metrics().size());
    checkMetricValue("updated metric value", cr, "m", 2);
    checkMetricValue("old metric value", cr, "m1", 1);
    checkMetricValue("new metric value", cr, "m2", 42);

    MetricsRecord mr3 = makeRecord("r",
        Arrays.asList(makeTag("t", "tv3")), // different tag value
        Arrays.asList(makeMetric("m3", 3)));
    cr = cache.update(mr3); // should get a new record
    assertEquals("contains 1 metric", 1, cr.metrics().size());
    checkMetricValue("updated metric value", cr, "m3", 3);
    // tags cache should be empty so far
    assertEquals("no tags", 0, cr.tags().size());
    // until now
    cr = cache.update(mr3, true);
    assertEquals("Got 1 tag", 1, cr.tags().size());
    assertEquals("Tag value", "tv3", cr.getTag("t"));
    checkMetricValue("Metric value", cr, "m3", 3);
  }

  @SuppressWarnings("deprecation")
  @Test public void testGet() {
    MetricsCache cache = new MetricsCache();
    assertNull("empty", cache.get("r", Arrays.asList(makeTag("t", "t"))));
    MetricsRecord mr = makeRecord("r",
        Arrays.asList(makeTag("t", "t")),
        Arrays.asList(makeMetric("m", 1)));
    cache.update(mr);
    MetricsCache.Record cr = cache.get("r", mr.tags());
    LOG.debug("tags="+ mr.tags() +" cr="+ cr);

    assertNotNull("Got record", cr);
    assertEquals("contains 1 metric", 1, cr.metrics().size());
    checkMetricValue("new metric value", cr, "m", 1);
  }

  /**
   * Make sure metrics tag has a sane hashCode impl
   */
  @Test public void testNullTag() {
    MetricsCache cache = new MetricsCache();
    MetricsRecord mr = makeRecord("r",
        Arrays.asList(makeTag("t", null)),
        Arrays.asList(makeMetric("m", 0), makeMetric("m1", 1)));

    MetricsCache.Record cr = cache.update(mr);
    assertTrue("t value should be null", null == cr.getTag("t"));
  }

  @Test public void testOverflow() {
    MetricsCache cache = new MetricsCache();
    MetricsCache.Record cr;
    Collection<MetricsTag> t0 = Arrays.asList(makeTag("t0", "0"));
    for (int i = 0; i < MetricsCache.MAX_RECS_PER_NAME_DEFAULT + 1; ++i) {
      cr = cache.update(makeRecord("r",
          Arrays.asList(makeTag("t"+ i, ""+ i)),
          Arrays.asList(makeMetric("m", i))));
      checkMetricValue("new metric value", cr, "m", i);
      if (i < MetricsCache.MAX_RECS_PER_NAME_DEFAULT) {
        assertNotNull("t0 is still there", cache.get("r", t0));
      }
    }
    assertNull("t0 is gone", cache.get("r", t0));
  }

  private void checkMetricValue(String description, MetricsCache.Record cr,
      String key, Number val) {
    assertEquals(description, val, cr.getMetric(key));
    assertNotNull("metric not null", cr.getMetricInstance(key));
    assertEquals(description, val, cr.getMetricInstance(key).value());
  }

  private MetricsRecord makeRecord(String name, Collection<MetricsTag> tags,
                                   Collection<AbstractMetric> metrics) {
    MetricsRecord mr = mock(MetricsRecord.class);
    when(mr.name()).thenReturn(name);
    when(mr.tags()).thenReturn(tags);
    when(mr.metrics()).thenReturn(metrics);
    return mr;
  }

  private MetricsTag makeTag(String name, String value) {
    return new MetricsTag(info(name, ""), value);
  }

  private AbstractMetric makeMetric(String name, Number value) {
    AbstractMetric metric = mock(AbstractMetric.class);
    when(metric.name()).thenReturn(name);
    when(metric.value()).thenReturn(value);
    return metric;
  }
}
