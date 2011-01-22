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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.metrics2.Metric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;

public class TestMetricsCache {
  private static final Log LOG = LogFactory.getLog(TestMetricsCache.class);

  @Test public void testUpdate() {
    MetricsCache cache = new MetricsCache();
    MetricsRecord mr = makeRecord("r",
        Arrays.asList(makeTag("t", "tv")),
        Arrays.asList(makeMetric("m", 0), makeMetric("m1", 1)));

    MetricsCache.Record cr = cache.update(mr);
    verify(mr).name();
    verify(mr).tags();
    verify(mr).metrics();
    assertEquals("same record size", cr.metrics.size(),
                 ((Collection<Metric>)mr.metrics()).size());
    assertEquals("same metric value", 0, cr.getMetric("m"));

    MetricsRecord mr2 = makeRecord("r",
        Arrays.asList(makeTag("t", "tv")),
        Arrays.asList(makeMetric("m", 2), makeMetric("m2", 42)));
    cr = cache.update(mr2);
    assertEquals("contains 3 metric", 3, cr.metrics.size());
    assertEquals("updated metric value", 2, cr.getMetric("m"));
    assertEquals("old metric value", 1, cr.getMetric("m1"));
    assertEquals("new metric value", 42, cr.getMetric("m2"));

    MetricsRecord mr3 = makeRecord("r",
        Arrays.asList(makeTag("t", "tv3")), // different tag value
        Arrays.asList(makeMetric("m3", 3)));
    cr = cache.update(mr3); // should get a new record
    assertEquals("contains 1 metric", 1, cr.metrics.size());
    assertEquals("updated metric value", 3, cr.getMetric("m3"));
    // tags cache should be empty so far
    assertEquals("no tags", 0, cr.tags.size());
    // until now
    cr = cache.update(mr3, true);
    assertEquals("Got 1 tag", 1, cr.tags.size());
    assertEquals("Tag value", "tv3", cr.getTag("t"));
    assertEquals("Metric value", 3, cr.getMetric("m3"));
  }

  @Test public void testGet() {
    MetricsCache cache = new MetricsCache();
    assertNull("empty", cache.get("r", Arrays.asList(makeTag("t", "t"))));
    MetricsRecord mr = makeRecord("r",
        Arrays.asList(makeTag("t", "t")),
        Arrays.asList(makeMetric("m", 1)));
    cache.update(mr);
    MetricsCache.Record cr = cache.get("r", (Collection<MetricsTag>)mr.tags());
    LOG.debug("tags="+ (Collection<MetricsTag>)mr.tags() +" cr="+ cr);

    assertNotNull("Got record", cr);
    assertEquals("contains 1 metric", 1, cr.metrics.size());
    assertEquals("new metric value", 1, cr.getMetric("m"));
  }

  private MetricsRecord makeRecord(String name, Collection<MetricsTag> tags,
                                   Collection<Metric> metrics) {
    MetricsRecord mr = mock(MetricsRecord.class);
    when(mr.name()).thenReturn(name);
    when(mr.tags()).thenReturn(tags);
    when(mr.metrics()).thenReturn(metrics);
    return mr;
  }

  private MetricsTag makeTag(String name, String value) {
    return new MetricsTag(name, "", value);
  }

  private Metric makeMetric(String name, Number value) {
    Metric metric = mock(Metric.class);
    when(metric.name()).thenReturn(name);
    when(metric.value()).thenReturn(value);
    return metric;
  }
}
