/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.metrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

import junit.framework.TestCase;

public class TestMetricsMBeanBase extends TestCase {

  private class TestStatistics extends MetricsMBeanBase {
    public TestStatistics(MetricsRegistry registry) {
      super(registry, "TestStatistics");
    }
  }

  private MetricsRegistry registry;
  private MetricsRecord metricsRecord;
  private TestStatistics stats;
  private MetricsRate metricsRate;
  private MetricsIntValue intValue;
  private MetricsTimeVaryingRate varyRate;

  public void setUp() {
    this.registry = new MetricsRegistry();
    this.metricsRate = new MetricsRate("metricsRate", registry, "test");
    this.intValue = new MetricsIntValue("intValue", registry, "test");
    this.varyRate = new MetricsTimeVaryingRate("varyRate", registry, "test");
    this.stats = new TestStatistics(registry);
    MetricsContext context = MetricsUtil.getContext("hbase");
    this.metricsRecord = MetricsUtil.createRecord(context, "test");
    this.metricsRecord.setTag("TestStatistics", "test");
    //context.registerUpdater(this);

  }

  public void tearDown() {

  }

  public void testGetAttribute() throws Exception {
    this.metricsRate.inc(2);
    Thread.sleep(1000);
    this.metricsRate.pushMetric(this.metricsRecord);
    this.intValue.set(5);
    this.intValue.pushMetric(this.metricsRecord);
    this.varyRate.inc(10);
    this.varyRate.inc(50);
    this.varyRate.pushMetric(this.metricsRecord);


    assertEquals( 2.0, (Float)this.stats.getAttribute("metricsRate"), 0.005 );
    assertEquals( 5, this.stats.getAttribute("intValue") );
    assertEquals( 10L, this.stats.getAttribute("varyRateMinTime") );
    assertEquals( 50L, this.stats.getAttribute("varyRateMaxTime") );
    assertEquals( 30L, this.stats.getAttribute("varyRateAvgTime") );
    assertEquals( 2, this.stats.getAttribute("varyRateNumOps") );
  }

  public void testGetMBeanInfo() {
    MBeanInfo info = this.stats.getMBeanInfo();
    MBeanAttributeInfo[] attributes = info.getAttributes();
    assertEquals( 6, attributes.length );

    Map<String,MBeanAttributeInfo> attributeByName =
        new HashMap<String,MBeanAttributeInfo>(attributes.length);
    for (MBeanAttributeInfo attr : attributes)
      attributeByName.put(attr.getName(), attr);

    assertAttribute( attributeByName.get("metricsRate"),
        "metricsRate", "java.lang.Float", "test");
    assertAttribute( attributeByName.get("intValue"),
        "intValue", "java.lang.Integer", "test");
    assertAttribute( attributeByName.get("varyRateMinTime"),
        "varyRateMinTime", "java.lang.Long", "test");
    assertAttribute( attributeByName.get("varyRateMaxTime"),
        "varyRateMaxTime", "java.lang.Long", "test");
    assertAttribute( attributeByName.get("varyRateAvgTime"),
        "varyRateAvgTime", "java.lang.Long", "test");
    assertAttribute( attributeByName.get("varyRateNumOps"),
        "varyRateNumOps", "java.lang.Integer", "test");
  }

  protected void assertAttribute(MBeanAttributeInfo attr, String name,
      String type, String description) {

    assertEquals(attr.getName(), name);
    assertEquals(attr.getType(), type);
    assertEquals(attr.getDescription(), description);
  }

}
