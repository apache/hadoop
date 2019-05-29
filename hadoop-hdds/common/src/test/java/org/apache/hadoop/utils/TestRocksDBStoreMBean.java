/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.management.MBeanServer;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test the JMX interface for the rocksdb metastore implementation.
 */
public class TestRocksDBStoreMBean {
  
  private Configuration conf;
  
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();

    conf.set(OzoneConfigKeys.OZONE_METADATA_STORE_IMPL,
        OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_ROCKSDB);
  }
  

  @Test
  public void testJmxBeans() throws Exception {

    RocksDBStore metadataStore = getTestRocksDBStoreWithData();

    MBeanServer platformMBeanServer =
        ManagementFactory.getPlatformMBeanServer();
    Thread.sleep(2000);

    Object keysWritten = platformMBeanServer
        .getAttribute(metadataStore.getStatMBeanName(), "NUMBER_KEYS_WRITTEN");

    assertEquals(10L, keysWritten);

    Object dbWriteAverage = platformMBeanServer
        .getAttribute(metadataStore.getStatMBeanName(), "DB_WRITE_AVERAGE");
    assertTrue((double) dbWriteAverage > 0);

    metadataStore.close();

  }

  @Test()
  public void testDisabledStat() throws Exception {
    File testDir = GenericTestUtils
        .getTestDir(getClass().getSimpleName() + "-withoutstat");

    conf.set(OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS,
        OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS_OFF);

    RocksDBStore metadataStore =
        (RocksDBStore) MetadataStoreBuilder.newBuilder().setConf(conf)
            .setCreateIfMissing(true).setDbFile(testDir).build();

    Assert.assertNull(metadataStore.getStatMBeanName());
  }

  @Test
  public void testMetricsSystemIntegration() throws Exception {

    RocksDBStore metadataStore = getTestRocksDBStoreWithData();
    Thread.sleep(2000);

    MetricsSystem ms = DefaultMetricsSystem.instance();
    MetricsSource rdbSource =
        ms.getSource("Rocksdb_TestRocksDBStoreMBean-withstat");

    BufferedMetricsCollector metricsCollector = new BufferedMetricsCollector();
    rdbSource.getMetrics(metricsCollector, true);

    Map<String, Double> metrics = metricsCollector.getMetricsRecordBuilder()
        .getMetrics();
    assertTrue(10.0 == metrics.get("NUMBER_KEYS_WRITTEN"));
    assertTrue(metrics.get("DB_WRITE_AVERAGE") > 0);
    metadataStore.close();
  }

  private RocksDBStore getTestRocksDBStoreWithData() throws IOException {
    File testDir =
        GenericTestUtils.getTestDir(getClass().getSimpleName() + "-withstat");

    conf.set(OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS, "ALL");

    RocksDBStore metadataStore =
        (RocksDBStore) MetadataStoreBuilder.newBuilder().setConf(conf)
            .setCreateIfMissing(true).setDbFile(testDir).build();

    for (int i = 0; i < 10; i++) {
      metadataStore.put("key".getBytes(UTF_8), "value".getBytes(UTF_8));
    }

    return metadataStore;
  }
}

/**
 * Test class to buffer a single MetricsRecordBuilder instance.
 */
class BufferedMetricsCollector implements MetricsCollector {

  private BufferedMetricsRecordBuilderImpl metricsRecordBuilder;

  BufferedMetricsCollector() {
    metricsRecordBuilder = new BufferedMetricsRecordBuilderImpl();
  }

  public BufferedMetricsRecordBuilderImpl getMetricsRecordBuilder() {
    return metricsRecordBuilder;
  }

  @Override
  public MetricsRecordBuilder addRecord(String s) {
    metricsRecordBuilder.setContext(s);
    return metricsRecordBuilder;
  }

  @Override
  public MetricsRecordBuilder addRecord(MetricsInfo metricsInfo) {
    return metricsRecordBuilder;
  }

  /**
   * Test class to buffer a single snapshot of metrics.
   */
  class BufferedMetricsRecordBuilderImpl extends MetricsRecordBuilder {

    private Map<String, Double> metrics = new HashMap<>();
    private String contextName;

    public Map<String, Double> getMetrics() {
      return metrics;
    }

    @Override
    public MetricsRecordBuilder tag(MetricsInfo metricsInfo, String s) {
      return null;
    }

    @Override
    public MetricsRecordBuilder add(MetricsTag metricsTag) {
      return null;
    }

    @Override
    public MetricsRecordBuilder add(AbstractMetric abstractMetric) {
      return null;
    }

    @Override
    public MetricsRecordBuilder setContext(String s) {
      this.contextName = s;
      return this;
    }

    @Override
    public MetricsRecordBuilder addCounter(MetricsInfo metricsInfo, int i) {
      return null;
    }

    @Override
    public MetricsRecordBuilder addCounter(MetricsInfo metricsInfo, long l) {
      metrics.put(metricsInfo.name(), (double)l);
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo metricsInfo, int i) {
      return null;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo metricsInfo, long l) {
      return null;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo metricsInfo, float v) {
      return null;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo metricsInfo, double v) {
      metrics.put(metricsInfo.name(), v);
      return this;
    }

    @Override
    public MetricsCollector parent() {
      return null;
    }
  }
}