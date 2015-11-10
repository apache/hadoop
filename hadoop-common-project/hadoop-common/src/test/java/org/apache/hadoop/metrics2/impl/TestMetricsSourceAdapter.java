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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
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

import org.apache.log4j.Logger;
import org.junit.Test;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;

public class TestMetricsSourceAdapter {
  private static final int RACE_TEST_RUNTIME = 10000; // 10 seconds

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
  private static class PurgableSource implements MetricsSource {
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

  /**
   * Test a race condition when updating the JMX cache (HADOOP-12482):
   * 1. Thread A reads the JMX metric every 2 JMX cache TTL. It marks the JMX
   *    cache to be updated by marking lastRecs to null. After this it adds a
   *    new key to the metrics. The next call to read should pick up this new
   *    key.
   * 2. Thread B triggers JMX metric update every 1 JMX cache TTL. It assigns
   *    lastRecs to a new object (not null any more).
   * 3. Thread A tries to read JMX metric again, sees lastRecs is not null and
   *    does not update JMX cache. As a result the read does not pickup the new
   *    metric.
   * @throws Exception
   */
  @Test
  public void testMetricCacheUpdateRace() throws Exception {
    // Create test source with a single metric counter of value 1.
    TestMetricsSource source = new TestMetricsSource();
    MetricsSourceBuilder sourceBuilder =
        MetricsAnnotations.newSourceBuilder(source);

    final long JMX_CACHE_TTL = 250; // ms
    List<MetricsTag> injectedTags = new ArrayList<>();
    MetricsSourceAdapter sourceAdapter =
        new MetricsSourceAdapter("test", "test",
            "test JMX cache update race condition", sourceBuilder.build(),
            injectedTags, null, null, JMX_CACHE_TTL, false);

    ScheduledExecutorService updaterExecutor =
        Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().build());
    ScheduledExecutorService readerExecutor =
        Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().build());

    final AtomicBoolean hasError = new AtomicBoolean(false);

    // Wake up every 1 JMX cache TTL to set lastRecs before updateJmxCache() is
    // called.
    SourceUpdater srcUpdater = new SourceUpdater(sourceAdapter, hasError);
    ScheduledFuture<?> updaterFuture =
        updaterExecutor.scheduleAtFixedRate(srcUpdater,
            sourceAdapter.getJmxCacheTTL(), sourceAdapter.getJmxCacheTTL(),
            TimeUnit.MILLISECONDS);
    srcUpdater.setFuture(updaterFuture);

    // Wake up every 2 JMX cache TTL so updateJmxCache() will try to update
    // JMX cache.
    SourceReader srcReader = new SourceReader(source, sourceAdapter, hasError);
    ScheduledFuture<?> readerFuture =
        readerExecutor.scheduleAtFixedRate(srcReader,
            0, // set JMX info cache at the beginning
            2 * sourceAdapter.getJmxCacheTTL(), TimeUnit.MILLISECONDS);
    srcReader.setFuture(readerFuture);

    // Let the threads do their work.
    Thread.sleep(RACE_TEST_RUNTIME);

    assertFalse("Hit error", hasError.get());

    // cleanup
    updaterExecutor.shutdownNow();
    readerExecutor.shutdownNow();
    updaterExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
    readerExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
  }

  /**
   * Thread safe source: stores a key value pair. Allows thread safe key-value
   * pair reads/writes.
   */
  private static class TestMetricsSource implements MetricsSource {
    private String key = "key0";
    private int val = 0;

    synchronized String getKey() {
      return key;
    }

    synchronized void setKV(final String newKey, final int newVal) {
      key = newKey;
      val = newVal;
    }

    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
      MetricsRecordBuilder rb =
          collector.addRecord("TestMetricsSource").setContext("test");
      synchronized(this) {
        rb.addGauge(info(key, "TestMetricsSource key"), val);
      }
    }
  }

  /**
   * An thread that updates the metrics source every 1 JMX cache TTL
   */
  private static class SourceUpdater implements Runnable {
    private MetricsSourceAdapter sa = null;
    private ScheduledFuture<?> future = null;
    private AtomicBoolean hasError = null;
    private static final Logger LOG = Logger.getLogger(SourceUpdater.class);

    public SourceUpdater(MetricsSourceAdapter sourceAdapter,
        AtomicBoolean err) {
      sa = sourceAdapter;
      hasError = err;
    }

    public void setFuture(ScheduledFuture<?> f) {
      future = f;
    }

    @Override
    public void run() {
      MetricsCollectorImpl builder = new MetricsCollectorImpl();
      try {
        // This resets lastRecs.
        sa.getMetrics(builder, true);
        LOG.info("reset lastRecs");
      } catch (Exception e) {
        // catch all errors
        hasError.set(true);
        LOG.error(e.getStackTrace());
      } finally {
        if (hasError.get()) {
          LOG.error("Hit error, stopping now");
          future.cancel(false);
        }
      }
    }
  }

  /**
   * An thread that reads the metrics source every JMX cache TTL. After each
   * read it updates the metric source to report a new key. The next read must
   * be able to pick up this new key.
   */
  private static class SourceReader implements Runnable {
    private MetricsSourceAdapter sa = null;
    private TestMetricsSource src = null;
    private int cnt = 0;
    private ScheduledFuture<?> future = null;
    private AtomicBoolean hasError = null;
    private static final Logger LOG = Logger.getLogger(SourceReader.class);

    public SourceReader(
        TestMetricsSource source, MetricsSourceAdapter sourceAdapter,
        AtomicBoolean err) {
      src = source;
      sa = sourceAdapter;
      hasError = err;
    }

    public void setFuture(ScheduledFuture<?> f) {
      future = f;
    }

    @Override
    public void run() {
      try {
        // This will trigger updateJmxCache().
        MBeanInfo info = sa.getMBeanInfo();
        final String key = src.getKey();
        for (MBeanAttributeInfo mBeanAttributeInfo : info.getAttributes()) {
          // Found the new key, update the metric source and move on.
          if (mBeanAttributeInfo.getName().equals(key)) {
            LOG.info("found key/val=" + cnt + "/" + cnt);
            cnt++;
            src.setKV("key" + cnt, cnt);
            return;
          }
        }
        LOG.error("key=" + key + " not found. Stopping now.");
        hasError.set(true);
      } catch (Exception e) {
        // catch other errors
        hasError.set(true);
        LOG.error(e.getStackTrace());
      } finally {
        if (hasError.get()) {
          future.cancel(false);
        }
      }
    }
  }
}
