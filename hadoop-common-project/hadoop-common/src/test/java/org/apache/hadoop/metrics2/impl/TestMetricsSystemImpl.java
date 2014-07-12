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

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import javax.annotation.Nullable;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.MetricsException;
import static org.apache.hadoop.test.MoreAsserts.*;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.annotation.*;
import static org.apache.hadoop.metrics2.lib.Interns.*;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

/**
 * Test the MetricsSystemImpl class
 */
@RunWith(MockitoJUnitRunner.class)
public class TestMetricsSystemImpl {
  private static final Log LOG = LogFactory.getLog(TestMetricsSystemImpl.class);
  
  static { DefaultMetricsSystem.setMiniClusterMode(true); }
  
  @Captor private ArgumentCaptor<MetricsRecord> r1;
  @Captor private ArgumentCaptor<MetricsRecord> r2;
  private static String hostname = MetricsSystemImpl.getHostname();

  public static class TestSink implements MetricsSink {

    @Override public void putMetrics(MetricsRecord record) {
      LOG.debug(record);
    }

    @Override public void flush() {}

    @Override public void init(SubsetConfiguration conf) {
      LOG.debug(MetricsConfig.toString(conf));
    }
  }

  @Test public void testInitFirstVerifyStopInvokedImmediately() throws Exception {
    DefaultMetricsSystem.shutdown();
    new ConfigBuilder().add("*.period", 8)
        //.add("test.sink.plugin.urls", getPluginUrlsAsString())
        .add("Test.sink.test.class", TestSink.class.getName())
        .add("Test.*.source.filter.exclude", "s0")
        .add("Test.source.s1.metric.filter.exclude", "X*")
        .add("Test.sink.sink1.metric.filter.exclude", "Y*")
        .add("Test.sink.sink2.metric.filter.exclude", "Y*")
        .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-test"));
    MetricsSystemImpl ms = new MetricsSystemImpl("Test");
    ms.start();
    ms.register("s0", "s0 desc", new TestSource("s0rec"));
    TestSource s1 = ms.register("s1", "s1 desc", new TestSource("s1rec"));
    s1.c1.incr();
    s1.xxx.incr();
    s1.g1.set(2);
    s1.yyy.incr(2);
    s1.s1.add(0);
    MetricsSink sink1 = mock(MetricsSink.class);
    MetricsSink sink2 = mock(MetricsSink.class);
    ms.registerSink("sink1", "sink1 desc", sink1);
    ms.registerSink("sink2", "sink2 desc", sink2);
    ms.publishMetricsNow(); // publish the metrics
    ms.stop();
    ms.shutdown();

    //When we call stop, at most two sources will be consumed by each sink thread.
    verify(sink1, atMost(2)).putMetrics(r1.capture());
    List<MetricsRecord> mr1 = r1.getAllValues();
    verify(sink2, atMost(2)).putMetrics(r2.capture());
    List<MetricsRecord> mr2 = r2.getAllValues();
    if (mr1.size() != 0 && mr2.size() != 0) {
      checkMetricsRecords(mr1);
      assertEquals("output", mr1, mr2);
    } else if (mr1.size() != 0) {
      checkMetricsRecords(mr1);
    } else if (mr2.size() != 0) {
      checkMetricsRecords(mr2);
    }
  }

  @Test public void testInitFirstVerifyCallBacks() throws Exception {
    DefaultMetricsSystem.shutdown(); 
    new ConfigBuilder().add("*.period", 8)
        //.add("test.sink.plugin.urls", getPluginUrlsAsString())
        .add("Test.sink.test.class", TestSink.class.getName())
        .add("Test.*.source.filter.exclude", "s0")
        .add("Test.source.s1.metric.filter.exclude", "X*")
        .add("Test.sink.sink1.metric.filter.exclude", "Y*")
        .add("Test.sink.sink2.metric.filter.exclude", "Y*")
        .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-test"));
    MetricsSystemImpl ms = new MetricsSystemImpl("Test");
    ms.start();
    ms.register("s0", "s0 desc", new TestSource("s0rec"));
    TestSource s1 = ms.register("s1", "s1 desc", new TestSource("s1rec"));
    s1.c1.incr();
    s1.xxx.incr();
    s1.g1.set(2);
    s1.yyy.incr(2);
    s1.s1.add(0);
    MetricsSink sink1 = mock(MetricsSink.class);
    MetricsSink sink2 = mock(MetricsSink.class);
    ms.registerSink("sink1", "sink1 desc", sink1);
    ms.registerSink("sink2", "sink2 desc", sink2);
    ms.publishMetricsNow(); // publish the metrics

    try {
      verify(sink1, timeout(200).times(2)).putMetrics(r1.capture());
      verify(sink2, timeout(200).times(2)).putMetrics(r2.capture());
    } finally {
      ms.stop();
      ms.shutdown();
    }
    //When we call stop, at most two sources will be consumed by each sink thread.
    List<MetricsRecord> mr1 = r1.getAllValues();
    List<MetricsRecord> mr2 = r2.getAllValues();
    checkMetricsRecords(mr1);
    assertEquals("output", mr1, mr2);

  }
  
  @Test public void testMultiThreadedPublish() throws Exception {
    final int numThreads = 10;
    new ConfigBuilder().add("*.period", 80)
      .add("Test.sink.Collector." + MetricsConfig.QUEUE_CAPACITY_KEY,
              numThreads)
      .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-test"));
    final MetricsSystemImpl ms = new MetricsSystemImpl("Test");
    ms.start();
    final CollectingSink sink = new CollectingSink(numThreads);
    ms.registerSink("Collector",
        "Collector of values from all threads.", sink);
    final TestSource[] sources = new TestSource[numThreads];
    final Thread[] threads = new Thread[numThreads];
    final String[] results = new String[numThreads];
    final CyclicBarrier barrier1 = new CyclicBarrier(numThreads),
        barrier2 = new CyclicBarrier(numThreads);
    for (int i = 0; i < numThreads; i++) {
      sources[i] = ms.register("threadSource" + i,
          "A source of my threaded goodness.",
          new TestSource("threadSourceRec" + i));
      threads[i] = new Thread(new Runnable() {
        private boolean safeAwait(int mySource, CyclicBarrier barrier) {
          try {
            barrier1.await(2, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            results[mySource] = "Interrupted";
            return false;
          } catch (BrokenBarrierException e) {
            results[mySource] = "Broken Barrier";
            return false;
          } catch (TimeoutException e) {
            results[mySource] = "Timed out on barrier";
            return false;
          }
          return true;
        }
        
        @Override
        public void run() {
          int mySource = Integer.parseInt(Thread.currentThread().getName());
          if (sink.collected[mySource].get() != 0L) {
            results[mySource] = "Someone else collected my metric!";
            return;
          }
          // Wait for all the threads to come here so we can hammer
          // the system at the same time
          if (!safeAwait(mySource, barrier1)) return;
          sources[mySource].g1.set(230);
          ms.publishMetricsNow();
          // Since some other thread may have snatched my metric,
          // I need to wait for the threads to finish before checking.
          if (!safeAwait(mySource, barrier2)) return;
          if (sink.collected[mySource].get() != 230L) {
            results[mySource] = "Metric not collected!";
            return;
          }
          results[mySource] = "Passed";
        }
      }, "" + i);
    }
    for (Thread t : threads)
      t.start();
    for (Thread t : threads)
      t.join();
    assertEquals(0L, ms.droppedPubAll.value());
    assertTrue(StringUtils.join("\n", Arrays.asList(results)),
      Iterables.all(Arrays.asList(results), new Predicate<String>() {
        @Override
        public boolean apply(@Nullable String input) {
          return input.equalsIgnoreCase("Passed");
        }
      }));
    ms.stop();
    ms.shutdown();
  }

  private static class CollectingSink implements MetricsSink {
    private final AtomicLong[] collected;
    
    public CollectingSink(int capacity) {
      collected = new AtomicLong[capacity];
      for (int i = 0; i < capacity; i++) {
        collected[i] = new AtomicLong();
      }
    }
    
    @Override
    public void init(SubsetConfiguration conf) {
    }

    @Override
    public void putMetrics(MetricsRecord record) {
      final String prefix = "threadSourceRec";
      if (record.name().startsWith(prefix)) {
        final int recordNumber = Integer.parseInt(
            record.name().substring(prefix.length()));
        ArrayList<String> names = new ArrayList<String>();
        for (AbstractMetric m : record.metrics()) {
          if (m.name().equalsIgnoreCase("g1")) {
            collected[recordNumber].set(m.value().longValue());
            return;
          }
          names.add(m.name());
        }
      }
    }

    @Override
    public void flush() {
    }
  }

  @Test public void testHangingSink() {
    new ConfigBuilder().add("*.period", 8)
      .add("Test.sink.test.class", TestSink.class.getName())
      .add("Test.sink.hanging.retry.delay", "1")
      .add("Test.sink.hanging.retry.backoff", "1.01")
      .add("Test.sink.hanging.retry.count", "0")
      .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-test"));
    MetricsSystemImpl ms = new MetricsSystemImpl("Test");
    ms.start();
    TestSource s = ms.register("s3", "s3 desc", new TestSource("s3rec"));
    s.c1.incr();
    HangingSink hanging = new HangingSink();
    ms.registerSink("hanging", "Hang the sink!", hanging);
    ms.publishMetricsNow();
    assertEquals(1L, ms.droppedPubAll.value());
    assertFalse(hanging.getInterrupted());
    ms.stop();
    ms.shutdown();
    assertTrue(hanging.getInterrupted());
    assertTrue("The sink didn't get called after its first hang " +
               "for subsequent records.", hanging.getGotCalledSecondTime());
  }

  private static class HangingSink implements MetricsSink {
    private volatile boolean interrupted;
    private boolean gotCalledSecondTime;
    private boolean firstTime = true;

    public boolean getGotCalledSecondTime() {
      return gotCalledSecondTime;
    }

    public boolean getInterrupted() {
      return interrupted;
    }

    @Override
    public void init(SubsetConfiguration conf) {
    }

    @Override
    public void putMetrics(MetricsRecord record) {
      // No need to hang every time, just the first record.
      if (!firstTime) {
        gotCalledSecondTime = true;
        return;
      }
      firstTime = false;
      try {
        Thread.sleep(10 * 1000);
      } catch (InterruptedException ex) {
        interrupted = true;
      }
    }

    @Override
    public void flush() {
    }
  }

  @Test public void testRegisterDups() {
    MetricsSystem ms = new MetricsSystemImpl();
    TestSource ts1 = new TestSource("ts1");
    TestSource ts2 = new TestSource("ts2");
    ms.register("ts1", "", ts1);
    MetricsSource s1 = ms.getSource("ts1");
    assertNotNull(s1);
    // should work when metrics system is not started
    ms.register("ts1", "", ts2);
    MetricsSource s2 = ms.getSource("ts1");
    assertNotNull(s2);
    assertNotSame(s1, s2);
    ms.shutdown();
  }

  @Test(expected=MetricsException.class) public void testRegisterDupError() {
    MetricsSystem ms = new MetricsSystemImpl("test");
    TestSource ts = new TestSource("ts");
    ms.register(ts);
    ms.register(ts);
  }

  @Test public void testStartStopStart() {
    DefaultMetricsSystem.shutdown(); // Clear pre-existing source names.
    MetricsSystemImpl ms = new MetricsSystemImpl("test");
    TestSource ts = new TestSource("ts");
    ms.start();
    ms.register("ts", "", ts);
    MetricsSourceAdapter sa = ms.getSourceAdapter("ts");
    assertNotNull(sa);
    assertNotNull(sa.getMBeanName());
    ms.stop();
    ms.shutdown();
    ms.start();
    sa = ms.getSourceAdapter("ts");
    assertNotNull(sa);
    assertNotNull(sa.getMBeanName());
    ms.stop();
    ms.shutdown();
  }

  private void checkMetricsRecords(List<MetricsRecord> recs) {
    LOG.debug(recs);
    MetricsRecord r = recs.get(0);
    assertEquals("name", "s1rec", r.name());
    assertEquals("tags", new MetricsTag[] {
      tag(MsInfo.Context, "test"),
      tag(MsInfo.Hostname, hostname)}, r.tags());
    assertEquals("metrics", MetricsLists.builder("")
      .addCounter(info("C1", "C1 desc"), 1L)
      .addGauge(info("G1", "G1 desc"), 2L)
      .addCounter(info("S1NumOps", "Number of ops for s1"), 1L)
      .addGauge(info("S1AvgTime", "Average time for s1"), 0.0)
      .metrics(), r.metrics());

    r = recs.get(1);
    assertTrue("NumActiveSinks should be 3", Iterables.contains(r.metrics(),
               new MetricGaugeInt(MsInfo.NumActiveSinks, 3)));
  }

  @Metrics(context="test")
  private static class TestSource {
    @Metric("C1 desc") MutableCounterLong c1;
    @Metric("XXX desc") MutableCounterLong xxx;
    @Metric("G1 desc") MutableGaugeLong g1;
    @Metric("YYY desc") MutableGaugeLong yyy;
    @Metric MutableRate s1;
    final MetricsRegistry registry;

    TestSource(String recName) {
      registry = new MetricsRegistry(recName);
    }
  }

  private static String getPluginUrlsAsString() {
    return "file:metrics2-test-plugin.jar";
  }
}
