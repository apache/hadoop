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

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;
import org.apache.hadoop.metrics2.lib.MetricMutableStat;
import org.apache.hadoop.metrics2.lib.MetricMutableGaugeLong;
import org.apache.hadoop.metrics2.lib.AbstractMetricsSource;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
 
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.runners.MockitoJUnitRunner;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import static org.apache.hadoop.test.MoreAsserts.*;
import org.apache.hadoop.metrics2.Metric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;

/**
 * Test the MetricsSystemImpl class
 */
@RunWith(MockitoJUnitRunner.class)
public class TestMetricsSystemImpl {
  private static final Log LOG = LogFactory.getLog(TestMetricsSystemImpl.class);
  @Captor private ArgumentCaptor<MetricsRecord> r1;
  @Captor private ArgumentCaptor<MetricsRecord> r2;
  @Captor private ArgumentCaptor<MetricsRecord> r3;
  private static String hostname = MetricsSystemImpl.getHostname();

  @Test public void testInitFirst() throws Exception {
    new ConfigBuilder().add("default.period", 8)
        .add("source.filter.class",
             "org.apache.hadoop.metrics2.filter.GlobFilter")
        .add("test.*.source.filter.class", "${source.filter.class}")
        .add("test.*.source.filter.exclude", "s1*")
        .add("test.sink.sink3.source.filter.class", "${source.filter.class}")
        .add("test.sink.sink3.source.filter.exclude", "s2*")
        .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-test"));
    MetricsSystemImpl ms = new MetricsSystemImpl("Test");
    ms.start();
    TestSource s1 = ms.register("s1", "s1 desc", new TestSource("s1rec"));
    TestSource s2 = ms.register("s2", "s2 desc", new TestSource("s2rec"));
    TestSource s3 = ms.register("s3", "s3 desc", new TestSource("s3rec"));
    s1.s1.add(0);
    s2.s1.add(0);
    s3.s1.add(0);
    MetricsSink sink1 = mock(MetricsSink.class);
    MetricsSink sink2 = mock(MetricsSink.class);
    MetricsSink sink3 = mock(MetricsSink.class);
    ms.register("sink1", "sink1 desc", sink1);
    ms.register("sink2", "sink2 desc", sink2);
    ms.register("sink3", "sink3 desc", sink3);
    ms.publishMetricsNow(); // publish the metrics
    ms.stop();
    ms.shutdown();

    verify(sink1, times(3)).putMetrics(r1.capture()); // 2 + 1 sys source
    List<MetricsRecord> mr1 = r1.getAllValues();
    verify(sink2, times(3)).putMetrics(r2.capture()); // ditto
    List<MetricsRecord> mr2 = r2.getAllValues();
    verify(sink3, times(2)).putMetrics(r3.capture()); // 1 + 1 (s1, s2 filtered)
    List<MetricsRecord> mr3 = r3.getAllValues();
    checkMetricsRecords(mr1, "s2rec");
    assertEquals("output", mr1, mr2);
    checkMetricsRecords(mr3, "s3rec");
  }

  @Test public void testMultiThreadedPublish() throws Exception {
    new ConfigBuilder().add("*.period", 80)
      .add("test.sink.Collector.queue.capacity", "20")
      .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-test"));
    final MetricsSystemImpl ms = new MetricsSystemImpl("Test");
    ms.start();
    final int numThreads = 10;
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
          // There is a race between setting the source value and
          // which thread takes a snapshot first. Set the value here
          // before any thread starts publishing so they all start
          // with the right value.
          sources[mySource].g1.set(230);
          // Wait for all the threads to come here so we can hammer
          // the system at the same time
          if (!safeAwait(mySource, barrier1)) return;
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
    boolean pass = true;
    String allResults = "";
    for (String r : results) {
      allResults += r + "\n";
      pass = pass && r.equalsIgnoreCase("Passed");
    }
    assertTrue(allResults, pass);
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
        for (Metric m : record.metrics()) {
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
      .add("test.sink.hanging.retry.delay", "1")
      .add("test.sink.hanging.retry.backoff", "1.01")
      .add("test.sink.hanging.retry.count", "0")
      .save(TestMetricsConfig.getTestFilename("hadoop-metrics2-test"));
    MetricsSystemImpl ms = new MetricsSystemImpl("Test");
    ms.start();
    TestSource s = ms.register("s3", "s3 desc", new TestSource("s3rec"));
    s.c1.incr();
    HangingSink hanging = new HangingSink();
    ms.registerSink("hanging", "Hang the sink!", hanging);
    ms.publishMetricsNow();
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

  static void checkMetricsRecords(List<MetricsRecord> recs, String expected) {
    LOG.debug(recs);
    MetricsRecord r = recs.get(0);
    assertEquals("name", expected, r.name());
    assertEquals("tags", new MetricsTag[] {
      new MetricsTag("context", "Metrics context", "test"),
      new MetricsTag("hostName", "Local hostname", hostname)}, r.tags());
    assertEquals("metrics", new Metric[] {
      new MetricCounterLong("c1", "c1 desc", 1),
      new MetricGaugeLong("g1", "g1 desc", 2),
      new MetricCounterLong("s1_num_ops", "Number of ops for s1 desc", 1),
      new MetricGaugeDouble("s1_avg_time", "Average time for s1 desc", 0)},
      r.metrics());

    // Skip the system metrics for now.
    // MetricsRecord r1 = recs.get(1);
  }

  private static class TestSource extends AbstractMetricsSource {
    final MetricMutableCounterLong c1;
    final MetricMutableGaugeLong g1;
    final MetricMutableStat s1;

    TestSource(String name) {
      super(name);
      registry.setContext("test");
      c1 = registry.newCounter("c1", "c1 desc", 1L);
      g1 = registry.newGauge("g1", "g1 desc", 2L);
      s1 = registry.newStat("s1", "s1 desc", "ops", "time");
    }
  }

}
