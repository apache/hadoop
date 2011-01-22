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

import java.util.Random;
import org.apache.hadoop.metrics2.lib.MetricMutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterInt;
import org.apache.hadoop.metrics2.lib.MetricMutableStat;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.util.Contracts;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsSink;

/**
 * An adapter class for metrics sink and associated filters
 */
class MetricsSinkAdapter {

  private final Log LOG = LogFactory.getLog(MetricsSinkAdapter.class);
  private final String name, description, context;
  private final MetricsSink sink;
  private final MetricsFilter sourceFilter, recordFilter, metricFilter;
  private final SinkQueue<MetricsBuffer> queue;
  private final Thread sinkThread;
  private volatile boolean stopping = false;
  private volatile boolean inError = false;
  private final int period, firstRetryDelay, retryCount;
  private final float retryBackoff;
  private final MetricsRegistry registry = new MetricsRegistry("sinkadapter");
  private final MetricMutableStat latency;
  private final MetricMutableCounterInt dropped;
  private final MetricMutableGaugeInt qsize;

  private final Consumer<MetricsBuffer> consumer =
      new Consumer<MetricsBuffer>() {
        public void consume(MetricsBuffer buffer) {
          publishMetrics(buffer);
        }
      };

  MetricsSinkAdapter(String name, String description, MetricsSink sink,
                     String context, MetricsFilter sourceFilter,
                     MetricsFilter recordFilter, MetricsFilter metricFilter,
                     int period, int queueCapacity, int retryDelay,
                     float retryBackoff, int retryCount) {
    this.name = Contracts.checkNotNull(name, "name");
    this.description = description;
    this.sink = Contracts.checkNotNull(sink, "sink object");
    this.context = context;
    this.sourceFilter = sourceFilter;
    this.recordFilter = recordFilter;
    this.metricFilter = metricFilter;
    this.period = Contracts.checkArg(period, period > 0, "period");
    firstRetryDelay =
        Contracts.checkArg(retryDelay, retryDelay > 0, "retry delay");
    this.retryBackoff =
        Contracts.checkArg(retryBackoff, retryBackoff > 1, "backoff factor");
    this.retryCount = retryCount;
    this.queue = new SinkQueue<MetricsBuffer>(
        Contracts.checkArg(queueCapacity, queueCapacity > 0, "queue capacity"));
    latency = registry.newStat("sink."+ name +".latency",
                               "Sink end to end latency", "ops", "time");
    dropped = registry.newCounter("sink."+ name +".dropped",
                                  "Dropped updates per sink", 0);
    qsize = registry.newGauge("sink."+ name + ".qsize", "Queue size", 0);

    sinkThread = new Thread() {
      @Override public void run() {
        publishMetricsFromQueue();
      }
    };
    sinkThread.setName(name);
    sinkThread.setDaemon(true);
  }

  boolean putMetrics(MetricsBuffer buffer, long logicalTime) {
    if (logicalTime % period == 0) {
      LOG.debug("enqueue, logicalTime="+ logicalTime);
      if (queue.enqueue(buffer)) return true;
      dropped.incr();
      return false;
    }
    return true; // OK
  }

  void publishMetricsFromQueue() {
    int retryDelay = firstRetryDelay;
    int n = retryCount;
    int minDelay = Math.min(500, retryDelay * 1000); // millis
    Random rng = new Random(System.nanoTime());
    while (!stopping) {
      try {
        queue.consumeAll(consumer);
        retryDelay = firstRetryDelay;
        n = retryCount;
        inError = false;
      }
      catch (InterruptedException e) {
        LOG.info(name +" thread interrupted.");
      }
      catch (Exception e) {
        if (n > 0) {
          int awhile = rng.nextInt(retryDelay * 1000 - minDelay) + minDelay;
          if (!inError) {
            LOG.error("Got sink exception, retry in "+ awhile +"ms", e);
          }
          retryDelay *= retryBackoff;
          try { Thread.sleep(awhile); }
          catch (InterruptedException e2) {
            LOG.info(name +" thread interrupted while waiting for retry", e2);
          }
          --n;
        }
        else {
          if (!inError) {
            LOG.error("Got sink exception and over retry limit, "+
                      "suppressing further error messages", e);
          }
          queue.clear();
          inError = true; // Don't keep complaining ad infinitum
        }
      }
    }
  }

  void publishMetrics(MetricsBuffer buffer) {
    long ts = 0;
    for (MetricsBuffer.Entry entry : buffer) {
      LOG.debug("sourceFilter="+ sourceFilter);
      if (sourceFilter == null || sourceFilter.accepts(entry.name())) {
        for (MetricsRecordImpl record : entry.records()) {
          if ((context == null || context.equals(record.context())) &&
              (recordFilter == null || recordFilter.accepts(record))) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Pushing record "+ entry.name() +"."+ record.context() +
                        "."+ record.name() +" to "+ name);
            }
            sink.putMetrics(metricFilter == null
                ? record
                : new MetricsRecordFiltered(record, metricFilter));
            if (ts == 0) ts = record.timestamp();
          }
        }
      }
    }
    if (ts > 0) {
      sink.flush();
      latency.add(System.currentTimeMillis() - ts);
    }
    LOG.debug("Done");
  }

  void start() {
    sinkThread.start();
    LOG.info("Sink "+ name +" started");
  }

  void stop() {
    stopping = true;
    sinkThread.interrupt();
    try {
      sinkThread.join();
    }
    catch (InterruptedException e) {
      LOG.warn("Stop interrupted", e);
    }
  }

  String name() {
    return name;
  }

  String description() {
    return description;
  }

  void snapshot(MetricsRecordBuilder rb, boolean all) {
    registry.snapshot(rb, all);
  }

  MetricsSink sink() {
    return sink;
  }

}
