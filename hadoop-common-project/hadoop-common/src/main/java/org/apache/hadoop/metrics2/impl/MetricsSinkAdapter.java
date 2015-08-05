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

import java.io.Closeable;
import java.util.Random;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import static org.apache.hadoop.metrics2.util.Contracts.*;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.util.Time;

/**
 * An adapter class for metrics sink and associated filters
 */
class MetricsSinkAdapter implements SinkQueue.Consumer<MetricsBuffer> {

  private final Log LOG = LogFactory.getLog(MetricsSinkAdapter.class);
  private final String name, description, context;
  private final MetricsSink sink;
  private final MetricsFilter sourceFilter, recordFilter, metricFilter;
  private final SinkQueue<MetricsBuffer> queue;
  private final Thread sinkThread;
  private volatile boolean stopping = false;
  private volatile boolean inError = false;
  private final int period, firstRetryDelay, retryCount;
  private final long oobPutTimeout;
  private final float retryBackoff;
  private final MetricsRegistry registry = new MetricsRegistry("sinkadapter");
  private final MutableStat latency;
  private final MutableCounterInt dropped;
  private final MutableGaugeInt qsize;

  MetricsSinkAdapter(String name, String description, MetricsSink sink,
                     String context, MetricsFilter sourceFilter,
                     MetricsFilter recordFilter, MetricsFilter metricFilter,
                     int period, int queueCapacity, int retryDelay,
                     float retryBackoff, int retryCount) {
    this.name = checkNotNull(name, "name");
    this.description = description;
    this.sink = checkNotNull(sink, "sink object");
    this.context = context;
    this.sourceFilter = sourceFilter;
    this.recordFilter = recordFilter;
    this.metricFilter = metricFilter;
    this.period = checkArg(period, period > 0, "period");
    firstRetryDelay = checkArg(retryDelay, retryDelay > 0, "retry delay");
    this.retryBackoff = checkArg(retryBackoff, retryBackoff>1, "retry backoff");
    oobPutTimeout = (long)
        (firstRetryDelay * Math.pow(retryBackoff, retryCount) * 1000);
    this.retryCount = retryCount;
    this.queue = new SinkQueue<MetricsBuffer>(checkArg(queueCapacity,
        queueCapacity > 0, "queue capacity"));
    latency = registry.newRate("Sink_"+ name, "Sink end to end latency", false);
    dropped = registry.newCounter("Sink_"+ name +"Dropped",
                                  "Dropped updates per sink", 0);
    qsize = registry.newGauge("Sink_"+ name + "Qsize", "Queue size", 0);

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
      if (queue.enqueue(buffer)) {
        refreshQueueSizeGauge();
        return true;
      }
      dropped.incr();
      return false;
    }
    return true; // OK
  }
  
  public boolean putMetricsImmediate(MetricsBuffer buffer) {
    WaitableMetricsBuffer waitableBuffer =
        new WaitableMetricsBuffer(buffer);
    if (queue.enqueue(waitableBuffer)) {
      refreshQueueSizeGauge();
    } else {
      LOG.warn(name + " has a full queue and can't consume the given metrics.");
      dropped.incr();
      return false;
    }
    if (!waitableBuffer.waitTillNotified(oobPutTimeout)) {
      LOG.warn(name +
          " couldn't fulfill an immediate putMetrics request in time." +
          " Abandoning.");
      return false;
    }
    return true;
  }

  void publishMetricsFromQueue() {
    int retryDelay = firstRetryDelay;
    int n = retryCount;
    int minDelay = Math.min(500, retryDelay * 1000); // millis
    Random rng = new Random(System.nanoTime());
    while (!stopping) {
      try {
        queue.consumeAll(this);
        refreshQueueSizeGauge();
        retryDelay = firstRetryDelay;
        n = retryCount;
        inError = false;
      }
      catch (InterruptedException e) {
        LOG.info(name +" thread interrupted.");
      }
      catch (Exception e) {
        if (n > 0) {
          int retryWindow = Math.max(0, 1000 / 2 * retryDelay - minDelay);
          int awhile = rng.nextInt(retryWindow) + minDelay;
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
          refreshQueueSizeGauge();
          inError = true; // Don't keep complaining ad infinitum
        }
      }
    }
  }

  private void refreshQueueSizeGauge() {
    qsize.set(queue.size());
  }

  @Override
  public void consume(MetricsBuffer buffer) {
    long ts = 0;
    for (MetricsBuffer.Entry entry : buffer) {
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
      latency.add(Time.now() - ts);
    }
    if (buffer instanceof WaitableMetricsBuffer) {
      ((WaitableMetricsBuffer)buffer).notifyAnyWaiters();
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
    if (sink instanceof Closeable) {
      IOUtils.cleanup(LOG, (Closeable)sink);
    }
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

  static class WaitableMetricsBuffer extends MetricsBuffer {
    private final Semaphore notificationSemaphore =
        new Semaphore(0);

    public WaitableMetricsBuffer(MetricsBuffer metricsBuffer) {
      super(metricsBuffer);
    }

    public boolean waitTillNotified(long millisecondsToWait) {
      try {
        return notificationSemaphore.tryAcquire(millisecondsToWait,
            TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        return false;
      }
    }

    public void notifyAnyWaiters() {
      notificationSemaphore.release();
    }
  }
}
