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

package org.apache.hadoop.yarn.sls.scheduler;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.Locale;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Lock;

import com.codahale.metrics.Counter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingWindowReservoir;
import com.codahale.metrics.Timer;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.web.SLSWebApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Private
@Unstable
public abstract class SchedulerMetrics {
  private static final String EOL = System.getProperty("line.separator");
  private static final int SAMPLING_SIZE = 60;
  private static final Logger LOG =
      LoggerFactory.getLogger(SchedulerMetrics.class);

  protected ResourceScheduler scheduler;
  protected Set<String> trackedQueues;
  protected MetricRegistry metrics;
  protected Set<String> appTrackedMetrics;
  protected Set<String> queueTrackedMetrics;

  private Configuration conf;
  private ScheduledExecutorService pool;
  private SLSWebApp web;

  // metrics
  private String metricsOutputDir;
  private BufferedWriter metricsLogBW;
  private BufferedWriter jobRuntimeLogBW;
  private boolean running = false;

  // counters for scheduler allocate/handle operations
  private Counter schedulerAllocateCounter;
  private Counter schedulerHandleCounter;
  private Map<SchedulerEventType, Counter> schedulerHandleCounterMap;

  // Timers for scheduler allocate/handle operations
  private Timer schedulerAllocateTimer;
  private Timer schedulerHandleTimer;
  private Map<SchedulerEventType, Timer> schedulerHandleTimerMap;
  private List<Histogram> schedulerHistogramList;
  private Map<Histogram, Timer> histogramTimerMap;
  private Lock samplerLock;
  private Lock queueLock;

  static Class getSchedulerMetricsClass(Configuration conf,
      Class schedulerClass) throws ClassNotFoundException {
    Class metricClass = null;
    String schedulerMetricsType = conf.get(schedulerClass.getName());
    if (schedulerMetricsType != null) {
      metricClass = Class.forName(schedulerMetricsType);
    }

    if (schedulerClass.equals(FairScheduler.class)) {
      metricClass = FairSchedulerMetrics.class;
    } else if (schedulerClass.equals(CapacityScheduler.class)) {
      metricClass = CapacitySchedulerMetrics.class;
    } else if (schedulerClass.equals(FifoScheduler.class)) {
      metricClass = FifoSchedulerMetrics.class;
    }

    return metricClass;
  }

  static SchedulerMetrics getInstance(Configuration conf, Class schedulerClass)
      throws ClassNotFoundException {
    Class schedulerMetricClass = getSchedulerMetricsClass(conf, schedulerClass);
    return (SchedulerMetrics) ReflectionUtils
        .newInstance(schedulerMetricClass, new Configuration());
  }

  public SchedulerMetrics() {
    metrics = new MetricRegistry();

    appTrackedMetrics = new HashSet<>();
    appTrackedMetrics.add("live.containers");
    appTrackedMetrics.add("reserved.containers");

    queueTrackedMetrics = new HashSet<>();
    trackedQueues = new HashSet<>();

    samplerLock = new ReentrantLock();
    queueLock = new ReentrantLock();
  }

  void init(ResourceScheduler resourceScheduler, Configuration config)
      throws Exception {
    this.scheduler = resourceScheduler;
    this.conf = config;

    metricsOutputDir = conf.get(SLSConfiguration.METRICS_OUTPUT_DIR);

    // register various metrics
    registerJvmMetrics();
    registerClusterResourceMetrics();
    registerContainerAppNumMetrics();
    registerSchedulerMetrics();

    // .csv output
    initMetricsCSVOutput();

    // start web app to provide real-time tracking
    int metricsWebAddressPort = conf.getInt(
        SLSConfiguration.METRICS_WEB_ADDRESS_PORT,
        SLSConfiguration.METRICS_WEB_ADDRESS_PORT_DEFAULT);
    web = new SLSWebApp((SchedulerWrapper)scheduler, metricsWebAddressPort);
    web.start();

    // a thread to update histogram timer
    pool = new ScheduledThreadPoolExecutor(2);
    pool.scheduleAtFixedRate(new HistogramsRunnable(), 0, 1000,
        TimeUnit.MILLISECONDS);

    // a thread to output metrics for real-tiem tracking
    pool.scheduleAtFixedRate(new MetricsLogRunnable(), 0, 1000,
        TimeUnit.MILLISECONDS);

    // application running information
    jobRuntimeLogBW =
        new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
            metricsOutputDir + "/jobruntime.csv"), "UTF-8"));
    jobRuntimeLogBW.write("JobID,real_start_time,real_end_time," +
        "simulate_start_time,simulate_end_time" + EOL);
    jobRuntimeLogBW.flush();
  }

  public MetricRegistry getMetrics() {
    return metrics;
  }

  protected SchedulerApplicationAttempt getSchedulerAppAttempt(
      ApplicationId appId) {
    AbstractYarnScheduler yarnScheduler = (AbstractYarnScheduler)scheduler;
    SchedulerApplication app = (SchedulerApplication)yarnScheduler
        .getSchedulerApplications().get(appId);
    if (app == null) {
      return null;
    }
    return app.getCurrentAppAttempt();
  }

  public void trackApp(final ApplicationId appId, String oldAppId) {
    metrics.register("variable.app." + oldAppId + ".live.containers",
        new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            SchedulerApplicationAttempt appAttempt =
                getSchedulerAppAttempt(appId);
            if (appAttempt != null) {
              return appAttempt.getLiveContainers().size();
            } else {
              return 0;
            }
          }
        }
    );
    metrics.register("variable.app." + oldAppId + ".reserved.containers",
        new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            SchedulerApplicationAttempt appAttempt =
                getSchedulerAppAttempt(appId);
            if (appAttempt != null) {
              return appAttempt.getReservedContainers().size();
            } else {
              return 0;
            }
          }
        }
    );
  }

  public void untrackApp(String oldAppId) {
    for (String m : appTrackedMetrics) {
      metrics.remove("variable.app." + oldAppId + "." + m);
    }
  }

  /**
   * Track a queue by registering its metrics.
   *
   * @param queue queue name
   */
  public void trackQueue(String queue) {
    queueLock.lock();
    try {
      if (!isTracked(queue)) {
        trackedQueues.add(queue);
        registerQueueMetrics(queue);
      }
    } finally {
      queueLock.unlock();
    }
  }

  protected void registerQueueMetrics(String queueName) {
    SortedMap<String, Counter> counterMap = metrics.getCounters();

    for (QueueMetric queueMetric : QueueMetric.values()) {
      String metricName = getQueueMetricName(queueName, queueMetric);
      if (!counterMap.containsKey(metricName)) {
        metrics.counter(metricName);
        queueTrackedMetrics.add(metricName);
      }
    }
  }

  public boolean isTracked(String queueName) {
    return trackedQueues.contains(queueName);
  }
  
  public Set<String> getAppTrackedMetrics() {
    return appTrackedMetrics;
  }

  public Set<String> getQueueTrackedMetrics() {
    return queueTrackedMetrics;
  }

  private void registerJvmMetrics() {
    // add JVM gauges
    metrics.register("variable.jvm.free.memory",
        new Gauge<Long>() {
          @Override
          public Long getValue() {
            return Runtime.getRuntime().freeMemory();
          }
        }
    );
    metrics.register("variable.jvm.max.memory",
        new Gauge<Long>() {
          @Override
          public Long getValue() {
            return Runtime.getRuntime().maxMemory();
          }
        }
    );
    metrics.register("variable.jvm.total.memory",
        new Gauge<Long>() {
          @Override
          public Long getValue() {
            return Runtime.getRuntime().totalMemory();
          }
        }
    );
  }

  private void registerClusterResourceMetrics() {
    metrics.register("variable.cluster.allocated.memory",
        new Gauge<Long>() {
          @Override
          public Long getValue() {
            if (scheduler.getRootQueueMetrics() == null) {
              return 0L;
            } else {
              return scheduler.getRootQueueMetrics().getAllocatedMB();
            }
          }
        }
    );
    metrics.register("variable.cluster.allocated.vcores",
        new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            if (scheduler.getRootQueueMetrics() == null) {
              return 0;
            } else {
              return scheduler.getRootQueueMetrics().getAllocatedVirtualCores();
            }
          }
        }
    );
    metrics.register("variable.cluster.available.memory",
        new Gauge<Long>() {
          @Override
          public Long getValue() {
            if (scheduler.getRootQueueMetrics() == null) {
              return 0L;
            } else {
              return scheduler.getRootQueueMetrics().getAvailableMB();
            }
          }
        }
    );
    metrics.register("variable.cluster.available.vcores",
        new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            if (scheduler.getRootQueueMetrics() == null) {
              return 0;
            } else {
              return scheduler.getRootQueueMetrics().getAvailableVirtualCores();
            }
          }
        }
    );
  }

  private void registerContainerAppNumMetrics() {
    metrics.register("variable.running.application",
        new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            if (scheduler.getRootQueueMetrics() == null) {
              return 0;
            } else {
              return scheduler.getRootQueueMetrics().getAppsRunning();
            }
          }
        }
    );
    metrics.register("variable.running.container",
        new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            if (scheduler.getRootQueueMetrics() == null) {
              return 0;
            } else {
              return scheduler.getRootQueueMetrics().getAllocatedContainers();
            }
          }
        }
    );
  }

  private void registerSchedulerMetrics() {
    samplerLock.lock();
    try {
      // counters for scheduler operations
      schedulerAllocateCounter = metrics.counter(
          "counter.scheduler.operation.allocate");
      schedulerHandleCounter = metrics.counter(
          "counter.scheduler.operation.handle");
      schedulerHandleCounterMap = new HashMap<>();
      for (SchedulerEventType e : SchedulerEventType.values()) {
        Counter counter = metrics.counter(
            "counter.scheduler.operation.handle." + e);
        schedulerHandleCounterMap.put(e, counter);
      }
      // timers for scheduler operations
      int timeWindowSize = conf.getInt(
          SLSConfiguration.METRICS_TIMER_WINDOW_SIZE,
          SLSConfiguration.METRICS_TIMER_WINDOW_SIZE_DEFAULT);
      schedulerAllocateTimer = new Timer(
          new SlidingWindowReservoir(timeWindowSize));
      schedulerHandleTimer = new Timer(
          new SlidingWindowReservoir(timeWindowSize));
      schedulerHandleTimerMap = new HashMap<>();
      for (SchedulerEventType e : SchedulerEventType.values()) {
        Timer timer = new Timer(new SlidingWindowReservoir(timeWindowSize));
        schedulerHandleTimerMap.put(e, timer);
      }
      // histogram for scheduler operations (Samplers)
      schedulerHistogramList = new ArrayList<>();
      histogramTimerMap = new HashMap<>();
      Histogram schedulerAllocateHistogram = new Histogram(
          new SlidingWindowReservoir(SAMPLING_SIZE));
      metrics.register("sampler.scheduler.operation.allocate.timecost",
          schedulerAllocateHistogram);
      schedulerHistogramList.add(schedulerAllocateHistogram);
      histogramTimerMap.put(schedulerAllocateHistogram, schedulerAllocateTimer);
      Histogram schedulerHandleHistogram = new Histogram(
          new SlidingWindowReservoir(SAMPLING_SIZE));
      metrics.register("sampler.scheduler.operation.handle.timecost",
          schedulerHandleHistogram);
      schedulerHistogramList.add(schedulerHandleHistogram);
      histogramTimerMap.put(schedulerHandleHistogram, schedulerHandleTimer);
      for (SchedulerEventType e : SchedulerEventType.values()) {
        Histogram histogram = new Histogram(
            new SlidingWindowReservoir(SAMPLING_SIZE));
        metrics.register(
            "sampler.scheduler.operation.handle." + e + ".timecost",
            histogram);
        schedulerHistogramList.add(histogram);
        histogramTimerMap.put(histogram, schedulerHandleTimerMap.get(e));
      }
    } finally {
      samplerLock.unlock();
    }
  }

  private void initMetricsCSVOutput() {
    int timeIntervalMS = conf.getInt(
        SLSConfiguration.METRICS_RECORD_INTERVAL_MS,
        SLSConfiguration.METRICS_RECORD_INTERVAL_MS_DEFAULT);
    File dir = new File(metricsOutputDir + "/metrics");
    if(!dir.exists() && !dir.mkdirs()) {
      LOG.error("Cannot create directory {}", dir.getAbsoluteFile());
    }
    final CsvReporter reporter = CsvReporter.forRegistry(metrics)
        .formatFor(Locale.US)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build(new File(metricsOutputDir + "/metrics"));
    reporter.start(timeIntervalMS, TimeUnit.MILLISECONDS);
  }

  boolean isRunning() {
    return running;
  }

  void setRunning(boolean running) {
    this.running = running;
  }

  class HistogramsRunnable implements Runnable {
    @Override
    public void run() {
      samplerLock.lock();
      try {
        for (Histogram histogram : schedulerHistogramList) {
          Timer timer = histogramTimerMap.get(histogram);
          histogram.update((int) timer.getSnapshot().getMean());
        }
      } finally {
        samplerLock.unlock();
      }
    }
  }

  class MetricsLogRunnable implements Runnable {
    private boolean firstLine = true;

    MetricsLogRunnable() {
      try {
        metricsLogBW =
            new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
                metricsOutputDir + "/realtimetrack.json"), "UTF-8"));
        metricsLogBW.write("[");
      } catch (IOException e) {
        LOG.info(e.getMessage());
      }
    }

    @Override
    public void run() {
      if(running) {
        // all WebApp to get real tracking json
        String trackingMetrics = web.generateRealTimeTrackingMetrics();
        // output
        try {
          if(firstLine) {
            metricsLogBW.write(trackingMetrics + EOL);
            firstLine = false;
          } else {
            metricsLogBW.write("," + trackingMetrics + EOL);
          }
          metricsLogBW.flush();
        } catch (IOException e) {
          LOG.info(e.getMessage());
        }
      }
    }
  }

  void tearDown() throws Exception {
    if (metricsLogBW != null)  {
      metricsLogBW.write("]");
      metricsLogBW.close();
    }

    if (web != null) {
      web.stop();
    }

    if (jobRuntimeLogBW != null) {
      jobRuntimeLogBW.close();
    }

    if (pool != null) {
      pool.shutdown();
    }
  }

  void increaseSchedulerAllocationCounter() {
    schedulerAllocateCounter.inc();
  }

  void increaseSchedulerHandleCounter(SchedulerEventType schedulerEventType) {
    schedulerHandleCounter.inc();
    schedulerHandleCounterMap.get(schedulerEventType).inc();
  }

  Timer getSchedulerAllocateTimer() {
    return schedulerAllocateTimer;
  }

  Timer getSchedulerHandleTimer() {
    return schedulerHandleTimer;
  }

  Timer getSchedulerHandleTimer(SchedulerEventType schedulerEventType) {
    return schedulerHandleTimerMap.get(schedulerEventType);
  }

  private enum QueueMetric {
    PENDING_MEMORY("pending.memory"),
    PENDING_VCORES("pending.cores"),
    ALLOCATED_MEMORY("allocated.memory"),
    ALLOCATED_VCORES("allocated.cores");

    private String value;

    QueueMetric(String value) {
      this.value = value;
    }
  }

  private String getQueueMetricName(String queue, QueueMetric metric) {
    return "counter.queue." + queue + "." + metric.value;
  }

  void updateQueueMetrics(Resource pendingResource, Resource allocatedResource,
      String queueName) {
    trackQueue(queueName);

    SortedMap<String, Counter> counterMap = metrics.getCounters();
    for(QueueMetric metric : QueueMetric.values()) {
      String metricName = getQueueMetricName(queueName, metric);

      if (metric == QueueMetric.PENDING_MEMORY) {
        counterMap.get(metricName).inc(pendingResource.getMemorySize());
      } else if (metric == QueueMetric.PENDING_VCORES) {
        counterMap.get(metricName).inc(pendingResource.getVirtualCores());
      } else if (metric == QueueMetric.ALLOCATED_MEMORY) {
        counterMap.get(metricName).inc(allocatedResource.getMemorySize());
      } else if (metric == QueueMetric.ALLOCATED_VCORES){
        counterMap.get(metricName).inc(allocatedResource.getVirtualCores());
      }
    }
  }

  void updateQueueMetricsByRelease(Resource releaseResource, String queue) {
    SortedMap<String, Counter> counterMap = metrics.getCounters();
    String name = getQueueMetricName(queue, QueueMetric.ALLOCATED_MEMORY);
    if (!counterMap.containsKey(name)) {
      metrics.counter(name);
      counterMap = metrics.getCounters();
    }
    counterMap.get(name).inc(-releaseResource.getMemorySize());

    String vcoreMetric =
        getQueueMetricName(queue, QueueMetric.ALLOCATED_VCORES);
    if (!counterMap.containsKey(vcoreMetric)) {
      metrics.counter(vcoreMetric);
      counterMap = metrics.getCounters();
    }
    counterMap.get(vcoreMetric).inc(-releaseResource.getVirtualCores());
  }

  public void addTrackedApp(ApplicationId appId,
      String oldAppId) {
    trackApp(appId, oldAppId);
  }

  public void removeTrackedApp(String oldAppId) {
    untrackApp(oldAppId);
  }

  public void addAMRuntime(ApplicationId appId, long traceStartTimeMS,
      long traceEndTimeMS, long simulateStartTimeMS, long simulateEndTimeMS) {
    try {
      // write job runtime information
      StringBuilder sb = new StringBuilder();
      sb.append(appId).append(",").append(traceStartTimeMS).append(",")
          .append(traceEndTimeMS).append(",").append(simulateStartTimeMS)
          .append(",").append(simulateEndTimeMS);
      jobRuntimeLogBW.write(sb.toString() + EOL);
      jobRuntimeLogBW.flush();
    } catch (IOException e) {
      LOG.info(e.getMessage());
    }
  }
}
