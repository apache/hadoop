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
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.web.SLSWebApp;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.log4j.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingWindowReservoir;
import com.codahale.metrics.Timer;

@Private
@Unstable
public class ResourceSchedulerWrapper
    extends AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode>
    implements ResourceScheduler, Configurable {
  private static final String EOL = System.getProperty("line.separator");
  private static final int SAMPLING_SIZE = 60;
  private ScheduledExecutorService pool;
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

  private Configuration conf;
  private ResourceScheduler scheduler;
  private Map<ApplicationId, String> appQueueMap =
          new ConcurrentHashMap<ApplicationId, String>();
  private BufferedWriter jobRuntimeLogBW;

  // Priority of the ResourceSchedulerWrapper shutdown hook.
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  // web app
  private SLSWebApp web;

  private Map<ContainerId, Resource> preemptionContainerMap =
          new ConcurrentHashMap<ContainerId, Resource>();

  // metrics
  private MetricRegistry metrics;
  private SchedulerMetrics schedulerMetrics;
  private boolean metricsON;
  private String metricsOutputDir;
  private BufferedWriter metricsLogBW;
  private boolean running = false;
  private static Map<Class, Class> defaultSchedulerMetricsMap =
          new HashMap<Class, Class>();
  static {
    defaultSchedulerMetricsMap.put(FairScheduler.class,
            FairSchedulerMetrics.class);
    defaultSchedulerMetricsMap.put(FifoScheduler.class,
            FifoSchedulerMetrics.class);
    defaultSchedulerMetricsMap.put(CapacityScheduler.class,
            CapacitySchedulerMetrics.class);
  }
  // must set by outside
  private Set<String> queueSet;
  private Set<String> trackedAppSet;

  public final Logger LOG = Logger.getLogger(ResourceSchedulerWrapper.class);

  public ResourceSchedulerWrapper() {
    super(ResourceSchedulerWrapper.class.getName());
    samplerLock = new ReentrantLock();
    queueLock = new ReentrantLock();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    // set scheduler
    Class<? extends ResourceScheduler> klass =
            conf.getClass(SLSConfiguration.RM_SCHEDULER, null,
                    ResourceScheduler.class);

    scheduler = ReflectionUtils.newInstance(klass, conf);
    // start metrics
    metricsON = conf.getBoolean(SLSConfiguration.METRICS_SWITCH, true);
    if (metricsON) {
      try {
        initMetrics();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    ShutdownHookManager.get().addShutdownHook(new Runnable() {
      @Override
      public void run() {
        try {
          if (metricsLogBW != null)  {
            metricsLogBW.write("]");
            metricsLogBW.close();
          }
          if (web != null) {
            web.stop();
          }
          tearDown();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }, SHUTDOWN_HOOK_PRIORITY);
  }

  @Override
  public Allocation allocate(ApplicationAttemptId attemptId,
                             List<ResourceRequest> resourceRequests,
                             List<ContainerId> containerIds,
                             List<String> strings, List<String> strings2) {
    if (metricsON) {
      final Timer.Context context = schedulerAllocateTimer.time();
      Allocation allocation = null;
      try {
        allocation = scheduler.allocate(attemptId, resourceRequests,
                containerIds, strings, strings2);
        return allocation;
      } finally {
        context.stop();
        schedulerAllocateCounter.inc();
        try {
          updateQueueWithAllocateRequest(allocation, attemptId,
                  resourceRequests, containerIds);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    } else {
      return scheduler.allocate(attemptId,
              resourceRequests, containerIds, strings, strings2);
    }
  }

  @Override
  public void handle(SchedulerEvent schedulerEvent) {
    // metrics off
    if (! metricsON) {
      scheduler.handle(schedulerEvent);
      return;
    }
    if(!running)    running = true;

    // metrics on
    Timer.Context handlerTimer = null;
    Timer.Context operationTimer = null;

    NodeUpdateSchedulerEventWrapper eventWrapper;
    try {
      //if (schedulerEvent instanceof NodeUpdateSchedulerEvent) {
      if (schedulerEvent.getType() == SchedulerEventType.NODE_UPDATE
              && schedulerEvent instanceof NodeUpdateSchedulerEvent) {
        eventWrapper = new NodeUpdateSchedulerEventWrapper(
                (NodeUpdateSchedulerEvent)schedulerEvent);
        schedulerEvent = eventWrapper;
        updateQueueWithNodeUpdate(eventWrapper);
      } else if (schedulerEvent.getType() == SchedulerEventType.APP_ATTEMPT_REMOVED
          && schedulerEvent instanceof AppAttemptRemovedSchedulerEvent) {
        // check if having AM Container, update resource usage information
        AppAttemptRemovedSchedulerEvent appRemoveEvent =
            (AppAttemptRemovedSchedulerEvent) schedulerEvent;
        ApplicationAttemptId appAttemptId =
                appRemoveEvent.getApplicationAttemptID();
        String queue = appQueueMap.get(appAttemptId.getApplicationId());
        SchedulerAppReport app = scheduler.getSchedulerAppInfo(appAttemptId);
        if (! app.getLiveContainers().isEmpty()) {  // have 0 or 1
          // should have one container which is AM container
          RMContainer rmc = app.getLiveContainers().iterator().next();
          updateQueueMetrics(queue,
                  rmc.getContainer().getResource().getMemory(),
                  rmc.getContainer().getResource().getVirtualCores());
        }
      }

      handlerTimer = schedulerHandleTimer.time();
      operationTimer = schedulerHandleTimerMap
              .get(schedulerEvent.getType()).time();

      scheduler.handle(schedulerEvent);
    } finally {
      if (handlerTimer != null)     handlerTimer.stop();
      if (operationTimer != null)   operationTimer.stop();
      schedulerHandleCounter.inc();
      schedulerHandleCounterMap.get(schedulerEvent.getType()).inc();

      if (schedulerEvent.getType() == SchedulerEventType.APP_REMOVED
          && schedulerEvent instanceof AppRemovedSchedulerEvent) {
        SLSRunner.decreaseRemainingApps();
        AppRemovedSchedulerEvent appRemoveEvent =
                (AppRemovedSchedulerEvent) schedulerEvent;
        appQueueMap.remove(appRemoveEvent.getApplicationID());
      } else if (schedulerEvent.getType() == SchedulerEventType.APP_ADDED
          && schedulerEvent instanceof AppAddedSchedulerEvent) {
        AppAddedSchedulerEvent appAddEvent =
                (AppAddedSchedulerEvent) schedulerEvent;
        String queueName = appAddEvent.getQueue();
        appQueueMap.put(appAddEvent.getApplicationId(), queueName);
      }
    }
  }

  private void updateQueueWithNodeUpdate(
          NodeUpdateSchedulerEventWrapper eventWrapper) {
    RMNodeWrapper node = (RMNodeWrapper) eventWrapper.getRMNode();
    List<UpdatedContainerInfo> containerList = node.getContainerUpdates();
    for (UpdatedContainerInfo info : containerList) {
      for (ContainerStatus status : info.getCompletedContainers()) {
        ContainerId containerId = status.getContainerId();
        SchedulerAppReport app = scheduler.getSchedulerAppInfo(
                containerId.getApplicationAttemptId());

        if (app == null) {
          // this happens for the AM container
          // The app have already removed when the NM sends the release
          // information.
          continue;
        }

        String queue =
            appQueueMap.get(containerId.getApplicationAttemptId()
              .getApplicationId());
        int releasedMemory = 0, releasedVCores = 0;
        if (status.getExitStatus() == ContainerExitStatus.SUCCESS) {
          for (RMContainer rmc : app.getLiveContainers()) {
            if (rmc.getContainerId() == containerId) {
              releasedMemory += rmc.getContainer().getResource().getMemory();
              releasedVCores += rmc.getContainer()
                      .getResource().getVirtualCores();
              break;
            }
          }
        } else if (status.getExitStatus() == ContainerExitStatus.ABORTED) {
          if (preemptionContainerMap.containsKey(containerId)) {
            Resource preResource = preemptionContainerMap.get(containerId);
            releasedMemory += preResource.getMemory();
            releasedVCores += preResource.getVirtualCores();
            preemptionContainerMap.remove(containerId);
          }
        }
        // update queue counters
        updateQueueMetrics(queue, releasedMemory, releasedVCores);
      }
    }
  }

  private void updateQueueWithAllocateRequest(Allocation allocation,
                        ApplicationAttemptId attemptId,
                        List<ResourceRequest> resourceRequests,
                        List<ContainerId> containerIds) throws IOException {
    // update queue information
    Resource pendingResource = Resources.createResource(0, 0);
    Resource allocatedResource = Resources.createResource(0, 0);
    String queueName = appQueueMap.get(attemptId.getApplicationId());
    // container requested
    for (ResourceRequest request : resourceRequests) {
      if (request.getResourceName().equals(ResourceRequest.ANY)) {
        Resources.addTo(pendingResource,
                Resources.multiply(request.getCapability(),
                        request.getNumContainers()));
      }
    }
    // container allocated
    for (Container container : allocation.getContainers()) {
      Resources.addTo(allocatedResource, container.getResource());
      Resources.subtractFrom(pendingResource, container.getResource());
    }
    // container released from AM
    SchedulerAppReport report = scheduler.getSchedulerAppInfo(attemptId);
    for (ContainerId containerId : containerIds) {
      Container container = null;
      for (RMContainer c : report.getLiveContainers()) {
        if (c.getContainerId().equals(containerId)) {
          container = c.getContainer();
          break;
        }
      }
      if (container != null) {
        // released allocated containers
        Resources.subtractFrom(allocatedResource, container.getResource());
      } else {
        for (RMContainer c : report.getReservedContainers()) {
          if (c.getContainerId().equals(containerId)) {
            container = c.getContainer();
            break;
          }
        }
        if (container != null) {
          // released reserved containers
          Resources.subtractFrom(pendingResource, container.getResource());
        }
      }
    }
    // containers released/preemption from scheduler
    Set<ContainerId> preemptionContainers = new HashSet<ContainerId>();
    if (allocation.getContainerPreemptions() != null) {
      preemptionContainers.addAll(allocation.getContainerPreemptions());
    }
    if (allocation.getStrictContainerPreemptions() != null) {
      preemptionContainers.addAll(allocation.getStrictContainerPreemptions());
    }
    if (! preemptionContainers.isEmpty()) {
      for (ContainerId containerId : preemptionContainers) {
        if (! preemptionContainerMap.containsKey(containerId)) {
          Container container = null;
          for (RMContainer c : report.getLiveContainers()) {
            if (c.getContainerId().equals(containerId)) {
              container = c.getContainer();
              break;
            }
          }
          if (container != null) {
            preemptionContainerMap.put(containerId, container.getResource());
          }
        }

      }
    }

    // update metrics
    SortedMap<String, Counter> counterMap = metrics.getCounters();
    String names[] = new String[]{
            "counter.queue." + queueName + ".pending.memory",
            "counter.queue." + queueName + ".pending.cores",
            "counter.queue." + queueName + ".allocated.memory",
            "counter.queue." + queueName + ".allocated.cores"};
    int values[] = new int[]{pendingResource.getMemory(),
            pendingResource.getVirtualCores(),
            allocatedResource.getMemory(), allocatedResource.getVirtualCores()};
    for (int i = names.length - 1; i >= 0; i --) {
      if (! counterMap.containsKey(names[i])) {
        metrics.counter(names[i]);
        counterMap = metrics.getCounters();
      }
      counterMap.get(names[i]).inc(values[i]);
    }

    queueLock.lock();
    try {
      if (! schedulerMetrics.isTracked(queueName)) {
        schedulerMetrics.trackQueue(queueName);
      }
    } finally {
      queueLock.unlock();
    }
  }

  private void tearDown() throws IOException {
    // close job runtime writer
    if (jobRuntimeLogBW != null) {
      jobRuntimeLogBW.close();
    }
    // shut pool
    if (pool != null)  pool.shutdown();
  }

  @SuppressWarnings("unchecked")
  private void initMetrics() throws Exception {
    metrics = new MetricRegistry();
    // configuration
    metricsOutputDir = conf.get(SLSConfiguration.METRICS_OUTPUT_DIR);
    int metricsWebAddressPort = conf.getInt(
            SLSConfiguration.METRICS_WEB_ADDRESS_PORT,
            SLSConfiguration.METRICS_WEB_ADDRESS_PORT_DEFAULT);
    // create SchedulerMetrics for current scheduler
    String schedulerMetricsType = conf.get(scheduler.getClass().getName());
    Class schedulerMetricsClass = schedulerMetricsType == null?
            defaultSchedulerMetricsMap.get(scheduler.getClass()) :
            Class.forName(schedulerMetricsType);
    schedulerMetrics = (SchedulerMetrics)ReflectionUtils
            .newInstance(schedulerMetricsClass, new Configuration());
    schedulerMetrics.init(scheduler, metrics);

    // register various metrics
    registerJvmMetrics();
    registerClusterResourceMetrics();
    registerContainerAppNumMetrics();
    registerSchedulerMetrics();

    // .csv output
    initMetricsCSVOutput();

    // start web app to provide real-time tracking
    web = new SLSWebApp(this, metricsWebAddressPort);
    web.start();

    // a thread to update histogram timer
    pool = new ScheduledThreadPoolExecutor(2);
    pool.scheduleAtFixedRate(new HistogramsRunnable(), 0, 1000,
            TimeUnit.MILLISECONDS);

    // a thread to output metrics for real-tiem tracking
    pool.scheduleAtFixedRate(new MetricsLogRunnable(), 0, 1000,
            TimeUnit.MILLISECONDS);

    // application running information
    jobRuntimeLogBW = new BufferedWriter(
            new FileWriter(metricsOutputDir + "/jobruntime.csv"));
    jobRuntimeLogBW.write("JobID,real_start_time,real_end_time," +
            "simulate_start_time,simulate_end_time" + EOL);
    jobRuntimeLogBW.flush();
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
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          if(scheduler == null || scheduler.getRootQueueMetrics() == null) {
            return 0;
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
          if(scheduler == null || scheduler.getRootQueueMetrics() == null) {
            return 0;
          } else {
            return scheduler.getRootQueueMetrics().getAllocatedVirtualCores();
          }
        }
      }
    );
    metrics.register("variable.cluster.available.memory",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          if(scheduler == null || scheduler.getRootQueueMetrics() == null) {
            return 0;
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
          if(scheduler == null || scheduler.getRootQueueMetrics() == null) {
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
          if (scheduler == null || scheduler.getRootQueueMetrics() == null) {
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
          if(scheduler == null || scheduler.getRootQueueMetrics() == null) {
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
      schedulerHandleCounterMap = new HashMap<SchedulerEventType, Counter>();
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
      schedulerHandleTimerMap = new HashMap<SchedulerEventType, Timer>();
      for (SchedulerEventType e : SchedulerEventType.values()) {
        Timer timer = new Timer(new SlidingWindowReservoir(timeWindowSize));
        schedulerHandleTimerMap.put(e, timer);
      }
      // histogram for scheduler operations (Samplers)
      schedulerHistogramList = new ArrayList<Histogram>();
      histogramTimerMap = new HashMap<Histogram, Timer>();
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
    if(! dir.exists()
            && ! dir.mkdirs()) {
      LOG.error("Cannot create directory " + dir.getAbsoluteFile());
    }
    final CsvReporter reporter = CsvReporter.forRegistry(metrics)
            .formatFor(Locale.US)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build(new File(metricsOutputDir + "/metrics"));
    reporter.start(timeIntervalMS, TimeUnit.MILLISECONDS);
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
    public MetricsLogRunnable() {
      try {
        metricsLogBW = new BufferedWriter(
                new FileWriter(metricsOutputDir + "/realtimetrack.json"));
        metricsLogBW.write("[");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void run() {
      if(running) {
        // all WebApp to get real tracking json
        String metrics = web.generateRealTimeTrackingMetrics();
        // output
        try {
          if(firstLine) {
            metricsLogBW.write(metrics + EOL);
            firstLine = false;
          } else {
            metricsLogBW.write("," + metrics + EOL);
          }
          metricsLogBW.flush();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  // the following functions are used by AMSimulator
  public void addAMRuntime(ApplicationId appId,
                           long traceStartTimeMS, long traceEndTimeMS,
                           long simulateStartTimeMS, long simulateEndTimeMS) {
    if (metricsON) {
      try {
        // write job runtime information
        StringBuilder sb = new StringBuilder();
        sb.append(appId).append(",").append(traceStartTimeMS).append(",")
            .append(traceEndTimeMS).append(",").append(simulateStartTimeMS)
            .append(",").append(simulateEndTimeMS);
        jobRuntimeLogBW.write(sb.toString() + EOL);
        jobRuntimeLogBW.flush();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void updateQueueMetrics(String queue,
                                  int releasedMemory, int releasedVCores) {
    // update queue counters
    SortedMap<String, Counter> counterMap = metrics.getCounters();
    if (releasedMemory != 0) {
      String name = "counter.queue." + queue + ".allocated.memory";
      if (! counterMap.containsKey(name)) {
        metrics.counter(name);
        counterMap = metrics.getCounters();
      }
      counterMap.get(name).inc(-releasedMemory);
    }
    if (releasedVCores != 0) {
      String name = "counter.queue." + queue + ".allocated.cores";
      if (! counterMap.containsKey(name)) {
        metrics.counter(name);
        counterMap = metrics.getCounters();
      }
      counterMap.get(name).inc(-releasedVCores);
    }
  }

  public void setQueueSet(Set<String> queues) {
    this.queueSet = queues;
  }

  public Set<String> getQueueSet() {
    return this.queueSet;
  }

  public void setTrackedAppSet(Set<String> apps) {
    this.trackedAppSet = apps;
  }

  public Set<String> getTrackedAppSet() {
    return this.trackedAppSet;
  }

  public MetricRegistry getMetrics() {
    return metrics;
  }

  public SchedulerMetrics getSchedulerMetrics() {
    return schedulerMetrics;
  }

  // API open to out classes
  public void addTrackedApp(ApplicationAttemptId appAttemptId,
                            String oldAppId) {
    if (metricsON) {
      schedulerMetrics.trackApp(appAttemptId, oldAppId);
    }
  }

  public void removeTrackedApp(ApplicationAttemptId appAttemptId,
                               String oldAppId) {
    if (metricsON) {
      schedulerMetrics.untrackApp(appAttemptId, oldAppId);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void serviceInit(Configuration conf) throws Exception {
    ((AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode>)
        scheduler).init(conf);
    super.serviceInit(conf);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void serviceStart() throws Exception {
    ((AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode>)
        scheduler).start();
    super.serviceStart();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void serviceStop() throws Exception {
    ((AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode>)
        scheduler).stop();
    super.serviceStop();
  }

  @Override
  public void setRMContext(RMContext rmContext) {
    scheduler.setRMContext(rmContext);
  }

  @Override
  public void reinitialize(Configuration conf, RMContext rmContext)
      throws IOException {
    scheduler.reinitialize(conf, rmContext);
  }

  @Override
  public void recover(RMStateStore.RMState rmState) throws Exception {
    scheduler.recover(rmState);
  }

  @Override
  public QueueInfo getQueueInfo(String s, boolean b, boolean b2)
          throws IOException {
    return scheduler.getQueueInfo(s, b, b2);
  }

  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo() {
    return scheduler.getQueueUserAclInfo();
  }

  @Override
  public Resource getMinimumResourceCapability() {
    return scheduler.getMinimumResourceCapability();
  }

  @Override
  public Resource getMaximumResourceCapability() {
    return scheduler.getMaximumResourceCapability();
  }

  @Override
  public int getNumClusterNodes() {
    return scheduler.getNumClusterNodes();
  }

  @Override
  public SchedulerNodeReport getNodeReport(NodeId nodeId) {
    return scheduler.getNodeReport(nodeId);
  }

  @Override
  public SchedulerAppReport getSchedulerAppInfo(
          ApplicationAttemptId attemptId) {
    return scheduler.getSchedulerAppInfo(attemptId);
  }

  @Override
  public QueueMetrics getRootQueueMetrics() {
    return scheduler.getRootQueueMetrics();
  }

  @Override
  public synchronized boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, String queueName) {
    return scheduler.checkAccess(callerUGI, acl, queueName);
  }

  @Override
  public ApplicationResourceUsageReport getAppResourceUsageReport(
      ApplicationAttemptId appAttemptId) {
    return scheduler.getAppResourceUsageReport(appAttemptId);
  }

  @Override
  public List<ApplicationAttemptId> getAppsInQueue(String queue) {
    return scheduler.getAppsInQueue(queue);
  }

  @Override
  public RMContainer getRMContainer(ContainerId containerId) {
    return null;
  }

  @Override
  public String moveApplication(ApplicationId appId, String newQueue)
      throws YarnException {
    return scheduler.moveApplication(appId, newQueue);
  }

  @Override
  @LimitedPrivate("yarn")
  @Unstable
  public Resource getClusterResource() {
    return null;
  }

  @Override
  public synchronized List<Container> getTransferredContainers(
      ApplicationAttemptId currentAttempt) {
    return new ArrayList<Container>();
  }

  @Override
  public Map<ApplicationId, SchedulerApplication<SchedulerApplicationAttempt>>
      getSchedulerApplications() {
    return new HashMap<ApplicationId,
        SchedulerApplication<SchedulerApplicationAttempt>>();
  }

  @Override
  protected void completedContainer(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) {
    // do nothing
  }
}

