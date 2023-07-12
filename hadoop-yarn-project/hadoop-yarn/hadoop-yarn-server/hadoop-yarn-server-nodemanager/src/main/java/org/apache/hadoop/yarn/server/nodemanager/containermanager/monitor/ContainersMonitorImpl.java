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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupElasticMemoryController;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.timelineservice.NMTimelinePublisher;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerHardwareUtils;
import org.apache.hadoop.yarn.server.nodemanager.webapp.ContainerLogsUtils;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;

import java.util.Arrays;
import java.io.File;
import java.util.Map;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Monitors containers collecting resource usage and preempting the container
 * if it exceeds its limits.
 */
public class ContainersMonitorImpl extends AbstractService implements
    ContainersMonitor {

  private final static Logger LOG =
       LoggerFactory.getLogger(ContainersMonitorImpl.class);
  private final static Logger AUDITLOG =
       LoggerFactory.getLogger(ContainersMonitorImpl.class.getName()+".audit");

  private long monitoringInterval;
  private MonitoringThread monitoringThread;
  private int logCheckInterval;
  private LogMonitorThread logMonitorThread;
  private long logDirSizeLimit;
  private long logTotalSizeLimit;
  private CGroupElasticMemoryController oomListenerThread;
  private boolean containerMetricsEnabled;
  private long containerMetricsPeriodMs;
  private long containerMetricsUnregisterDelayMs;

  @VisibleForTesting
  final Map<ContainerId, ProcessTreeInfo> trackingContainers =
      new ConcurrentHashMap<>();

  private final ContainerExecutor containerExecutor;
  private final Dispatcher eventDispatcher;
  private final Context context;
  private ResourceCalculatorPlugin resourceCalculatorPlugin;
  private Configuration conf;
  private static float vmemRatio;
  private Class<? extends ResourceCalculatorProcessTree> processTreeClass;

  /** Maximum virtual memory in bytes. */
  private long maxVmemAllottedForContainers = UNKNOWN_MEMORY_LIMIT;
  /** Maximum physical memory in bytes. */
  private long maxPmemAllottedForContainers = UNKNOWN_MEMORY_LIMIT;

  private boolean pmemCheckEnabled;
  private boolean vmemCheckEnabled;
  private boolean elasticMemoryEnforcement;
  private boolean strictMemoryEnforcement;
  private boolean containersMonitorEnabled;
  private boolean logMonitorEnabled;

  private long maxVCoresAllottedForContainers;

  private static final long UNKNOWN_MEMORY_LIMIT = -1L;
  private int nodeCpuPercentageForYARN;

  /**
   * Type of container metric.
   */
  @Private
  public enum ContainerMetric {
    CPU, MEMORY
  }

  private ResourceUtilization containersUtilization;

  private volatile boolean stopped = false;

  public ContainersMonitorImpl(ContainerExecutor exec,
      AsyncDispatcher dispatcher, Context context) {
    super("containers-monitor");

    this.containerExecutor = exec;
    this.eventDispatcher = dispatcher;
    this.context = context;

    this.monitoringThread = new MonitoringThread();

    this.logMonitorThread = new LogMonitorThread();

    this.containersUtilization = ResourceUtilization.newInstance(0, 0, 0.0f);
  }

  @Override
  protected void serviceInit(Configuration myConf) throws Exception {
    this.conf = myConf;
    this.monitoringInterval =
        this.conf.getLong(YarnConfiguration.NM_CONTAINER_MON_INTERVAL_MS,
            this.conf.getLong(YarnConfiguration.NM_RESOURCE_MON_INTERVAL_MS,
                YarnConfiguration.DEFAULT_NM_RESOURCE_MON_INTERVAL_MS));

    this.logCheckInterval =
        conf.getInt(YarnConfiguration.NM_CONTAINER_LOG_MON_INTERVAL_MS,
            YarnConfiguration.DEFAULT_NM_CONTAINER_LOG_MON_INTERVAL_MS);
    this.logDirSizeLimit =
        conf.getLong(YarnConfiguration.NM_CONTAINER_LOG_DIR_SIZE_LIMIT_BYTES,
            YarnConfiguration.DEFAULT_NM_CONTAINER_LOG_DIR_SIZE_LIMIT_BYTES);
    this.logTotalSizeLimit =
        conf.getLong(YarnConfiguration.NM_CONTAINER_LOG_TOTAL_SIZE_LIMIT_BYTES,
            YarnConfiguration.DEFAULT_NM_CONTAINER_LOG_TOTAL_SIZE_LIMIT_BYTES);

    this.resourceCalculatorPlugin =
        ResourceCalculatorPlugin.getContainersMonitorPlugin(this.conf);
    LOG.info("Using ResourceCalculatorPlugin: {}",
        this.resourceCalculatorPlugin);
    processTreeClass = this.conf.getClass(
            YarnConfiguration.NM_CONTAINER_MON_PROCESS_TREE, null,
            ResourceCalculatorProcessTree.class);
    LOG.info("Using ResourceCalculatorProcessTree: {}", this.processTreeClass);

    this.containerMetricsEnabled =
        this.conf.getBoolean(YarnConfiguration.NM_CONTAINER_METRICS_ENABLE,
            YarnConfiguration.DEFAULT_NM_CONTAINER_METRICS_ENABLE);
    this.containerMetricsPeriodMs =
        this.conf.getLong(YarnConfiguration.NM_CONTAINER_METRICS_PERIOD_MS,
            YarnConfiguration.DEFAULT_NM_CONTAINER_METRICS_PERIOD_MS);
    this.containerMetricsUnregisterDelayMs = this.conf.getLong(
        YarnConfiguration.NM_CONTAINER_METRICS_UNREGISTER_DELAY_MS,
        YarnConfiguration.DEFAULT_NM_CONTAINER_METRICS_UNREGISTER_DELAY_MS);

    long configuredPMemForContainers =
        NodeManagerHardwareUtils.getContainerMemoryMB(
            this.resourceCalculatorPlugin, this.conf);

    int configuredVCoresForContainers =
        NodeManagerHardwareUtils.getVCores(
            this.resourceCalculatorPlugin, this.conf);

    // ///////// Virtual memory configuration //////
    vmemRatio = this.conf.getFloat(YarnConfiguration.NM_VMEM_PMEM_RATIO,
        YarnConfiguration.DEFAULT_NM_VMEM_PMEM_RATIO);
    Preconditions.checkArgument(vmemRatio > 0.99f,
        YarnConfiguration.NM_VMEM_PMEM_RATIO + " should be at least 1.0");

    // Setting these irrespective of whether checks are enabled.
    // Required in the UI.
    Resource resourcesForContainers = Resource.newInstance(
        configuredPMemForContainers, configuredVCoresForContainers);
    setAllocatedResourcesForContainers(resourcesForContainers);

    pmemCheckEnabled = this.conf.getBoolean(
        YarnConfiguration.NM_PMEM_CHECK_ENABLED,
        YarnConfiguration.DEFAULT_NM_PMEM_CHECK_ENABLED);
    vmemCheckEnabled = this.conf.getBoolean(
        YarnConfiguration.NM_VMEM_CHECK_ENABLED,
        YarnConfiguration.DEFAULT_NM_VMEM_CHECK_ENABLED);
    elasticMemoryEnforcement = this.conf.getBoolean(
        YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_ENABLED,
        YarnConfiguration.DEFAULT_NM_ELASTIC_MEMORY_CONTROL_ENABLED);
    strictMemoryEnforcement = conf.getBoolean(
        YarnConfiguration.NM_MEMORY_RESOURCE_ENFORCED,
        YarnConfiguration.DEFAULT_NM_MEMORY_RESOURCE_ENFORCED);
    LOG.info("Physical memory check enabled: {}", pmemCheckEnabled);
    LOG.info("Virtual memory check enabled: {}", vmemCheckEnabled);
    LOG.info("Elastic memory control enabled: {}", elasticMemoryEnforcement);
    LOG.info("Strict memory control enabled: {}", strictMemoryEnforcement);

    if (elasticMemoryEnforcement) {
      if (!CGroupElasticMemoryController.isAvailable()) {
        // Test for availability outside the constructor
        // to be able to write non-Linux unit tests for
        // CGroupElasticMemoryController
        throw new YarnException(
            "CGroup Elastic Memory controller enabled but " +
            "it is not available. Exiting.");
      } else {
        this.oomListenerThread = new CGroupElasticMemoryController(
            conf,
            context,
            ResourceHandlerModule.getCGroupsHandler(),
            pmemCheckEnabled,
            vmemCheckEnabled,
            pmemCheckEnabled ?
                maxPmemAllottedForContainers : maxVmemAllottedForContainers
        );
      }
    }

    containersMonitorEnabled =
        isContainerMonitorEnabled() && monitoringInterval > 0;
    LOG.info("ContainersMonitor enabled: {}", containersMonitorEnabled);

    logMonitorEnabled =
            conf.getBoolean(YarnConfiguration.NM_CONTAINER_LOG_MONITOR_ENABLED,
                    YarnConfiguration.DEFAULT_NM_CONTAINER_LOG_MONITOR_ENABLED);
    LOG.info("Container Log Monitor Enabled: "+ logMonitorEnabled);

    nodeCpuPercentageForYARN =
        NodeManagerHardwareUtils.getNodeCpuPercentage(this.conf);

    if (pmemCheckEnabled) {
      // Logging if actual pmem cannot be determined.
      long totalPhysicalMemoryOnNM = UNKNOWN_MEMORY_LIMIT;
      if (this.resourceCalculatorPlugin != null) {
        totalPhysicalMemoryOnNM = this.resourceCalculatorPlugin
            .getPhysicalMemorySize();
        if (totalPhysicalMemoryOnNM <= 0) {
          LOG.warn("NodeManager's totalPmem could not be calculated. "
              + "Setting it to {}", UNKNOWN_MEMORY_LIMIT);
          totalPhysicalMemoryOnNM = UNKNOWN_MEMORY_LIMIT;
        }
      }

      if (totalPhysicalMemoryOnNM != UNKNOWN_MEMORY_LIMIT &&
          this.maxPmemAllottedForContainers > totalPhysicalMemoryOnNM * 0.80f) {
        LOG.warn(
            "NodeManager configured with {} physical memory allocated to " +
            "containers, which is more than 80% of the total physical memory " +
            "available ({}). Thrashing might happen.",
            TraditionalBinaryPrefix.long2String(
                maxPmemAllottedForContainers, "B", 1),
            TraditionalBinaryPrefix.long2String(
                totalPhysicalMemoryOnNM, "B", 1));
      }
    }
    super.serviceInit(this.conf);
  }

  private boolean isContainerMonitorEnabled() {
    return conf.getBoolean(YarnConfiguration.NM_CONTAINER_MONITOR_ENABLED,
        YarnConfiguration.DEFAULT_NM_CONTAINER_MONITOR_ENABLED);
  }

  /**
   * Get the best process tree calculator.
   * @param pId container process id
   * @return process tree calculator
   */
  private ResourceCalculatorProcessTree
      getResourceCalculatorProcessTree(String pId) {
    return ResourceCalculatorProcessTree.
        getResourceCalculatorProcessTree(
            pId, processTreeClass, conf);
  }

  private boolean isResourceCalculatorAvailable() {
    if (resourceCalculatorPlugin == null) {
      LOG.info("ResourceCalculatorPlugin is unavailable on this system. "
          + "{} is disabled.", this.getClass().getName());
      return false;
    }
    if (getResourceCalculatorProcessTree("0") == null) {
      LOG.info("ResourceCalculatorProcessTree is unavailable on this system. "
          + "{} is disabled.", this.getClass().getName());
      return false;
    }
    return true;
  }

  @Override
  protected void serviceStart() throws Exception {
    if (containersMonitorEnabled) {
      this.monitoringThread.start();
    }
    if (oomListenerThread != null) {
      oomListenerThread.start();
    }
    if (logMonitorEnabled) {
      this.logMonitorThread.start();
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
    if (containersMonitorEnabled) {
      this.monitoringThread.interrupt();
      try {
        this.monitoringThread.join();
      } catch (InterruptedException e) {
        LOG.info("ContainersMonitorImpl monitoring thread interrupted");
      }
      if (this.oomListenerThread != null) {
        this.oomListenerThread.stopListening();
        try {
          this.oomListenerThread.join();
        } finally {
          this.oomListenerThread = null;
        }
      }
    }
    if (logMonitorEnabled) {
      this.logMonitorThread.interrupt();
      try {
        this.logMonitorThread.join();
      } catch (InterruptedException e) {
      }
    }
    super.serviceStop();
  }

  /**
   * Encapsulates resource requirements of a process and its tree.
   */
  public static class ProcessTreeInfo {
    private ContainerId containerId;
    private String pid;
    private ResourceCalculatorProcessTree pTree;
    private long vmemLimit;
    private long pmemLimit;
    private int cpuVcores;

    public ProcessTreeInfo(ContainerId containerId, String pid,
        ResourceCalculatorProcessTree pTree, long vmemLimit, long pmemLimit,
        int cpuVcores) {
      this.containerId = containerId;
      this.pid = pid;
      this.pTree = pTree;
      this.vmemLimit = vmemLimit;
      this.pmemLimit = pmemLimit;
      this.cpuVcores = cpuVcores;
    }

    public ContainerId getContainerId() {
      return this.containerId;
    }

    public String getPID() {
      return this.pid;
    }

    public void setPid(String pid) {
      this.pid = pid;
    }

    ResourceCalculatorProcessTree getProcessTree() {
      return this.pTree;
    }

    void setProcessTree(ResourceCalculatorProcessTree mypTree) {
      this.pTree = mypTree;
    }

    /**
     * @return Virtual memory limit for the process tree in bytes
     */
    public synchronized long getVmemLimit() {
      return this.vmemLimit;
    }

    /**
     * @return Physical memory limit for the process tree in bytes
     */
    public synchronized long getPmemLimit() {
      return this.pmemLimit;
    }

    /**
     * @return Number of cpu vcores assigned
     */
    public synchronized int getCpuVcores() {
      return this.cpuVcores;
    }

    /**
     * Set resource limit for enforcement.
     * @param myPmemLimit
     *          Physical memory limit for the process tree in bytes
     * @param myVmemLimit
     *          Virtual memory limit for the process tree in bytes
     * @param myCpuVcores
     *          Number of cpu vcores assigned
     */
    synchronized void setResourceLimit(
        long myPmemLimit, long myVmemLimit, int myCpuVcores) {
      this.pmemLimit = myPmemLimit;
      this.vmemLimit = myVmemLimit;
      this.cpuVcores = myCpuVcores;
    }
  }

  /**
   * Check whether a container's process tree's current memory usage is over
   * limit.
   *
   * When a java process exec's a program, it could momentarily account for
   * double the size of it's memory, because the JVM does a fork()+exec()
   * which at fork time creates a copy of the parent's memory. If the
   * monitoring thread detects the memory used by the container tree at the
   * same instance, it could assume it is over limit and kill the tree, for no
   * fault of the process itself.
   *
   * We counter this problem by employing a heuristic check: - if a process
   * tree exceeds the memory limit by more than twice, it is killed
   * immediately - if a process tree has processes older than the monitoring
   * interval exceeding the memory limit by even 1 time, it is killed. Else it
   * is given the benefit of doubt to lie around for one more iteration.
   *
   * @param containerId
   *          Container Id for the container tree
   * @param currentMemUsage
   *          Memory usage of a container tree
   * @param curMemUsageOfAgedProcesses
   *          Memory usage of processes older than an iteration in a container
   *          tree
   * @param memLimit
   *          The limit specified for the container
   * @return true if the memory usage is more than twice the specified limit,
   *         or if processes in the tree, older than this thread's monitoring
   *         interval, exceed the memory limit. False, otherwise.
   */
  private boolean isProcessTreeOverLimit(String containerId,
                                  long currentMemUsage,
                                  long curMemUsageOfAgedProcesses,
                                  long memLimit) {
    boolean isOverLimit = false;

    if (currentMemUsage > (2 * memLimit)) {
      LOG.warn("Process tree for container: {} running over twice "
          + "the configured limit. Limit={}, current usage = {}",
          containerId, memLimit, currentMemUsage);
      isOverLimit = true;
    } else if (curMemUsageOfAgedProcesses > memLimit) {
      LOG.warn("Process tree for container: {} has processes older than 1 "
          + "iteration running over the configured limit. "
          + "Limit={}, current usage = {}",
          containerId, memLimit, curMemUsageOfAgedProcesses);
      isOverLimit = true;
    }

    return isOverLimit;
  }

  // method provided just for easy testing purposes
  boolean isProcessTreeOverLimit(ResourceCalculatorProcessTree pTree,
      String containerId, long limit) {
    long currentMemUsage = pTree.getVirtualMemorySize();
    // as processes begin with an age 1, we want to see if there are processes
    // more than 1 iteration old.
    long curMemUsageOfAgedProcesses = pTree.getVirtualMemorySize(1);
    return isProcessTreeOverLimit(containerId, currentMemUsage,
                                  curMemUsageOfAgedProcesses, limit);
  }

  private class MonitoringThread extends Thread {
    MonitoringThread() {
      super("Container Monitor");
    }

    @Override
    public void run() {

      while (!stopped && !Thread.currentThread().isInterrupted()) {
        long start = Time.monotonicNow();
        // Print the processTrees for debugging.
        if (LOG.isDebugEnabled()) {
          StringBuilder tmp = new StringBuilder("[ ");
          for (ProcessTreeInfo p : trackingContainers.values()) {
            tmp.append(p.getPID());
            tmp.append(" ");
          }
          tmp.append("]");
          LOG.debug("Current ProcessTree list : {}", tmp);
        }

        // Temporary structure to calculate the total resource utilization of
        // the containers
        ResourceUtilization trackedContainersUtilization  =
            ResourceUtilization.newInstance(0, 0, 0.0f);

        // Now do the monitoring for the trackingContainers
        // Check memory usage and kill any overflowing containers
        long vmemUsageByAllContainers = 0;
        long pmemByAllContainers = 0;
        long cpuUsagePercentPerCoreByAllContainers = 0;
        for (Entry<ContainerId, ProcessTreeInfo> entry : trackingContainers
            .entrySet()) {
          ContainerId containerId = entry.getKey();
          ProcessTreeInfo ptInfo = entry.getValue();
          try {
            // Initialize uninitialized process trees
            initializeProcessTrees(entry);

            String pId = ptInfo.getPID();
            if (pId == null || !isResourceCalculatorAvailable()) {
              continue; // processTree cannot be tracked
            }
            LOG.debug(
                "Constructing ProcessTree for : PID = {} ContainerId = {}",
                pId, containerId);
            ResourceCalculatorProcessTree pTree = ptInfo.getProcessTree();
            pTree.updateProcessTree();    // update process-tree
            long currentVmemUsage = pTree.getVirtualMemorySize();
            long currentPmemUsage = pTree.getRssMemorySize();
            if (currentVmemUsage < 0 || currentPmemUsage < 0) {
              // YARN-6862/YARN-5021 If the container just exited or for
              // another reason the physical/virtual memory is UNAVAILABLE (-1)
              // the values shouldn't be aggregated.
              LOG.info("Skipping monitoring container {} because "
                  + "memory usage is not available.", containerId);
              continue;
            }

            // if machine has 6 cores and 3 are used,
            // cpuUsagePercentPerCore should be 300%
            float cpuUsagePercentPerCore = pTree.getCpuUsagePercent();
            if (cpuUsagePercentPerCore < 0) {
              // CPU usage is not available likely because the container just
              // started. Let us skip this turn and consider this container
              // in the next iteration.
              LOG.info("Skipping monitoring container {} since "
                  + "CPU usage is not yet available.", containerId);
              continue;
            }

            recordUsage(containerId, pId, pTree, ptInfo, currentVmemUsage,
                    currentPmemUsage, trackedContainersUtilization);

            checkLimit(containerId, pId, pTree, ptInfo,
                    currentVmemUsage, currentPmemUsage);

            // Accounting the total memory in usage for all containers
            vmemUsageByAllContainers += currentVmemUsage;
            pmemByAllContainers += currentPmemUsage;
            // Accounting the total cpu usage for all containers
            cpuUsagePercentPerCoreByAllContainers += cpuUsagePercentPerCore;

            reportResourceUsage(containerId, currentPmemUsage,
                    cpuUsagePercentPerCore);
          } catch (Exception e) {
            // Log the exception and proceed to the next container.
            LOG.warn("Uncaught exception in ContainersMonitorImpl "
                + "while monitoring resource of {}", containerId, e);
          }
        }
        LOG.debug("Total Resource Usage stats in NM by all containers : "
            + "Virtual Memory= {}, Physical Memory= {}, "
            + "Total CPU usage(% per core)= {}", vmemUsageByAllContainers,
            pmemByAllContainers, cpuUsagePercentPerCoreByAllContainers);


        // Save the aggregated utilization of the containers
        setContainersUtilization(trackedContainersUtilization);

        long duration = Time.monotonicNow() - start;
        LOG.debug("Finished monitoring container cost {} ms", duration);

        // Publish the container utilization metrics to node manager
        // metrics system.
        NodeManagerMetrics nmMetrics = context.getNodeManagerMetrics();
        if (nmMetrics != null) {
          nmMetrics.setContainerUsedMemGB(
              trackedContainersUtilization.getPhysicalMemory());
          nmMetrics.setContainerUsedVMemGB(
              trackedContainersUtilization.getVirtualMemory());
          nmMetrics.setContainerCpuUtilization(
              trackedContainersUtilization.getCPU());
          nmMetrics.addContainerMonitorCostTime(duration);
        }

        try {
          Thread.sleep(monitoringInterval);
        } catch (InterruptedException e) {
          LOG.warn("{} is interrupted. Exiting.",
              ContainersMonitorImpl.class.getName());
          break;
        }
      }
    }

    /**
     * Initialize any uninitialized processTrees.
     * @param entry process tree entry to fill in
     */
    private void initializeProcessTrees(
            Entry<ContainerId, ProcessTreeInfo> entry)
        throws ContainerExecutionException {
      ContainerId containerId = entry.getKey();
      ProcessTreeInfo ptInfo = entry.getValue();
      String pId = ptInfo.getPID();

      // Initialize any uninitialized processTrees
      if (pId == null) {
        // get pid from ContainerId
        pId = containerExecutor.getProcessId(ptInfo.getContainerId());
        if (pId != null) {
          // pId will be null, either if the container is not spawned yet
          // or if the container's pid is removed from ContainerExecutor
          LOG.debug("Tracking ProcessTree {} for the first time", pId);
          ResourceCalculatorProcessTree pt =
              getResourceCalculatorProcessTree(pId);
          ptInfo.setPid(pId);
          ptInfo.setProcessTree(pt);

          if (containerMetricsEnabled) {
            ContainerMetrics usageMetrics = ContainerMetrics
                    .forContainer(containerId, containerMetricsPeriodMs,
                      containerMetricsUnregisterDelayMs);
            usageMetrics.recordProcessId(pId);
          }

          Container container = context.getContainers().get(containerId);

          if (container != null) {
            String[] ipAndHost = containerExecutor.getIpAndHost(container);

            if ((ipAndHost != null) && (ipAndHost[0] != null) &&
                (ipAndHost[1] != null)) {
              container.setIpAndHost(ipAndHost);
              LOG.info("{}'s ip = {}, and hostname = {}",
                  containerId, ipAndHost[0], ipAndHost[1]);
            } else {
              LOG.info("Can not get both ip and hostname: {}",
                  Arrays.toString(ipAndHost));
            }
            String exposedPorts = containerExecutor.getExposedPorts(container);
            container.setExposedPorts(exposedPorts);
          } else {
            LOG.info("{} is missing. Not setting ip and hostname", containerId);
          }
        }
      }
      // End of initializing any uninitialized processTrees
    }

    /**
     * Record usage metrics.
     * @param containerId container id
     * @param pId process id
     * @param pTree valid process tree entry with CPU measurement
     * @param ptInfo process tree info with limit information
     * @param currentVmemUsage virtual memory measurement
     * @param currentPmemUsage physical memory measurement
     * @param trackedContainersUtilization utilization tracker to update
     */
    private void recordUsage(ContainerId containerId, String pId,
                             ResourceCalculatorProcessTree pTree,
                             ProcessTreeInfo ptInfo,
                             long currentVmemUsage, long currentPmemUsage,
                             ResourceUtilization trackedContainersUtilization) {
      // if machine has 6 cores and 3 are used,
      // cpuUsagePercentPerCore should be 300% and
      // cpuUsageTotalCoresPercentage should be 50%
      float cpuUsagePercentPerCore = pTree.getCpuUsagePercent();
      float cpuUsageTotalCoresPercentage = cpuUsagePercentPerCore /
              resourceCalculatorPlugin.getNumProcessors();

      // Multiply by 1000 to avoid losing data when converting to int
      int milliVcoresUsed = (int) (cpuUsageTotalCoresPercentage * 1000
              * maxVCoresAllottedForContainers /nodeCpuPercentageForYARN);
      long vmemLimit = ptInfo.getVmemLimit();
      long pmemLimit = ptInfo.getPmemLimit();
      if (AUDITLOG.isDebugEnabled()) {
        int vcoreLimit = ptInfo.getCpuVcores();
        long cumulativeCpuTime = pTree.getCumulativeCpuTime();
        AUDITLOG.debug(
            "Resource usage of ProcessTree {} for container-id {}:" +
            " {} %CPU: {} %CPU-cores: {}" +
            " vCores-used: {} of {} Cumulative-CPU-ms: {}",
            pId, containerId,
            formatUsageString(
                currentVmemUsage, vmemLimit,
                currentPmemUsage, pmemLimit),
            cpuUsagePercentPerCore,
            cpuUsageTotalCoresPercentage,
            milliVcoresUsed / 1000, vcoreLimit,
            cumulativeCpuTime);
      }

      // Add resource utilization for this container
      trackedContainersUtilization.addTo(
              (int) (currentPmemUsage >> 20),
              (int) (currentVmemUsage >> 20),
              milliVcoresUsed / 1000.0f);

      // Add usage to container metrics
      if (containerMetricsEnabled) {
        ContainerMetrics.forContainer(
                containerId, containerMetricsPeriodMs,
                containerMetricsUnregisterDelayMs).recordMemoryUsage(
                (int) (currentPmemUsage >> 20));
        ContainerMetrics.forContainer(
                containerId, containerMetricsPeriodMs,
                containerMetricsUnregisterDelayMs).recordCpuUsage((int)
                cpuUsagePercentPerCore, milliVcoresUsed);
      }
    }

    /**
     * Check resource limits and take actions if the limit is exceeded.
     * @param containerId container id
     * @param pId process id
     * @param pTree valid process tree entry with CPU measurement
     * @param ptInfo process tree info with limit information
     * @param currentVmemUsage virtual memory measurement
     * @param currentPmemUsage physical memory measurement
     */
    @SuppressWarnings("unchecked")
    private void checkLimit(ContainerId containerId, String pId,
                            ResourceCalculatorProcessTree pTree,
                            ProcessTreeInfo ptInfo,
                            long currentVmemUsage,
                            long currentPmemUsage) {
      if (strictMemoryEnforcement && !elasticMemoryEnforcement) {
        // When cgroup-based strict memory enforcement is used alone without
        // elastic memory control, the oom-kill would take care of it.
        // However, when elastic memory control is also enabled, the oom killer
        // would be disabled at the root yarn container cgroup level (all child
        // cgroups would inherit that setting). Hence, we fall back to the
        // polling-based mechanism.
        return;
      }
      boolean isMemoryOverLimit = false;
      String msg = "";
      int containerExitStatus = ContainerExitStatus.INVALID;

      long vmemLimit = ptInfo.getVmemLimit();
      long pmemLimit = ptInfo.getPmemLimit();
      // as processes begin with an age 1, we want to see if there
      // are processes more than 1 iteration old.
      long curMemUsageOfAgedProcesses = pTree.getVirtualMemorySize(1);
      long curRssMemUsageOfAgedProcesses = pTree.getRssMemorySize(1);
      if (isVmemCheckEnabled()
          && isProcessTreeOverLimit(containerId.toString(),
          currentVmemUsage, curMemUsageOfAgedProcesses, vmemLimit)) {
        // The current usage (age=0) is always higher than the aged usage. We
        // do not show the aged size in the message, base the delta on the
        // current usage
        long delta = currentVmemUsage - vmemLimit;
        // Container (the root process) is still alive and overflowing
        // memory.
        // Dump the process-tree and then clean it up.
        msg = formatErrorMessage("virtual",
            formatUsageString(currentVmemUsage, vmemLimit,
                currentPmemUsage, pmemLimit),
            pId, containerId, pTree, delta);
        isMemoryOverLimit = true;
        containerExitStatus = ContainerExitStatus.KILLED_EXCEEDED_VMEM;
      } else if (isPmemCheckEnabled()
          && isProcessTreeOverLimit(containerId.toString(),
          currentPmemUsage, curRssMemUsageOfAgedProcesses,
          pmemLimit)) {
        // The current usage (age=0) is always higher than the aged usage. We
        // do not show the aged size in the message, base the delta on the
        // current usage
        long delta = currentPmemUsage - pmemLimit;
        // Container (the root process) is still alive and overflowing
        // memory.
        // Dump the process-tree and then clean it up.
        msg = formatErrorMessage("physical",
            formatUsageString(currentVmemUsage, vmemLimit,
                currentPmemUsage, pmemLimit),
            pId, containerId, pTree, delta);
        isMemoryOverLimit = true;
        containerExitStatus = ContainerExitStatus.KILLED_EXCEEDED_PMEM;
      }

      if (isMemoryOverLimit
          && trackingContainers.remove(containerId) != null) {
        // Virtual or physical memory over limit. Fail the container and
        // remove
        // the corresponding process tree
        LOG.warn(msg);
        // warn if not a leader
        if (!pTree.checkPidPgrpidForMatch()) {
          LOG.error("Killed container process with PID {} "
              + "but it is not a process group leader.", pId);
        }
        // kill the container
        eventDispatcher.getEventHandler().handle(
                new ContainerKillEvent(containerId,
                      containerExitStatus, msg));
        LOG.info("Removed ProcessTree with root {}", pId);
      }
    }

    /**
     * Report usage metrics to the timeline service.
     * @param containerId container id
     * @param currentPmemUsage physical memory measurement
     * @param cpuUsagePercentPerCore CPU usage
     */
    private void reportResourceUsage(ContainerId containerId,
        long currentPmemUsage, float cpuUsagePercentPerCore) {
      ContainerImpl container =
              (ContainerImpl) context.getContainers().get(containerId);
      if (container != null) {
        NMTimelinePublisher nmMetricsPublisher =
                container.getNMTimelinePublisher();
        if (nmMetricsPublisher != null) {
          nmMetricsPublisher.reportContainerResourceUsage(container,
                  currentPmemUsage, cpuUsagePercentPerCore);
        }
      } else {
        LOG.info("{} does not exist to report", containerId);
      }
    }

    /**
     * Format string when memory limit has been exceeded.
     * @param memTypeExceeded type of memory
     * @param usageString general memory usage information string
     * @param pId process id
     * @param containerId container id
     * @param pTree process tree to dump full resource utilization graph
     * @return formatted resource usage information
     */
    private String formatErrorMessage(String memTypeExceeded,
        String usageString, String pId, ContainerId containerId,
        ResourceCalculatorProcessTree pTree, long delta) {
      return
        String.format("Container [pid=%s,containerID=%s] is " +
            "running %dB beyond the '%S' memory limit. ",
            pId, containerId, delta, memTypeExceeded) +
        "Current usage: " + usageString +
        ". Killing container.\n" +
        "Dump of the process-tree for " + containerId + " :\n" +
        pTree.getProcessTreeDump();
    }

    /**
     * Format memory usage string for reporting.
     * @param currentVmemUsage virtual memory usage
     * @param vmemLimit virtual memory limit
     * @param currentPmemUsage physical memory usage
     * @param pmemLimit physical memory limit
     * @return formatted memory information
     */
    private String formatUsageString(long currentVmemUsage, long vmemLimit,
        long currentPmemUsage, long pmemLimit) {
      return String.format("%sB of %sB physical memory used; " +
          "%sB of %sB virtual memory used",
          TraditionalBinaryPrefix.long2String(currentPmemUsage, "", 1),
          TraditionalBinaryPrefix.long2String(pmemLimit, "", 1),
          TraditionalBinaryPrefix.long2String(currentVmemUsage, "", 1),
          TraditionalBinaryPrefix.long2String(vmemLimit, "", 1));
    }
  }

  private class LogMonitorThread extends Thread {
    LogMonitorThread() {
      super("Container Log Monitor");
    }

    @Override
    public void run() {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        for (Entry<ContainerId, ProcessTreeInfo> entry :
            trackingContainers.entrySet()) {
          ContainerId containerId = entry.getKey();
          ProcessTreeInfo ptInfo = entry.getValue();
          Container container = context.getContainers().get(containerId);
          if (container == null) {
            continue;
          }
          try {
            List<File> logDirs = ContainerLogsUtils.getContainerLogDirs(
                containerId, container.getUser(), context);
            long totalLogDataBytes = 0;
            for (File dir : logDirs) {
              long currentDirSizeBytes = FileUtil.getDU(dir);
              totalLogDataBytes += currentDirSizeBytes;
              String killMsg = null;
              if (currentDirSizeBytes > logDirSizeLimit) {
                killMsg = String.format(
                    "Container [pid=%s,containerID=%s] is logging beyond "
                        + "the container single log directory limit.%n"
                        + "Limit: %d Log Directory Size: %d Log Directory: %s"
                        + "%nKilling container.%n",
                    ptInfo.getPID(), containerId, logDirSizeLimit,
                    currentDirSizeBytes, dir);
              } else if (totalLogDataBytes > logTotalSizeLimit) {
                killMsg = String.format(
                    "Container [pid=%s,containerID=%s] is logging beyond "
                        + "the container total log limit.%n"
                        + "Limit: %d Total Size: >=%d"
                        + "%nKilling container.%n",
                    ptInfo.getPID(), containerId, logTotalSizeLimit,
                    totalLogDataBytes);
              }
              if (killMsg != null
                  && trackingContainers.remove(containerId) != null) {
                LOG.warn(killMsg);
                eventDispatcher.getEventHandler().handle(
                    new ContainerKillEvent(containerId,
                        ContainerExitStatus.KILLED_FOR_EXCESS_LOGS, killMsg));
                LOG.info("Removed ProcessTree with root " + ptInfo.getPID());
                break;
              }
            }
          } catch (Exception e) {
            LOG.warn("Uncaught exception in ContainerMemoryManager "
                + "while monitoring log usage for " + containerId, e);
          }
        }
        try {
          Thread.sleep(logCheckInterval);
        } catch (InterruptedException e) {
          LOG.info("Log monitor thread was interrupted. "
              + "Stopping container log monitoring.");
        }
      }
    }
  }

  private void updateContainerMetrics(ContainersMonitorEvent monitoringEvent) {
    if (!containerMetricsEnabled || monitoringEvent == null) {
      return;
    }

    ContainerId containerId = monitoringEvent.getContainerId();
    ContainerMetrics usageMetrics;

    int vmemLimitMBs;
    int pmemLimitMBs;
    int cpuVcores;
    switch (monitoringEvent.getType()) {
    case START_MONITORING_CONTAINER:
      usageMetrics = ContainerMetrics
          .forContainer(containerId, containerMetricsPeriodMs,
          containerMetricsUnregisterDelayMs);
      ContainerStartMonitoringEvent startEvent =
          (ContainerStartMonitoringEvent) monitoringEvent;
      usageMetrics.recordStateChangeDurations(
          startEvent.getLaunchDuration(),
          startEvent.getLocalizationDuration());
      cpuVcores = startEvent.getCpuVcores();
      vmemLimitMBs = (int) (startEvent.getVmemLimit() >> 20);
      pmemLimitMBs = (int) (startEvent.getPmemLimit() >> 20);
      usageMetrics.recordResourceLimit(
          vmemLimitMBs, pmemLimitMBs, cpuVcores);
      break;
    case STOP_MONITORING_CONTAINER:
      ContainerStopMonitoringEvent stopEvent =
          (ContainerStopMonitoringEvent) monitoringEvent;
      usageMetrics = ContainerMetrics.getContainerMetrics(
          containerId);
      if (usageMetrics != null) {
        usageMetrics.finished(stopEvent.isForReInit());
      }
      break;
    case CHANGE_MONITORING_CONTAINER_RESOURCE:
      usageMetrics = ContainerMetrics
          .forContainer(containerId, containerMetricsPeriodMs,
          containerMetricsUnregisterDelayMs);
      ChangeMonitoringContainerResourceEvent changeEvent =
          (ChangeMonitoringContainerResourceEvent) monitoringEvent;
      Resource resource = changeEvent.getResource();
      pmemLimitMBs = (int) resource.getMemorySize();
      vmemLimitMBs = (int) (pmemLimitMBs * vmemRatio);
      cpuVcores = resource.getVirtualCores();
      usageMetrics.recordResourceLimit(
          vmemLimitMBs, pmemLimitMBs, cpuVcores);
      break;
    default:
      break;
    }
  }

  @Override
  public long getVmemAllocatedForContainers() {
    return this.maxVmemAllottedForContainers;
  }

  /**
   * Is the total physical memory check enabled?
   *
   * @return true if total physical memory check is enabled.
   */
  @Override
  public boolean isPmemCheckEnabled() {
    return this.pmemCheckEnabled;
  }

  @Override
  public long getPmemAllocatedForContainers() {
    return this.maxPmemAllottedForContainers;
  }

  @Override
  public long getVCoresAllocatedForContainers() {
    return this.maxVCoresAllottedForContainers;
  }

  @Override
  public void setAllocatedResourcesForContainers(final Resource resource) {
    LOG.info("Setting the resources allocated to containers to {}", resource);
    this.maxVCoresAllottedForContainers = resource.getVirtualCores();
    this.maxPmemAllottedForContainers = convertMBytesToBytes(
        resource.getMemorySize());
    this.maxVmemAllottedForContainers =
        (long) (getVmemRatio() * maxPmemAllottedForContainers);
  }

  /**
   * Is the total virtual memory check enabled?
   *
   * @return true if total virtual memory check is enabled.
   */
  @Override
  public boolean isVmemCheckEnabled() {
    return this.vmemCheckEnabled;
  }

  @Override
  public ResourceUtilization getContainersUtilization() {
    return this.containersUtilization;
  }

  private void setContainersUtilization(ResourceUtilization utilization) {
    this.containersUtilization = utilization;
  }

  @Override
  public void subtractNodeResourcesFromResourceUtilization(
      ResourceUtilization resourceUtil) {
    resourceUtil.subtractFrom((int) (getPmemAllocatedForContainers() >> 20),
        (int) (getVmemAllocatedForContainers() >> 20),
        getVCoresAllocatedForContainers());
  }

  @Override
  public float getVmemRatio() {
    return vmemRatio;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void handle(ContainersMonitorEvent monitoringEvent) {
    ContainerId containerId = monitoringEvent.getContainerId();

    switch (monitoringEvent.getType()) {
    case START_MONITORING_CONTAINER:
      onStartMonitoringContainer(monitoringEvent, containerId);
      break;
    case STOP_MONITORING_CONTAINER:
      onStopMonitoringContainer(monitoringEvent, containerId);
      break;
    case CHANGE_MONITORING_CONTAINER_RESOURCE:
      onChangeMonitoringContainerResource(monitoringEvent, containerId);
      break;
    default:
      // TODO: Wrong event.
    }
  }

  private void onChangeMonitoringContainerResource(
      ContainersMonitorEvent monitoringEvent, ContainerId containerId) {
    ChangeMonitoringContainerResourceEvent changeEvent =
        (ChangeMonitoringContainerResourceEvent) monitoringEvent;
    if (containersMonitorEnabled) {
      ProcessTreeInfo processTreeInfo = trackingContainers.get(containerId);
      if (processTreeInfo == null) {
        LOG.warn("Failed to track container {}. It may have already completed.",
            containerId);
        return;
      }
      LOG.info("Changing resource-monitoring for {}", containerId);
      updateContainerMetrics(monitoringEvent);
      Resource resource = changeEvent.getResource();
      long pmemLimit = convertMBytesToBytes(resource.getMemorySize());
      long vmemLimit = (long) (pmemLimit * vmemRatio);
      int cpuVcores = resource.getVirtualCores();
      processTreeInfo.setResourceLimit(pmemLimit, vmemLimit, cpuVcores);
    }
  }

  private void onStopMonitoringContainer(
      ContainersMonitorEvent monitoringEvent, ContainerId containerId) {
    LOG.info("Stopping resource-monitoring for {}", containerId);
    updateContainerMetrics(monitoringEvent);
    trackingContainers.remove(containerId);
  }

  private void onStartMonitoringContainer(
      ContainersMonitorEvent monitoringEvent, ContainerId containerId) {
    ContainerStartMonitoringEvent startEvent =
        (ContainerStartMonitoringEvent) monitoringEvent;
    LOG.info("Starting resource-monitoring for {}", containerId);
    updateContainerMetrics(monitoringEvent);
    trackingContainers.put(containerId,
        new ProcessTreeInfo(containerId, null, null,
            startEvent.getVmemLimit(), startEvent.getPmemLimit(),
            startEvent.getCpuVcores()));
  }

  /**
   * Convert MegaBytes to Bytes.
   * @param mb MegaBytes (MB).
   * @return Bytes representing the input MB.
   */
  private static long convertMBytesToBytes(long mb) {
    return mb * 1024L * 1024L;
  }
}
