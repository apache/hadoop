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

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerHardwareUtils;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;

import com.google.common.base.Preconditions;

public class ContainersMonitorImpl extends AbstractService implements
    ContainersMonitor {

  final static Log LOG = LogFactory
      .getLog(ContainersMonitorImpl.class);

  private long monitoringInterval;
  private MonitoringThread monitoringThread;
  private boolean containerMetricsEnabled;
  private long containerMetricsPeriodMs;
  private long containerMetricsUnregisterDelayMs;

  @VisibleForTesting
  final Map<ContainerId, ProcessTreeInfo> trackingContainers =
      new ConcurrentHashMap<>();

  private final ContainerExecutor containerExecutor;
  private final Dispatcher eventDispatcher;
  protected final Context context;
  private ResourceCalculatorPlugin resourceCalculatorPlugin;
  private Configuration conf;
  private static float vmemRatio;
  private Class<? extends ResourceCalculatorProcessTree> processTreeClass;

  private long maxVmemAllottedForContainers = UNKNOWN_MEMORY_LIMIT;
  private long maxPmemAllottedForContainers = UNKNOWN_MEMORY_LIMIT;

  private boolean pmemCheckEnabled;
  private boolean vmemCheckEnabled;
  private boolean containersMonitorEnabled;

  private long maxVCoresAllottedForContainers;

  private static final long UNKNOWN_MEMORY_LIMIT = -1L;
  private int nodeCpuPercentageForYARN;

  private ResourceUtilization containersUtilization;
  // Tracks the aggregated allocation of the currently allocated containers
  // when queuing of containers at the NMs is enabled.
  private ResourceUtilization containersAllocation;

  private volatile boolean stopped = false;

  public ContainersMonitorImpl(ContainerExecutor exec,
      AsyncDispatcher dispatcher, Context context) {
    super("containers-monitor");

    this.containerExecutor = exec;
    this.eventDispatcher = dispatcher;
    this.context = context;

    this.monitoringThread = new MonitoringThread();

    this.containersUtilization = ResourceUtilization.newInstance(0, 0, 0.0f);
    this.containersAllocation = ResourceUtilization.newInstance(0, 0, 0.0f);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.monitoringInterval =
        conf.getLong(YarnConfiguration.NM_CONTAINER_MON_INTERVAL_MS,
            conf.getLong(YarnConfiguration.NM_RESOURCE_MON_INTERVAL_MS,
                YarnConfiguration.DEFAULT_NM_RESOURCE_MON_INTERVAL_MS));

    Class<? extends ResourceCalculatorPlugin> clazz =
        conf.getClass(YarnConfiguration.NM_CONTAINER_MON_RESOURCE_CALCULATOR,
            conf.getClass(
                YarnConfiguration.NM_MON_RESOURCE_CALCULATOR, null,
                ResourceCalculatorPlugin.class),
            ResourceCalculatorPlugin.class);
    this.resourceCalculatorPlugin =
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(clazz, conf);
    LOG.info(" Using ResourceCalculatorPlugin : "
        + this.resourceCalculatorPlugin);
    processTreeClass = conf.getClass(YarnConfiguration.NM_CONTAINER_MON_PROCESS_TREE, null,
            ResourceCalculatorProcessTree.class);
    this.conf = conf;
    LOG.info(" Using ResourceCalculatorProcessTree : "
        + this.processTreeClass);

    this.containerMetricsEnabled =
        conf.getBoolean(YarnConfiguration.NM_CONTAINER_METRICS_ENABLE,
            YarnConfiguration.DEFAULT_NM_CONTAINER_METRICS_ENABLE);
    this.containerMetricsPeriodMs =
        conf.getLong(YarnConfiguration.NM_CONTAINER_METRICS_PERIOD_MS,
            YarnConfiguration.DEFAULT_NM_CONTAINER_METRICS_PERIOD_MS);
    this.containerMetricsUnregisterDelayMs = conf.getLong(
        YarnConfiguration.NM_CONTAINER_METRICS_UNREGISTER_DELAY_MS,
        YarnConfiguration.DEFAULT_NM_CONTAINER_METRICS_UNREGISTER_DELAY_MS);

    long configuredPMemForContainers =
        NodeManagerHardwareUtils.getContainerMemoryMB(
            this.resourceCalculatorPlugin, conf) * 1024 * 1024L;

    long configuredVCoresForContainers =
        NodeManagerHardwareUtils.getVCores(this.resourceCalculatorPlugin, conf);

    // Setting these irrespective of whether checks are enabled. Required in
    // the UI.
    // ///////// Physical memory configuration //////
    this.maxPmemAllottedForContainers = configuredPMemForContainers;
    this.maxVCoresAllottedForContainers = configuredVCoresForContainers;

    // ///////// Virtual memory configuration //////
    vmemRatio = conf.getFloat(YarnConfiguration.NM_VMEM_PMEM_RATIO,
        YarnConfiguration.DEFAULT_NM_VMEM_PMEM_RATIO);
    Preconditions.checkArgument(vmemRatio > 0.99f,
        YarnConfiguration.NM_VMEM_PMEM_RATIO + " should be at least 1.0");
    this.maxVmemAllottedForContainers =
        (long) (vmemRatio * configuredPMemForContainers);

    pmemCheckEnabled = conf.getBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED,
        YarnConfiguration.DEFAULT_NM_PMEM_CHECK_ENABLED);
    vmemCheckEnabled = conf.getBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED,
        YarnConfiguration.DEFAULT_NM_VMEM_CHECK_ENABLED);
    LOG.info("Physical memory check enabled: " + pmemCheckEnabled);
    LOG.info("Virtual memory check enabled: " + vmemCheckEnabled);

    containersMonitorEnabled = isEnabled();
    LOG.info("ContainersMonitor enabled: " + containersMonitorEnabled);

    nodeCpuPercentageForYARN =
        NodeManagerHardwareUtils.getNodeCpuPercentage(conf);

    if (pmemCheckEnabled) {
      // Logging if actual pmem cannot be determined.
      long totalPhysicalMemoryOnNM = UNKNOWN_MEMORY_LIMIT;
      if (this.resourceCalculatorPlugin != null) {
        totalPhysicalMemoryOnNM = this.resourceCalculatorPlugin
            .getPhysicalMemorySize();
        if (totalPhysicalMemoryOnNM <= 0) {
          LOG.warn("NodeManager's totalPmem could not be calculated. "
              + "Setting it to " + UNKNOWN_MEMORY_LIMIT);
          totalPhysicalMemoryOnNM = UNKNOWN_MEMORY_LIMIT;
        }
      }

      if (totalPhysicalMemoryOnNM != UNKNOWN_MEMORY_LIMIT &&
          this.maxPmemAllottedForContainers > totalPhysicalMemoryOnNM * 0.80f) {
        LOG.warn("NodeManager configured with "
            + TraditionalBinaryPrefix.long2String(maxPmemAllottedForContainers,
                "", 1)
            + " physical memory allocated to containers, which is more than "
            + "80% of the total physical memory available ("
            + TraditionalBinaryPrefix.long2String(totalPhysicalMemoryOnNM, "",
                1) + "). Thrashing might happen.");
      }
    }
    super.serviceInit(conf);
  }

  private boolean isEnabled() {
    if (resourceCalculatorPlugin == null) {
            LOG.info("ResourceCalculatorPlugin is unavailable on this system. "
                + this.getClass().getName() + " is disabled.");
            return false;
    }
    if (ResourceCalculatorProcessTree.getResourceCalculatorProcessTree("0", processTreeClass, conf) == null) {
        LOG.info("ResourceCalculatorProcessTree is unavailable on this system. "
                + this.getClass().getName() + " is disabled.");
            return false;
    }
    if (!(isPmemCheckEnabled() || isVmemCheckEnabled())) {
      LOG.info("Neither virutal-memory nor physical-memory monitoring is " +
          "needed. Not running the monitor-thread");
      return false;
    }

    return true;
  }

  @Override
  protected void serviceStart() throws Exception {
    if (containersMonitorEnabled) {
      this.monitoringThread.start();
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (containersMonitorEnabled) {
      stopped = true;
      this.monitoringThread.interrupt();
      try {
        this.monitoringThread.join();
      } catch (InterruptedException e) {
        ;
      }
    }
    super.serviceStop();
  }

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

    public ResourceCalculatorProcessTree getProcessTree() {
      return this.pTree;
    }

    public void setProcessTree(ResourceCalculatorProcessTree pTree) {
      this.pTree = pTree;
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
     * Set resource limit for enforcement
     * @param pmemLimit
     *          Physical memory limit for the process tree in bytes
     * @param vmemLimit
     *          Virtual memory limit for the process tree in bytes
     * @param cpuVcores
     *          Number of cpu vcores assigned
     */
    public synchronized void setResourceLimit(
        long pmemLimit, long vmemLimit, int cpuVcores) {
      this.pmemLimit = pmemLimit;
      this.vmemLimit = vmemLimit;
      this.cpuVcores = cpuVcores;
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
   * @param vmemLimit
   *          The limit specified for the container
   * @return true if the memory usage is more than twice the specified limit,
   *         or if processes in the tree, older than this thread's monitoring
   *         interval, exceed the memory limit. False, otherwise.
   */
  boolean isProcessTreeOverLimit(String containerId,
                                  long currentMemUsage,
                                  long curMemUsageOfAgedProcesses,
                                  long vmemLimit) {
    boolean isOverLimit = false;

    if (currentMemUsage > (2 * vmemLimit)) {
      LOG.warn("Process tree for container: " + containerId
          + " running over twice " + "the configured limit. Limit=" + vmemLimit
          + ", current usage = " + currentMemUsage);
      isOverLimit = true;
    } else if (curMemUsageOfAgedProcesses > vmemLimit) {
      LOG.warn("Process tree for container: " + containerId
          + " has processes older than 1 "
          + "iteration running over the configured limit. Limit=" + vmemLimit
          + ", current usage = " + curMemUsageOfAgedProcesses);
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
    public MonitoringThread() {
      super("Container Monitor");
    }

    @Override
    public void run() {

      while (!stopped && !Thread.currentThread().isInterrupted()) {
        // Print the processTrees for debugging.
        if (LOG.isDebugEnabled()) {
          StringBuilder tmp = new StringBuilder("[ ");
          for (ProcessTreeInfo p : trackingContainers.values()) {
            tmp.append(p.getPID());
            tmp.append(" ");
          }
          LOG.debug("Current ProcessTree list : "
              + tmp.substring(0, tmp.length()) + "]");
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
        long cpuUsageTotalCoresByAllContainers = 0;
        for (Entry<ContainerId, ProcessTreeInfo> entry : trackingContainers
            .entrySet()) {
          ContainerId containerId = entry.getKey();
          ProcessTreeInfo ptInfo = entry.getValue();
          try {
            String pId = ptInfo.getPID();

            // Initialize any uninitialized processTrees
            if (pId == null) {
              // get pid from ContainerId
              pId = containerExecutor.getProcessId(ptInfo.getContainerId());
              if (pId != null) {
                // pId will be null, either if the container is not spawned yet
                // or if the container's pid is removed from ContainerExecutor
                LOG.debug("Tracking ProcessTree " + pId
                    + " for the first time");

                ResourceCalculatorProcessTree pt =
                    ResourceCalculatorProcessTree.getResourceCalculatorProcessTree(pId, processTreeClass, conf);
                ptInfo.setPid(pId);
                ptInfo.setProcessTree(pt);

                if (containerMetricsEnabled) {
                  ContainerMetrics usageMetrics = ContainerMetrics
                      .forContainer(containerId, containerMetricsPeriodMs,
                      containerMetricsUnregisterDelayMs);
                  usageMetrics.recordProcessId(pId);
                }
              }
            }
            // End of initializing any uninitialized processTrees

            if (pId == null) {
              continue; // processTree cannot be tracked
            }

            LOG.debug("Constructing ProcessTree for : PID = " + pId
                + " ContainerId = " + containerId);
            ResourceCalculatorProcessTree pTree = ptInfo.getProcessTree();
            pTree.updateProcessTree();    // update process-tree
            long currentVmemUsage = pTree.getVirtualMemorySize();
            long currentPmemUsage = pTree.getRssMemorySize();
            // if machine has 6 cores and 3 are used,
            // cpuUsagePercentPerCore should be 300% and
            // cpuUsageTotalCoresPercentage should be 50%
            float cpuUsagePercentPerCore = pTree.getCpuUsagePercent();
            if (cpuUsagePercentPerCore < 0) {
              // CPU usage is not available likely because the container just
              // started. Let us skip this turn and consider this container
              // in the next iteration.
              LOG.info("Skipping monitoring container " + containerId
                  + " since CPU usage is not yet available.");
              continue;
            }

            float cpuUsageTotalCoresPercentage = cpuUsagePercentPerCore /
                resourceCalculatorPlugin.getNumProcessors();

            // Multiply by 1000 to avoid losing data when converting to int
            int milliVcoresUsed = (int) (cpuUsageTotalCoresPercentage * 1000
                * maxVCoresAllottedForContainers /nodeCpuPercentageForYARN);
            // as processes begin with an age 1, we want to see if there
            // are processes more than 1 iteration old.
            long curMemUsageOfAgedProcesses = pTree.getVirtualMemorySize(1);
            long curRssMemUsageOfAgedProcesses = pTree.getRssMemorySize(1);
            long vmemLimit = ptInfo.getVmemLimit();
            long pmemLimit = ptInfo.getPmemLimit();
            if (LOG.isDebugEnabled()) {
              LOG.debug(String.format(
                  "Memory usage of ProcessTree %s for container-id %s: ",
                  pId, containerId.toString()) +
                  formatUsageString(
                      currentVmemUsage, vmemLimit,
                      currentPmemUsage, pmemLimit));
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
                  containerMetricsUnregisterDelayMs).recordCpuUsage
                  ((int)cpuUsagePercentPerCore, milliVcoresUsed);
            }

            boolean isMemoryOverLimit = false;
            String msg = "";
            int containerExitStatus = ContainerExitStatus.INVALID;
            if (isVmemCheckEnabled()
                && isProcessTreeOverLimit(containerId.toString(),
                    currentVmemUsage, curMemUsageOfAgedProcesses, vmemLimit)) {
              // Container (the root process) is still alive and overflowing
              // memory.
              // Dump the process-tree and then clean it up.
              msg = formatErrorMessage("virtual",
                  currentVmemUsage, vmemLimit,
                  currentPmemUsage, pmemLimit,
                  pId, containerId, pTree);
              isMemoryOverLimit = true;
              containerExitStatus = ContainerExitStatus.KILLED_EXCEEDED_VMEM;
            } else if (isPmemCheckEnabled()
                && isProcessTreeOverLimit(containerId.toString(),
                    currentPmemUsage, curRssMemUsageOfAgedProcesses,
                    pmemLimit)) {
              // Container (the root process) is still alive and overflowing
              // memory.
              // Dump the process-tree and then clean it up.
              msg = formatErrorMessage("physical",
                  currentVmemUsage, vmemLimit,
                  currentPmemUsage, pmemLimit,
                  pId, containerId, pTree);
              isMemoryOverLimit = true;
              containerExitStatus = ContainerExitStatus.KILLED_EXCEEDED_PMEM;
            }

            // Accounting the total memory in usage for all containers
            vmemUsageByAllContainers += currentVmemUsage;
            pmemByAllContainers += currentPmemUsage;
            // Accounting the total cpu usage for all containers
            cpuUsagePercentPerCoreByAllContainers += cpuUsagePercentPerCore;
            cpuUsageTotalCoresByAllContainers += cpuUsagePercentPerCore;

            if (isMemoryOverLimit) {
              // Virtual or physical memory over limit. Fail the container and
              // remove
              // the corresponding process tree
              LOG.warn(msg);
              // warn if not a leader
              if (!pTree.checkPidPgrpidForMatch()) {
                LOG.error("Killed container process with PID " + pId
                    + " but it is not a process group leader.");
              }
              // kill the container
              eventDispatcher.getEventHandler().handle(
                  new ContainerKillEvent(containerId,
                      containerExitStatus, msg));
              trackingContainers.remove(containerId);
              LOG.info("Removed ProcessTree with root " + pId);
            }
          } catch (Exception e) {
            // Log the exception and proceed to the next container.
            LOG.warn("Uncaught exception in ContainerMemoryManager "
                + "while managing memory of " + containerId, e);
          }
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Total Resource Usage stats in NM by all containers : "
              + "Virtual Memory= " + vmemUsageByAllContainers
              + ", Physical Memory= " + pmemByAllContainers
              + ", Total CPU usage= " + cpuUsageTotalCoresByAllContainers
              + ", Total CPU(% per core) usage"
              + cpuUsagePercentPerCoreByAllContainers);
        }

        // Save the aggregated utilization of the containers
        setContainersUtilization(trackedContainersUtilization);

        try {
          Thread.sleep(monitoringInterval);
        } catch (InterruptedException e) {
          LOG.warn(ContainersMonitorImpl.class.getName()
              + " is interrupted. Exiting.");
          break;
        }
      }
    }

    private String formatErrorMessage(String memTypeExceeded,
        long currentVmemUsage, long vmemLimit,
        long currentPmemUsage, long pmemLimit,
        String pId, ContainerId containerId, ResourceCalculatorProcessTree pTree) {
      return
        String.format("Container [pid=%s,containerID=%s] is running beyond %s memory limits. ",
            pId, containerId, memTypeExceeded) +
        "Current usage: " +
        formatUsageString(currentVmemUsage, vmemLimit,
                          currentPmemUsage, pmemLimit) +
        ". Killing container.\n" +
        "Dump of the process-tree for " + containerId + " :\n" +
        pTree.getProcessTreeDump();
    }

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

  private void changeContainerResource(
      ContainerId containerId, Resource resource) {
    Container container = context.getContainers().get(containerId);
    // Check container existence
    if (container == null) {
      LOG.warn("Container " + containerId.toString() + "does not exist");
      return;
    }
    container.setResource(resource);
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
      usageMetrics = ContainerMetrics.getContainerMetrics(
          containerId);
      if (usageMetrics != null) {
        usageMetrics.finished();
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

  public void setContainersUtilization(ResourceUtilization utilization) {
    this.containersUtilization = utilization;
  }

  public ResourceUtilization getContainersAllocation() {
    return this.containersAllocation;
  }

  /**
   * @return true if there are available allocated resources for the given
   *         container to start.
   */
  @Override
  public boolean hasResourcesAvailable(ProcessTreeInfo pti) {
    synchronized (this.containersAllocation) {
      // Check physical memory.
      if (this.containersAllocation.getPhysicalMemory() +
          (int) (pti.getPmemLimit() >> 20) >
          (int) (getPmemAllocatedForContainers() >> 20)) {
        return false;
      }
      // Check virtual memory.
      if (isVmemCheckEnabled() &&
          this.containersAllocation.getVirtualMemory() +
          (int) (pti.getVmemLimit() >> 20) >
          (int) (getVmemAllocatedForContainers() >> 20)) {
        return false;
      }
      // Check CPU.
      if (this.containersAllocation.getCPU()
          + allocatedCpuUsage(pti) > 1.0f) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void increaseContainersAllocation(ProcessTreeInfo pti) {
    synchronized (this.containersAllocation) {
      increaseResourceUtilization(this.containersAllocation, pti);
    }
  }

  @Override
  public void decreaseContainersAllocation(ProcessTreeInfo pti) {
    synchronized (this.containersAllocation) {
      decreaseResourceUtilization(this.containersAllocation, pti);
    }
  }

  @Override
  public void increaseResourceUtilization(ResourceUtilization resourceUtil,
      ProcessTreeInfo pti) {
    resourceUtil.addTo((int) (pti.getPmemLimit() >> 20),
        (int) (pti.getVmemLimit() >> 20), allocatedCpuUsage(pti));
  }

  @Override
  public void decreaseResourceUtilization(ResourceUtilization resourceUtil,
      ProcessTreeInfo pti) {
    resourceUtil.subtractFrom((int) (pti.getPmemLimit() >> 20),
        (int) (pti.getVmemLimit() >> 20), allocatedCpuUsage(pti));
  }

  @Override
  public void subtractNodeResourcesFromResourceUtilization(
      ResourceUtilization resourceUtil) {
    resourceUtil.subtractFrom((int) (getPmemAllocatedForContainers() >> 20),
        (int) (getVmemAllocatedForContainers() >> 20), 1.0f);
  }

  /**
   * Calculates the vCores CPU usage that is assigned to the given
   * {@link ProcessTreeInfo}. In particular, it takes into account the number of
   * vCores that are allowed to be used by the NM and returns the CPU usage
   * as a normalized value between {@literal >=} 0 and {@literal <=} 1.
   */
  private float allocatedCpuUsage(ProcessTreeInfo pti) {
    return (float) pti.getCpuVcores() / getVCoresAllocatedForContainers();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void handle(ContainersMonitorEvent monitoringEvent) {
    ContainerId containerId = monitoringEvent.getContainerId();
    if (!containersMonitorEnabled) {
      if (monitoringEvent.getType() == ContainersMonitorEventType
          .CHANGE_MONITORING_CONTAINER_RESOURCE) {
        // Nothing to enforce. Update container resource immediately.
        ChangeMonitoringContainerResourceEvent changeEvent =
            (ChangeMonitoringContainerResourceEvent) monitoringEvent;
        changeContainerResource(containerId, changeEvent.getResource());
      }
      return;
    }

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

  protected void onChangeMonitoringContainerResource(
      ContainersMonitorEvent monitoringEvent, ContainerId containerId) {
    ChangeMonitoringContainerResourceEvent changeEvent =
        (ChangeMonitoringContainerResourceEvent) monitoringEvent;
    ProcessTreeInfo processTreeInfo = trackingContainers.get(containerId);
    if (processTreeInfo == null) {
      LOG.warn("Failed to track container "
          + containerId.toString()
          + ". It may have already completed.");
      return;
    }
    LOG.info("Changing resource-monitoring for " + containerId);
    updateContainerMetrics(monitoringEvent);
    long pmemLimit = changeEvent.getResource().getMemorySize() * 1024L * 1024L;
    long vmemLimit = (long) (pmemLimit * vmemRatio);
    int cpuVcores = changeEvent.getResource().getVirtualCores();
    processTreeInfo.setResourceLimit(pmemLimit, vmemLimit, cpuVcores);
    changeContainerResource(containerId, changeEvent.getResource());
  }

  protected void onStopMonitoringContainer(
      ContainersMonitorEvent monitoringEvent, ContainerId containerId) {
    LOG.info("Stopping resource-monitoring for " + containerId);
    updateContainerMetrics(monitoringEvent);
    trackingContainers.remove(containerId);
  }

  protected void onStartMonitoringContainer(
      ContainersMonitorEvent monitoringEvent, ContainerId containerId) {
    ContainerStartMonitoringEvent startEvent =
        (ContainerStartMonitoringEvent) monitoringEvent;
    LOG.info("Starting resource-monitoring for " + containerId);
    updateContainerMetrics(monitoringEvent);
    trackingContainers.put(containerId,
        new ProcessTreeInfo(containerId, null, null,
            startEvent.getVmemLimit(), startEvent.getPmemLimit(),
            startEvent.getCpuVcores()));
  }

}
