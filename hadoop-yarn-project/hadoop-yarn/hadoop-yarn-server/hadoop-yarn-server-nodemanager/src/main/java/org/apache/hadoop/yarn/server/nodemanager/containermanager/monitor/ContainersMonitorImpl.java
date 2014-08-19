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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;

import com.google.common.base.Preconditions;

public class ContainersMonitorImpl extends AbstractService implements
    ContainersMonitor {

  final static Log LOG = LogFactory
      .getLog(ContainersMonitorImpl.class);

  private long monitoringInterval;
  private MonitoringThread monitoringThread;

  final List<ContainerId> containersToBeRemoved;
  final Map<ContainerId, ProcessTreeInfo> containersToBeAdded;
  Map<ContainerId, ProcessTreeInfo> trackingContainers =
      new HashMap<ContainerId, ProcessTreeInfo>();

  final ContainerExecutor containerExecutor;
  private final Dispatcher eventDispatcher;
  private final Context context;
  private ResourceCalculatorPlugin resourceCalculatorPlugin;
  private Configuration conf;
  private Class<? extends ResourceCalculatorProcessTree> processTreeClass;

  private long maxVmemAllottedForContainers = UNKNOWN_MEMORY_LIMIT;
  private long maxPmemAllottedForContainers = UNKNOWN_MEMORY_LIMIT;

  private boolean pmemCheckEnabled;
  private boolean vmemCheckEnabled;

  private long maxVCoresAllottedForContainers;

  private static final long UNKNOWN_MEMORY_LIMIT = -1L;

  public ContainersMonitorImpl(ContainerExecutor exec,
      AsyncDispatcher dispatcher, Context context) {
    super("containers-monitor");

    this.containerExecutor = exec;
    this.eventDispatcher = dispatcher;
    this.context = context;

    this.containersToBeAdded = new HashMap<ContainerId, ProcessTreeInfo>();
    this.containersToBeRemoved = new ArrayList<ContainerId>();
    this.monitoringThread = new MonitoringThread();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.monitoringInterval =
        conf.getLong(YarnConfiguration.NM_CONTAINER_MON_INTERVAL_MS,
            YarnConfiguration.DEFAULT_NM_CONTAINER_MON_INTERVAL_MS);

    Class<? extends ResourceCalculatorPlugin> clazz =
        conf.getClass(YarnConfiguration.NM_CONTAINER_MON_RESOURCE_CALCULATOR, null,
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

    long configuredPMemForContainers = conf.getLong(
        YarnConfiguration.NM_PMEM_MB,
        YarnConfiguration.DEFAULT_NM_PMEM_MB) * 1024 * 1024l;

    long configuredVCoresForContainers = conf.getLong(
        YarnConfiguration.NM_VCORES,
        YarnConfiguration.DEFAULT_NM_VCORES);


    // Setting these irrespective of whether checks are enabled. Required in
    // the UI.
    // ///////// Physical memory configuration //////
    this.maxPmemAllottedForContainers = configuredPMemForContainers;
    this.maxVCoresAllottedForContainers = configuredVCoresForContainers;

    // ///////// Virtual memory configuration //////
    float vmemRatio = conf.getFloat(YarnConfiguration.NM_VMEM_PMEM_RATIO,
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
    if (this.isEnabled()) {
      this.monitoringThread.start();
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.isEnabled()) {
      this.monitoringThread.interrupt();
      try {
        this.monitoringThread.join();
      } catch (InterruptedException e) {
        ;
      }
    }
    super.serviceStop();
  }

  private static class ProcessTreeInfo {
    private ContainerId containerId;
    private String pid;
    private ResourceCalculatorProcessTree pTree;
    private long vmemLimit;
    private long pmemLimit;

    public ProcessTreeInfo(ContainerId containerId, String pid,
        ResourceCalculatorProcessTree pTree, long vmemLimit, long pmemLimit) {
      this.containerId = containerId;
      this.pid = pid;
      this.pTree = pTree;
      this.vmemLimit = vmemLimit;
      this.pmemLimit = pmemLimit;
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

    public long getVmemLimit() {
      return this.vmemLimit;
    }

    /**
     * @return Physical memory limit for the process tree in bytes
     */
    public long getPmemLimit() {
      return this.pmemLimit;
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
    long currentMemUsage = pTree.getCumulativeVmem();
    // as processes begin with an age 1, we want to see if there are processes
    // more than 1 iteration old.
    long curMemUsageOfAgedProcesses = pTree.getCumulativeVmem(1);
    return isProcessTreeOverLimit(containerId, currentMemUsage,
                                  curMemUsageOfAgedProcesses, limit);
  }

  private class MonitoringThread extends Thread {
    public MonitoringThread() {
      super("Container Monitor");
    }

    @Override
    public void run() {

      while (true) {

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

        // Add new containers
        synchronized (containersToBeAdded) {
          for (Entry<ContainerId, ProcessTreeInfo> entry : containersToBeAdded
              .entrySet()) {
            ContainerId containerId = entry.getKey();
            ProcessTreeInfo processTreeInfo = entry.getValue();
            LOG.info("Starting resource-monitoring for " + containerId);
            trackingContainers.put(containerId, processTreeInfo);
          }
          containersToBeAdded.clear();
        }

        // Remove finished containers
        synchronized (containersToBeRemoved) {
          for (ContainerId containerId : containersToBeRemoved) {
            trackingContainers.remove(containerId);
            LOG.info("Stopping resource-monitoring for " + containerId);
          }
          containersToBeRemoved.clear();
        }

        // Now do the monitoring for the trackingContainers
        // Check memory usage and kill any overflowing containers
        long vmemStillInUsage = 0;
        long pmemStillInUsage = 0;
        for (Iterator<Map.Entry<ContainerId, ProcessTreeInfo>> it =
            trackingContainers.entrySet().iterator(); it.hasNext();) {

          Map.Entry<ContainerId, ProcessTreeInfo> entry = it.next();
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
            long currentVmemUsage = pTree.getCumulativeVmem();
            long currentPmemUsage = pTree.getCumulativeRssmem();
            // as processes begin with an age 1, we want to see if there
            // are processes more than 1 iteration old.
            long curMemUsageOfAgedProcesses = pTree.getCumulativeVmem(1);
            long curRssMemUsageOfAgedProcesses = pTree.getCumulativeRssmem(1);
            long vmemLimit = ptInfo.getVmemLimit();
            long pmemLimit = ptInfo.getPmemLimit();
            LOG.info(String.format(
                "Memory usage of ProcessTree %s for container-id %s: ",
                     pId, containerId.toString()) +
                formatUsageString(currentVmemUsage, vmemLimit, currentPmemUsage, pmemLimit));

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
              it.remove();
              LOG.info("Removed ProcessTree with root " + pId);
            } else {
              // Accounting the total memory in usage for all containers that
              // are still
              // alive and within limits.
              vmemStillInUsage += currentVmemUsage;
              pmemStillInUsage += currentPmemUsage;
            }
          } catch (Exception e) {
            // Log the exception and proceed to the next container.
            LOG.warn("Uncaught exception in ContainerMemoryManager "
                + "while managing memory of " + containerId, e);
          }
        }

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
  public void handle(ContainersMonitorEvent monitoringEvent) {

    if (!isEnabled()) {
      return;
    }

    ContainerId containerId = monitoringEvent.getContainerId();
    switch (monitoringEvent.getType()) {
    case START_MONITORING_CONTAINER:
      ContainerStartMonitoringEvent startEvent =
          (ContainerStartMonitoringEvent) monitoringEvent;
      synchronized (this.containersToBeAdded) {
        ProcessTreeInfo processTreeInfo =
            new ProcessTreeInfo(containerId, null, null,
                startEvent.getVmemLimit(), startEvent.getPmemLimit());
        this.containersToBeAdded.put(containerId, processTreeInfo);
      }
      break;
    case STOP_MONITORING_CONTAINER:
      synchronized (this.containersToBeRemoved) {
        this.containersToBeRemoved.add(containerId);
      }
      break;
    default:
      // TODO: Wrong event.
    }
  }
}
