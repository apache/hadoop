/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PROCS_FILE;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_OOM_CONTROL;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_USAGE_BYTES;

/**
 * A very basic OOM handler implementation.
 * See the javadoc on the run() method for details.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DefaultOOMHandler implements Runnable {
  protected static final Logger LOG = LoggerFactory
      .getLogger(DefaultOOMHandler.class);
  private final Context context;
  private final String memoryStatFile;
  private final CGroupsHandler cgroups;

  /**
   * Create an OOM handler.
   * This has to be public to be able to construct through reflection.
   * @param context node manager context to work with
   * @param enforceVirtualMemory true if virtual memory needs to be checked,
   *                   false if physical memory needs to be checked instead
   */
  public DefaultOOMHandler(Context context, boolean enforceVirtualMemory) {
    this.context = context;
    this.memoryStatFile = enforceVirtualMemory ?
        CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES :
        CGROUP_PARAM_MEMORY_USAGE_BYTES;
    this.cgroups = getCGroupsHandler();
  }

  @VisibleForTesting
  protected CGroupsHandler getCGroupsHandler() {
    return ResourceHandlerModule.getCGroupsHandler();
  }

  /**
   * Check if a given container exceeds its limits.
   */
  private boolean isContainerOutOfLimit(Container container) {
    boolean outOfLimit = false;

    String value = null;
    try {
      value = cgroups.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
          container.getContainerId().toString(), memoryStatFile);
      long usage = Long.parseLong(value);
      long request = container.getResource().getMemorySize() * 1024 * 1024;

      // Check if the container has exceeded its limits.
      if (usage > request) {
        outOfLimit = true;
        String message = String.format(
            "Container %s is out of its limits, using %d " +
                "when requested only %d",
            container.getContainerId(), usage, request);
        LOG.warn(message);
      }
    } catch (ResourceHandlerException ex) {
      LOG.warn(String.format("Could not access memory resource for %s",
          container.getContainerId()), ex);
    } catch (NumberFormatException ex) {
      LOG.warn(String.format("Could not parse %s in %s", value,
          container.getContainerId()));
    }
    return outOfLimit;
  }

  /**
   * SIGKILL the specified container. We do this not using the standard
   * container logic. The reason is that the processes are frozen by
   * the cgroups OOM handler, so they cannot respond to SIGTERM.
   * On the other hand we have to be as fast as possible.
   * We walk through the list of active processes in the container.
   * This is needed because frozen parents cannot signal their children.
   * We kill each process and then try again until the whole cgroup
   * is cleaned up. This logic avoids leaking processes in a cgroup.
   * Currently the killing only succeeds for PGIDS.
   *
   * @param container Container to clean up
   * @return true if the container is killed successfully, false otherwise
   */
  private boolean sigKill(Container container) {
    boolean containerKilled = false;
    boolean finished = false;
    try {
      while (!finished) {
        String[] pids =
            cgroups.getCGroupParam(
                CGroupsHandler.CGroupController.MEMORY,
                container.getContainerId().toString(),
                CGROUP_PROCS_FILE)
                .split("\n");
        finished = true;
        for (String pid : pids) {
          // Note: this kills only PGIDs currently
          if (pid != null && !pid.isEmpty()) {
            LOG.debug(String.format(
                "Terminating container %s Sending SIGKILL to -%s",
                container.getContainerId().toString(),
                pid));
            finished = false;
            try {
              context.getContainerExecutor().signalContainer(
                  new ContainerSignalContext.Builder().setContainer(container)
                      .setUser(container.getUser())
                      .setPid(pid).setSignal(ContainerExecutor.Signal.KILL)
                      .build());
            } catch (IOException ex) {
              LOG.warn(String.format("Cannot kill container %s pid -%s.",
                  container.getContainerId(), pid), ex);
            }
          }
        }
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          LOG.debug("Interrupted while waiting for processes to disappear");
        }
      }
      containerKilled = true;
    } catch (ResourceHandlerException ex) {
      // the tasks file of the container may not be available because the
      // container may not have been launched at this point when the root
      // cgroup is under oom
      LOG.warn(String.format(
          "Cannot list more tasks in container %s to kill.",
          container.getContainerId()));
    }

    return containerKilled;
  }

  /**
   * It is called when the node is under an OOM condition. All processes in
   * all sub-cgroups are suspended. We need to act fast, so that we do not
   * affect the overall system utilization. In general we try to find a
   * newly launched container that exceeded its limits. The justification is
   * cost, since probably this is the one that has accumulated the least
   * amount of uncommitted data so far. OPPORTUNISTIC containers are always
   * killed before any GUARANTEED containers are considered.  We continue the
   * process until the OOM is resolved.
   */
  @Override
  public void run() {
    try {
      // We kill containers until the kernel reports the OOM situation resolved
      // Note: If the kernel has a delay this may kill more than necessary
      while (true) {
        String status = cgroups.getCGroupParam(
            CGroupsHandler.CGroupController.MEMORY,
            "",
            CGROUP_PARAM_MEMORY_OOM_CONTROL);
        if (!status.contains(CGroupsHandler.UNDER_OOM)) {
          break;
        }

        boolean containerKilled = killContainer();

        if (!containerKilled) {
          // This can happen, if SIGKILL did not clean up
          // non-PGID or containers or containers launched by other users
          // or if a process was put to the root YARN cgroup.
          throw new YarnRuntimeException(
              "Could not find any containers but CGroups " +
                  "reserved for containers ran out of memory. " +
                  "I am giving up");
        }
      }
    } catch (ResourceHandlerException ex) {
      LOG.warn("Could not fetch OOM status. " +
          "This is expected at shutdown. Exiting.", ex);
    }
  }

  /**
   * Choose and kill a container in case of OOM. We try to find the most
   * recently launched OPPORTUNISTIC container that exceeds its limit
   * and fall back to the most recently launched OPPORTUNISTIC container
   * If there is no such container found, we choose to kill a GUARANTEED
   * container in the same way.
   * @return true if a container is killed, false otherwise
   */
  protected boolean killContainer() {
    boolean containerKilled = false;

    ArrayList<ContainerCandidate> candidates = new ArrayList<>(0);
    for (Container container : context.getContainers().values()) {
      if (!container.isRunning()) {
        // skip containers that are not running yet because killing them
        // won't release any memory to get us out of OOM.
        continue;
        // note even if it is indicated that the container is running from
        // container.isRunning(), the container process might not have been
        // running yet. From NM's perspective, a container is running as
        // soon as the container launch is handed over the container executor
      }
      candidates.add(
          new ContainerCandidate(container, isContainerOutOfLimit(container)));
    }
    Collections.sort(candidates);
    if (candidates.isEmpty()) {
      LOG.warn(
          "Found no running containers to kill in order to release memory");
    }

    // make sure one container is killed successfully to release memory
    for(int i = 0; !containerKilled && i < candidates.size(); i++) {
      ContainerCandidate candidate = candidates.get(i);
      if (sigKill(candidate.container)) {
        String message = String.format(
            "container %s killed by elastic cgroups OOM handler.",
            candidate.container.getContainerId());
        LOG.warn(message);
        containerKilled = true;
      }
    }
    return containerKilled;
  }

  /**
   * Note: this class has a natural ordering that is inconsistent with equals.
   */
  private static class ContainerCandidate
      implements Comparable<ContainerCandidate> {
    private final boolean outOfLimit;
    final Container container;

    ContainerCandidate(Container container, boolean outOfLimit) {
      this.outOfLimit = outOfLimit;
      this.container = container;
    }

    /**
     * Order two containers by their execution type, followed by
     * their out-of-limit status and then launch time. Opportunistic
     * containers are ordered before Guaranteed containers. If two
     * containers are of the same execution type, the one that is
     * out of its limits is ordered before the one that isn't. If
     * two containers have the same execution type and out-of-limit
     * status, the one that's launched later is ordered before the
     * other one.
     */
    @Override
    public int compareTo(ContainerCandidate o) {
      boolean isThisOpportunistic = isOpportunistic(container);
      boolean isOtherOpportunistic = isOpportunistic(o.container);
      int ret = Boolean.compare(isOtherOpportunistic, isThisOpportunistic);
      if (ret == 0) {
        // the two containers are of the same execution type, order them
        // by their out-of-limit status.
        int outOfLimitRet = Boolean.compare(o.outOfLimit, outOfLimit);
        if (outOfLimitRet == 0) {
          // the two containers are also of the same out-of-limit status,
          // order them by their launch time
          ret = Long.compare(o.container.getContainerLaunchTime(),
              this.container.getContainerLaunchTime());
        } else {
          ret = outOfLimitRet;
        }
      }
      return ret;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (this.getClass() != obj.getClass()) {
        return false;
      }
      ContainerCandidate other = (ContainerCandidate) obj;
      if (this.outOfLimit != other.outOfLimit) {
        return false;
      }
      if (this.container == null) {
        return other.container == null;
      } else {
        return this.container.equals(other.container);
      }
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().append(container).append(outOfLimit)
          .toHashCode();
    }

    /**
     * Check if a container is OPPORTUNISTIC or not. A container is
     * considered OPPORTUNISTIC only if its execution type is not
     * null and is OPPORTUNISTIC.
     */
    private static boolean isOpportunistic(Container container) {
      return container.getContainerTokenIdentifier() != null &&
          ExecutionType.OPPORTUNISTIC.equals(
              container.getContainerTokenIdentifier().getExecutionType());
    }
  }
}
