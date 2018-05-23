/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_FILE_TASKS;
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
  protected static final Log LOG = LogFactory
      .getLog(DefaultOOMHandler.class);
  private Context context;
  private boolean virtual;
  private CGroupsHandler cgroups;

  /**
   * Create an OOM handler.
   * This has to be public to be able to construct through reflection.
   * @param context node manager context to work with
   * @param testVirtual Test virtual memory or physical
   */
  public DefaultOOMHandler(Context context, boolean testVirtual) {
    this.context = context;
    this.virtual = testVirtual;
    this.cgroups = ResourceHandlerModule.getCGroupsHandler();
  }

  @VisibleForTesting
  void setCGroupsHandler(CGroupsHandler handler) {
    cgroups = handler;
  }

  /**
   * Kill the container, if it has exceeded its request.
   *
   * @param container Container to check
   * @param fileName  CGroup filename (physical or swap/virtual)
   * @return true, if the container was preempted
   */
  private boolean killContainerIfOOM(Container container, String fileName) {
    String value = null;
    try {
      value = cgroups.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
          container.getContainerId().toString(),
          fileName);
      long usage = Long.parseLong(value);
      long request = container.getResource().getMemorySize() * 1024 * 1024;

      // Check if the container has exceeded its limits.
      if (usage > request) {
        // Kill the container
        // We could call the regular cleanup but that sends a
        // SIGTERM first that cannot be handled by frozen processes.
        // Walk through the cgroup
        // tasks file and kill all processes in it
        sigKill(container);
        String message = String.format(
            "Container %s was killed by elastic cgroups OOM handler using %d " +
                "when requested only %d",
            container.getContainerId(), usage, request);
        LOG.warn(message);
        return true;
      }
    } catch (ResourceHandlerException ex) {
      LOG.warn(String.format("Could not access memory resource for %s",
          container.getContainerId()), ex);
    } catch (NumberFormatException ex) {
      LOG.warn(String.format("Could not parse %s in %s",
          value, container.getContainerId()));
    }
    return false;
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
   */
  private void sigKill(Container container) {
    boolean finished = false;
    try {
      while (!finished) {
        String[] pids =
            cgroups.getCGroupParam(
                CGroupsHandler.CGroupController.MEMORY,
                container.getContainerId().toString(),
                CGROUP_FILE_TASKS)
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
    } catch (ResourceHandlerException ex) {
      LOG.warn(String.format(
          "Cannot list more tasks in container %s to kill.",
          container.getContainerId()));
    }
  }

  /**
   * It is called when the node is under an OOM condition. All processes in
   * all sub-cgroups are suspended. We need to act fast, so that we do not
   * affect the overall system utilization.
   * In general we try to find a newly run container that exceeded its limits.
   * The justification is cost, since probably this is the one that has
   * accumulated the least amount of uncommitted data so far.
   * We continue the process until the OOM is resolved.
   */
  @Override
  public void run() {
    try {
      // Reverse order by start time
      Comparator<Container> comparator = (Container o1, Container o2) -> {
        long order = o1.getContainerStartTime() - o2.getContainerStartTime();
        return order > 0 ? -1 : order < 0 ? 1 : 0;
      };

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

        // The first pass kills a recent container
        // that uses more than its request
        ArrayList<Container> containers = new ArrayList<>();
        containers.addAll(context.getContainers().values());
        // Note: Sorting may take a long time with 10K+ containers
        // but it is acceptable now with low number of containers per node
        containers.sort(comparator);

        // Kill the latest container that exceeded its request
        boolean found = false;
        for (Container container : containers) {
          if (!virtual) {
            if (killContainerIfOOM(container,
                CGROUP_PARAM_MEMORY_USAGE_BYTES)) {
              found = true;
              break;
            }
          } else {
            if (killContainerIfOOM(container,
                CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES)) {
              found = true;
              break;
            }
          }
        }
        if (found) {
          continue;
        }

        // We have not found any containers that ran out of their limit,
        // so we will kill the latest one. This can happen, if all use
        // close to their request and one of them requests a big block
        // triggering the OOM freeze.
        // Currently there is no other way to identify the outstanding one.
        if (containers.size() > 0) {
          Container container = containers.get(0);
          sigKill(container);
          String message = String.format(
              "Newest container %s killed by elastic cgroups OOM handler using",
              container.getContainerId());
          LOG.warn(message);
          continue;
        }

        // This can happen, if SIGKILL did not clean up
        // non-PGID or containers or containers launched by other users
        // or if a process was put to the root YARN cgroup.
        throw new YarnRuntimeException(
            "Could not find any containers but CGroups " +
                "reserved for containers ran out of memory. " +
                "I am giving up");
      }
    } catch (ResourceHandlerException ex) {
      LOG.warn("Could not fecth OOM status. " +
          "This is expected at shutdown. Exiting.", ex);
    }
  }
}
