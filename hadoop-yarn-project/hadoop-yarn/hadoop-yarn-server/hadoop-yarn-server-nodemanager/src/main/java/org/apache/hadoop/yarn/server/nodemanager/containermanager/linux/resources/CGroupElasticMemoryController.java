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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_ELASTIC_MEMORY_CONTROL_OOM_TIMEOUT_SEC;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_ENABLED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_OOM_TIMEOUT_SEC;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_PMEM_CHECK_ENABLED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_VMEM_CHECK_ENABLED;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_HARD_LIMIT_BYTES;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_OOM_CONTROL;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_SWAP_HARD_LIMIT_BYTES;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_NO_LIMIT;

/**
 * This thread controls memory usage using cgroups. It listens to out of memory
 * events of all the containers together, and if we go over the limit picks
 * a container to kill. The algorithm that picks the container is a plugin.
 */
public class CGroupElasticMemoryController extends Thread {
  protected static final Logger LOG = LoggerFactory
      .getLogger(CGroupElasticMemoryController.class);
  private final Clock clock = new MonotonicClock();
  private String yarnCGroupPath;
  private String oomListenerPath;
  private Runnable oomHandler;
  private CGroupsHandler cgroups;
  private boolean controlPhysicalMemory;
  private boolean controlVirtualMemory;
  private long limit;
  private Process process = null;
  private boolean stopped = false;
  private int timeoutMS;

  /**
   * Default constructor.
   * @param conf Yarn configuration to use
   * @param context Node manager context to out of memory handler
   * @param cgroups Cgroups handler configured
   * @param controlPhysicalMemory Whether to listen to physical memory OOM
   * @param controlVirtualMemory Whether to listen to virtual memory OOM
   * @param limit memory limit in bytes
   * @param oomHandlerOverride optional OOM handler
   * @exception YarnException Could not instantiate class
   */
  @VisibleForTesting
  CGroupElasticMemoryController(Configuration conf,
                                       Context context,
                                       CGroupsHandler cgroups,
                                       boolean controlPhysicalMemory,
                                       boolean controlVirtualMemory,
                                       long limit,
                                       Runnable oomHandlerOverride)
      throws YarnException {
    super("CGroupElasticMemoryController");
    boolean controlVirtual = controlVirtualMemory && !controlPhysicalMemory;
    Runnable oomHandlerTemp =
        getDefaultOOMHandler(conf, context, oomHandlerOverride, controlVirtual);
    if (controlPhysicalMemory && controlVirtualMemory) {
      LOG.warn(
          NM_ELASTIC_MEMORY_CONTROL_ENABLED + " is on. " +
          "We cannot control both virtual and physical " +
          "memory at the same time. Enforcing virtual memory. " +
          "If swapping is enabled set " +
          "only " + NM_PMEM_CHECK_ENABLED + " to true otherwise set " +
          "only " + NM_VMEM_CHECK_ENABLED + " to true.");
    }
    if (!controlPhysicalMemory && !controlVirtualMemory) {
      throw new YarnException(
          NM_ELASTIC_MEMORY_CONTROL_ENABLED + " is on. " +
              "We need either virtual or physical memory check requested. " +
              "If swapping is enabled set " +
              "only " + NM_PMEM_CHECK_ENABLED + " to true otherwise set " +
              "only " + NM_VMEM_CHECK_ENABLED + " to true.");
    }
    // We are safe at this point that no more exceptions can be thrown
    this.timeoutMS =
        1000 * conf.getInt(NM_ELASTIC_MEMORY_CONTROL_OOM_TIMEOUT_SEC,
        DEFAULT_NM_ELASTIC_MEMORY_CONTROL_OOM_TIMEOUT_SEC);
    this.oomListenerPath = getOOMListenerExecutablePath(conf);
    this.oomHandler = oomHandlerTemp;
    this.cgroups = cgroups;
    this.controlPhysicalMemory = !controlVirtual;
    this.controlVirtualMemory = controlVirtual;
    this.yarnCGroupPath = this.cgroups
        .getPathForCGroup(CGroupsHandler.CGroupController.MEMORY, "");
    this.limit = limit;
  }

  /**
   * Get the configured OOM handler.
   * @param conf configuration
   * @param context context to pass to constructor
   * @param oomHandlerLocal Default override
   * @param controlVirtual Control physical or virtual memory
   * @return The configured or overridden OOM handler.
   * @throws YarnException in case the constructor failed
   */
  private Runnable getDefaultOOMHandler(
      Configuration conf, Context context, Runnable oomHandlerLocal,
      boolean controlVirtual)
      throws YarnException {
    Class oomHandlerClass =
        conf.getClass(
            YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_OOM_HANDLER,
            DefaultOOMHandler.class);
    if (oomHandlerLocal == null) {
      try {
        Constructor constr = oomHandlerClass.getConstructor(
            Context.class, boolean.class);
        oomHandlerLocal = (Runnable)constr.newInstance(
            context, controlVirtual);
      } catch (Exception ex) {
        throw new YarnException(ex);
      }
    }
    return oomHandlerLocal;
  }

  /**
   * Default constructor.
   * @param conf Yarn configuration to use
   * @param context Node manager context to out of memory handler
   * @param cgroups Cgroups handler configured
   * @param controlPhysicalMemory Whether to listen to physical memory OOM
   * @param controlVirtualMemory Whether to listen to virtual memory OOM
   * @param limit memory limit in bytes
   * @exception YarnException Could not instantiate class
   */
  public CGroupElasticMemoryController(Configuration conf,
                                       Context context,
                                       CGroupsHandler cgroups,
                                       boolean controlPhysicalMemory,
                                       boolean controlVirtualMemory,
                                       long limit)
      throws YarnException {
    this(conf,
        context,
        cgroups,
        controlPhysicalMemory,
        controlVirtualMemory,
        limit,
        null);
  }

  /**
   * Exception thrown if the OOM situation is not resolved.
   */
  static private class OOMNotResolvedException extends YarnRuntimeException {
    OOMNotResolvedException(String message, Exception parent) {
      super(message, parent);
    }
  }

  /**
   * Stop listening to the cgroup.
   */
  public synchronized void stopListening() {
    stopped = true;
    if (process != null) {
      process.destroyForcibly();
    } else {
      LOG.warn("Trying to stop listening, when listening is not running");
    }
  }

  /**
   * Checks if the CGroupElasticMemoryController is available on this system.
   * This assumes that Linux container executor is already initialized.
   * We need to have CGroups enabled.
   *
   * @return True if CGroupElasticMemoryController is available.
   * False otherwise.
   */
  public static boolean isAvailable() {
    try {
      if (!Shell.LINUX) {
        LOG.info("CGroupElasticMemoryController currently is supported only "
            + "on Linux.");
        return false;
      }
      if (ResourceHandlerModule.getCGroupsHandler() == null ||
          ResourceHandlerModule.getMemoryResourceHandler() == null) {
        LOG.info("CGroupElasticMemoryController requires enabling " +
            "memory CGroups with" +
            YarnConfiguration.NM_MEMORY_RESOURCE_ENABLED);
        return false;
      }
    } catch (SecurityException se) {
      LOG.info("Failed to get Operating System name. " + se);
      return false;
    }
    return true;
  }

  /**
   * Main OOM listening thread. It uses an external process to listen to
   * Linux events. The external process does not need to run as root, so
   * it is not related to container-executor. We do not use JNI for security
   * reasons.
   */
  @Override
  public void run() {
    ExecutorService executor = null;
    try {
      // Disable OOM killer and set a limit.
      // This has to be set first, so that we get notified about valid events.
      // We will be notified about events even, if they happened before
      // oom-listener started
      setCGroupParameters();

      // Start a listener process
      ProcessBuilder oomListener = new ProcessBuilder();
      oomListener.command(oomListenerPath, yarnCGroupPath);
      synchronized (this) {
        if (!stopped) {
          process = oomListener.start();
        } else {
          resetCGroupParameters();
          LOG.info("Listener stopped before starting");
          return;
        }
      }
      LOG.info(String.format("Listening on %s with %s",
          yarnCGroupPath,
          oomListenerPath));

      // We need 1 thread for the error stream and a few others
      // as a watchdog for the OOM killer
      executor = Executors.newFixedThreadPool(2);

      // Listen to any errors in the background. We do not expect this to
      // be large in size, so it will fit into a string.
      Future<String> errorListener = executor.submit(
          () -> IOUtils.toString(process.getErrorStream(),
              Charset.defaultCharset()));

      // We get Linux event increments (8 bytes) forwarded from the event stream
      // The events cannot be split, so it is safe to read them as a whole
      // There is no race condition with the cgroup
      // running out of memory. If oom is 1 at startup
      // oom_listener will send an initial notification
      InputStream events = process.getInputStream();
      byte[] event = new byte[8];
      int read;
      // This loop can be exited by terminating the process
      // with stopListening()
      while ((read = events.read(event)) == event.length) {
        // An OOM event has occurred
        resolveOOM(executor);
      }

      if (read != -1) {
        LOG.warn(String.format("Characters returned from event hander: %d",
            read));
      }

      // If the input stream is closed, we wait for exit or process terminated.
      int exitCode = process.waitFor();
      String error = errorListener.get();
      process = null;
      LOG.info(String.format("OOM listener exited %d %s", exitCode, error));
    } catch (OOMNotResolvedException ex) {
      // We could mark the node unhealthy but it shuts down the node anyways.
      // Let's just bring down the node manager all containers are frozen.
      throw new YarnRuntimeException("Could not resolve OOM", ex);
    } catch (Exception ex) {
      synchronized (this) {
        if (!stopped) {
          LOG.warn("OOM Listener exiting.", ex);
        }
      }
    } finally {
      // Make sure we do not leak the child process,
      // especially if process.waitFor() did not finish.
      if (process != null && process.isAlive()) {
        process.destroyForcibly();
      }
      if (executor != null) {
        try {
          executor.awaitTermination(6, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          LOG.warn("Exiting without processing all OOM events.");
        }
        executor.shutdown();
      }
      resetCGroupParameters();
    }
  }

  /**
   * Resolve an OOM event.
   * Listen to the handler timeouts.
   * @param executor Executor to create watchdog with.
   * @throws InterruptedException interrupted
   * @throws java.util.concurrent.ExecutionException cannot launch watchdog
   */
  private void resolveOOM(ExecutorService executor)
      throws InterruptedException, java.util.concurrent.ExecutionException {
    // Just log, when we are still in OOM after a couple of seconds
    final long start = clock.getTime();
    Future<Boolean> watchdog =
        executor.submit(() -> watchAndLogOOMState(start));
    // Kill something to resolve the issue
    try {
      oomHandler.run();
    } catch (RuntimeException ex) {
      watchdog.cancel(true);
      throw new OOMNotResolvedException("OOM handler failed", ex);
    }
    if (!watchdog.get()) {
      // If we are still in OOM,
      // the watchdog will trigger stop
      // listening to exit this loop
      throw new OOMNotResolvedException("OOM handler timed out", null);
    }
  }

  /**
   * Just watch until we are in OOM and log. Send an update log every second.
   * @return if the OOM was resolved successfully
   */
  private boolean watchAndLogOOMState(long start) {
    long lastLog = start;
    try {
      long end = start;
      // Throw an error, if we are still in OOM after 5 seconds
      while(end - start < timeoutMS) {
        end = clock.getTime();
        String underOOM = cgroups.getCGroupParam(
            CGroupsHandler.CGroupController.MEMORY,
            "",
            CGROUP_PARAM_MEMORY_OOM_CONTROL);
        if (underOOM.contains(CGroupsHandler.UNDER_OOM)) {
          if (end - lastLog > 1000) {
            LOG.warn(String.format(
                "OOM not resolved in %d ms", end - start));
            lastLog = end;
          }
        } else {
          LOG.info(String.format(
              "Resolved OOM in %d ms", end - start));
          return true;
        }
        // We do not want to saturate the CPU
        // leaving the resources to the actual OOM killer
        // but we want to be fast, too.
        Thread.sleep(10);
      }
    } catch (InterruptedException ex) {
      LOG.debug("Watchdog interrupted");
    } catch (Exception e) {
      LOG.warn("Exception running logging thread", e);
    }
    LOG.warn(String.format("OOM was not resolved in %d ms",
        clock.getTime() - start));
    stopListening();
    return false;
  }

  /**
   * Update root memory cgroup. This contains all containers.
   * The physical limit has to be set first then the virtual limit.
   */
  private void setCGroupParameters() throws ResourceHandlerException {
    // Disable the OOM killer
    cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL, "1");
    if (controlPhysicalMemory && !controlVirtualMemory) {
      try {
        // Ignore virtual memory limits, since we do not know what it is set to
        cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
            CGROUP_PARAM_MEMORY_SWAP_HARD_LIMIT_BYTES, CGROUP_NO_LIMIT);
      } catch (ResourceHandlerException ex) {
        LOG.debug("Swap monitoring is turned off in the kernel");
      }
      // Set physical memory limits
      cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_PARAM_MEMORY_HARD_LIMIT_BYTES, Long.toString(limit));
    } else if (controlVirtualMemory && !controlPhysicalMemory) {
      // Ignore virtual memory limits, since we do not know what it is set to
      cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_PARAM_MEMORY_SWAP_HARD_LIMIT_BYTES, CGROUP_NO_LIMIT);
      // Set physical limits to no more than virtual limits
      cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_PARAM_MEMORY_HARD_LIMIT_BYTES, Long.toString(limit));
      // Set virtual memory limits
      // Important: it has to be set after physical limit is set
      cgroups.updateCGroupParam(CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_PARAM_MEMORY_SWAP_HARD_LIMIT_BYTES, Long.toString(limit));
    } else {
      throw new ResourceHandlerException(
          String.format("Unsupported scenario physical:%b virtual:%b",
              controlPhysicalMemory, controlVirtualMemory));
    }
  }

  /**
   * Reset root memory cgroup to OS defaults. This controls all containers.
   */
  private void resetCGroupParameters() {
    try {
      try {
        // Disable memory limits
        cgroups.updateCGroupParam(
            CGroupsHandler.CGroupController.MEMORY, "",
            CGROUP_PARAM_MEMORY_SWAP_HARD_LIMIT_BYTES, CGROUP_NO_LIMIT);
      } catch (ResourceHandlerException ex) {
        LOG.debug("Swap monitoring is turned off in the kernel");
      }
      cgroups.updateCGroupParam(
          CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_PARAM_MEMORY_HARD_LIMIT_BYTES, CGROUP_NO_LIMIT);
      // Enable the OOM killer
      cgroups.updateCGroupParam(
          CGroupsHandler.CGroupController.MEMORY, "",
          CGROUP_PARAM_MEMORY_OOM_CONTROL, "0");
    } catch (ResourceHandlerException ex) {
      LOG.warn("Error in cleanup", ex);
    }
  }

  private static String getOOMListenerExecutablePath(Configuration conf) {
    String yarnHomeEnvVar =
        System.getenv(ApplicationConstants.Environment.HADOOP_YARN_HOME.key());
    if (yarnHomeEnvVar == null) {
      yarnHomeEnvVar = ".";
    }
    File hadoopBin = new File(yarnHomeEnvVar, "bin");
    String defaultPath =
        new File(hadoopBin, "oom-listener").getAbsolutePath();
    final String path = conf.get(
        YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_OOM_LISTENER_PATH,
        defaultPath);
    LOG.debug(String.format("oom-listener path: %s %s", path, defaultPath));
    return path;
  }
}
