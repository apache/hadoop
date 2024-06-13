/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractCGroupsHandler implements CGroupsHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractCGroupsHandler.class);
  protected static final String MTAB_FILE = "/proc/mounts";

  private final long deleteCGroupTimeout;
  private final long deleteCGroupDelay;
  private final Clock clock;

  protected final String mtabFile;
  protected final CGroupsMountConfig cGroupsMountConfig;
  protected final ReadWriteLock rwLock;
  protected Map<CGroupController, String> controllerPaths;
  protected Map<String, Set<String>> parsedMtab;
  protected final PrivilegedOperationExecutor privilegedOperationExecutor;
  protected final String cGroupPrefix;

  /**
   * Create cgroup handler object.
   *
   * @param conf                        configuration
   * @param privilegedOperationExecutor provides mechanisms to execute
   *                                    PrivilegedContainerOperations
   * @param mtab                        mount file location
   * @throws ResourceHandlerException if initialization failed
   */
  AbstractCGroupsHandler(Configuration conf, PrivilegedOperationExecutor
      privilegedOperationExecutor, String mtab)
      throws ResourceHandlerException {
    // Remove leading and trialing slash(es)
    this.cGroupPrefix = conf.get(YarnConfiguration.
            NM_LINUX_CONTAINER_CGROUPS_HIERARCHY, "/hadoop-yarn")
        .replaceAll("^/+", "").replaceAll("/+$", "");
    this.cGroupsMountConfig = new CGroupsMountConfig(conf);
    this.deleteCGroupTimeout = conf.getLong(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT,
        YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT) +
        conf.getLong(YarnConfiguration.NM_SLEEP_DELAY_BEFORE_SIGKILL_MS,
            YarnConfiguration.DEFAULT_NM_SLEEP_DELAY_BEFORE_SIGKILL_MS) + 1000;
    this.deleteCGroupDelay =
        conf.getLong(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_DELETE_DELAY,
            YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_DELAY);
    this.controllerPaths = new HashMap<>();
    this.parsedMtab = new HashMap<>();
    this.rwLock = new ReentrantReadWriteLock();
    this.privilegedOperationExecutor = privilegedOperationExecutor;
    this.clock = SystemClock.getInstance();
    mtabFile = mtab;
    init();
  }

  protected void init() throws ResourceHandlerException {
    initializeControllerPaths();
  }

  @Override
  public String getControllerPath(CGroupController controller) {
    rwLock.readLock().lock();
    try {
      return controllerPaths.get(controller);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  private void initializeControllerPaths() throws ResourceHandlerException {
    // Cluster admins may have some subsystems mounted in specific locations
    // We'll attempt to figure out mount points. We do this even if we plan
    // to mount cgroups into our own tree to control the path permissions or
    // to mount subsystems that are not mounted previously.
    // The subsystems for new and existing mount points have to match, and
    // the same hierarchy will be mounted at each mount point with the same
    // subsystem set.

    Map<String, Set<String>> newMtab = null;
    Map<CGroupController, String> cPaths;
    try {
      if (this.cGroupsMountConfig.mountDisabledButMountPathDefined()) {
        newMtab = parsePreConfiguredMountPath();
      }

      if (newMtab == null) {
        // parse mtab
        newMtab = parseMtab(mtabFile);
      }

      // find cgroup controller paths
      cPaths = initializeControllerPathsFromMtab(newMtab);
    } catch (IOException e) {
      LOG.warn("Failed to initialize controller paths! Exception: ", e);
      throw new ResourceHandlerException(
          "Failed to initialize controller paths!");
    }

    // we want to do a bulk update without the paths changing concurrently
    rwLock.writeLock().lock();
    try {
      controllerPaths = cPaths;
      parsedMtab = newMtab;
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  protected abstract Map<String, Set<String>> parsePreConfiguredMountPath() throws IOException;

  protected Map<CGroupController, String> initializeControllerPathsFromMtab(
      Map<String, Set<String>> mtab) {
    Map<CGroupController, String> ret = new HashMap<>();

    for (CGroupController controller : getCGroupControllers()) {
      String subsystemName = controller.getName();
      String controllerPath = findControllerInMtab(subsystemName, mtab);

      if (controllerPath != null) {
        ret.put(controller, controllerPath);
      }
    }
    return ret;
  }

  protected abstract List<CGroupController> getCGroupControllers();

  /* We are looking for entries of the form:
   * none /cgroup/path/mem cgroup rw,memory 0 0
   *
   * Use a simple pattern that splits on the five spaces, and
   * grabs the 2, 3, and 4th fields.
   */

  private static final Pattern MTAB_FILE_FORMAT = Pattern.compile(
      "^[^\\s]+\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s[^\\s]+\\s[^\\s]+$");

  /*
   * Returns a map: path -> mount options
   * for mounts with type "cgroup". Cgroup controllers will
   * appear in the list of options for a path.
   */
  protected Map<String, Set<String>> parseMtab(String mtab)
      throws IOException {
    Map<String, Set<String>> ret = new HashMap<>();
    BufferedReader in = null;

    try {
      FileInputStream fis = new FileInputStream(mtab);
      in = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));

      for (String str = in.readLine(); str != null;
           str = in.readLine()) {
        Matcher m = MTAB_FILE_FORMAT.matcher(str);
        boolean mat = m.find();
        if (mat) {
          String path = m.group(1);
          String type = m.group(2);
          String options = m.group(3);

          Set<String> controllerSet = handleMtabEntry(path, type, options);
          if (controllerSet != null) {
            ret.put(path, controllerSet);
          }
        }
      }
    } catch (IOException e) {
      if (Shell.LINUX) {
        throw new IOException("Error while reading " + mtab, e);
      } else {
        // Ignore the error, if we are running on an os other than Linux
        LOG.warn("Error while reading " + mtab, e);
      }
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }

    return ret;
  }

  protected abstract Set<String> handleMtabEntry(String path, String type, String options)
      throws IOException;

  /**
   * Find the hierarchy of the subsystem.
   * The kernel ensures that a subsystem can only be part of a single hierarchy.
   * The subsystem can be part of multiple mount points, if they belong to the
   * same hierarchy.
   *
   * @param controller subsystem like cpu, cpuset, etc...
   * @param entries    map of paths to mount options
   * @return the first mount path that has the requested subsystem
   */
  protected String findControllerInMtab(String controller,
                                        Map<String, Set<String>> entries) {
    for (Map.Entry<String, Set<String>> e : entries.entrySet()) {
      if (e.getValue().contains(controller)) {
        if (new File(e.getKey()).canRead()) {
          return e.getKey();
        } else {
          LOG.warn(String.format(
              "Skipping inaccessible cgroup mount point %s", e.getKey()));
        }
      }
    }

    return null;
  }

  protected abstract void mountCGroupController(CGroupController controller)
      throws ResourceHandlerException;

  @Override
  public String getRelativePathForCGroup(String cGroupId) {
    return cGroupPrefix + Path.SEPARATOR + cGroupId;
  }

  @Override
  public String getPathForCGroup(CGroupController controller, String cGroupId) {
    return getControllerPath(controller) + Path.SEPARATOR + cGroupPrefix
        + Path.SEPARATOR + cGroupId;
  }

  @Override
  public String getPathForCGroupTasks(CGroupController controller,
                                      String cGroupId) {
    return getPathForCGroup(controller, cGroupId)
        + Path.SEPARATOR + CGROUP_PROCS_FILE;
  }

  @Override
  public String getPathForCGroupParam(CGroupController controller,
                                      String cGroupId, String param) {
    return getPathForCGroup(controller, cGroupId)
        + Path.SEPARATOR + controller.getName()
        + "." + param;
  }

  /**
   * Mount cgroup or use existing mount point based on configuration.
   *
   * @param controller - the controller being initialized
   * @throws ResourceHandlerException yarn hierarchy cannot be created or
   *                                  accessed for any reason
   */
  @Override
  public void initializeCGroupController(CGroupController controller) throws
      ResourceHandlerException {
    if (this.cGroupsMountConfig.isMountEnabled() &&
        cGroupsMountConfig.ensureMountPathIsDefined()) {
      // We have a controller that needs to be mounted
      mountCGroupController(controller);
    }

    // We are working with a pre-mounted contoller
    // Make sure that YARN cgroup hierarchy path exists
    initializePreMountedCGroupController(controller);
  }

  /**
   * This function is called when the administrator opted
   * to use a pre-mounted cgroup controller.
   * There are two options.
   * 1. YARN hierarchy already exists. We verify, whether we have write access
   * in this case.
   * 2. YARN hierarchy does not exist, yet. We create it in this case. If cgroup v2 is used
   * an additional step is required to update the cgroup.subtree_control file, see
   * {@link CGroupsV2HandlerImpl#updateEnabledControllersInHierarchy}
   *
   * @param controller the controller being initialized
   * @throws ResourceHandlerException yarn hierarchy cannot be created or
   *                                  accessed for any reason
   */
  private void initializePreMountedCGroupController(CGroupController controller)
      throws ResourceHandlerException {
    // Check permissions to cgroup hierarchy and
    // create YARN cgroup if it does not exist, yet
    String controllerPath = getControllerPath(controller);

    if (controllerPath == null) {
      throw new ResourceHandlerException(
          String.format("Controller %s not mounted."
                  + " You either need to mount it with %s"
                  + " or mount cgroups before launching Yarn",
              controller.getName(), YarnConfiguration.
                  NM_LINUX_CONTAINER_CGROUPS_MOUNT));
    }

    File rootHierarchy = new File(controllerPath);
    File yarnHierarchy = new File(rootHierarchy, cGroupPrefix);
    String subsystemName = controller.getName();

    LOG.info("Initializing mounted controller " + controller.getName() + " " +
        "at " + yarnHierarchy);

    if (!rootHierarchy.exists()) {
      throw new ResourceHandlerException(getErrorWithDetails(
          "Cgroups mount point does not exist or not accessible",
          subsystemName,
          rootHierarchy.getAbsolutePath()
      ));
    } else if (!yarnHierarchy.exists()) {
      LOG.info("Yarn control group does not exist. Creating " +
          yarnHierarchy.getAbsolutePath());
      try {
        if (yarnHierarchy.mkdir()) {
          updateEnabledControllersInHierarchy(rootHierarchy, controller);
        } else {
          // Unexpected: we just checked that it was missing
          throw new ResourceHandlerException(getErrorWithDetails(
              "Unexpected: Cannot create yarn cgroup hierarchy",
              subsystemName,
              yarnHierarchy.getAbsolutePath()
          ));
        }
      } catch (SecurityException e) {
        throw new ResourceHandlerException(getErrorWithDetails(
            "No permissions to create yarn cgroup hierarchy",
            subsystemName,
            yarnHierarchy.getAbsolutePath()
        ), e);
      }
    } else if (!FileUtil.canWrite(yarnHierarchy)) {
      throw new ResourceHandlerException(getErrorWithDetails(
          "Yarn control group not writable",
          subsystemName,
          yarnHierarchy.getAbsolutePath()
      ));
    }

    updateEnabledControllersInHierarchy(yarnHierarchy, controller);
  }

  protected abstract void updateEnabledControllersInHierarchy(
      File yarnHierarchy, CGroupController controller)
      throws ResourceHandlerException;

  /**
   * Creates an actionable error message for mtab parsing.
   *
   * @param errorMessage   message to use
   * @param subsystemName  cgroup subsystem
   * @param yarnCgroupPath cgroup path that failed
   * @return a string builder that can be appended by the caller
   */
  protected String getErrorWithDetails(
      String errorMessage,
      String subsystemName,
      String yarnCgroupPath) {
    return String.format("%s Subsystem:%s Mount points:%s User:%s Path:%s ",
        errorMessage, subsystemName, mtabFile, System.getProperty("user.name"),
        yarnCgroupPath);
  }

  @Override
  public String createCGroup(CGroupController controller, String cGroupId)
      throws ResourceHandlerException {
    String path = getPathForCGroup(controller, cGroupId);
    File cgroup = new File(path);
    LOG.debug("createCgroup: {}", path);

    if (!cgroup.exists() && !cgroup.mkdir()) {
      throw new ResourceHandlerException("Failed to create cgroup at " + path);
    }

    return path;
  }

  /*
   * Utility routine to print first line from cgroup.procs file
   */
  private void logLineFromProcsFile(File cgf) {
    String str;
    if (LOG.isDebugEnabled()) {
      try (BufferedReader inl =
               new BufferedReader(new InputStreamReader(
                   Files.newInputStream(Paths.get(cgf + Path.SEPARATOR + CGROUP_PROCS_FILE)),
                   StandardCharsets.UTF_8))) {
        str = inl.readLine();
        if (str != null) {
          LOG.debug("First line in cgroup tasks file: {} {}", cgf, str);
        }
      } catch (IOException e) {
        LOG.warn("Failed to read cgroup tasks file. ", e);
      }
    }
  }

  /**
   * If tasks file is empty, delete the cgroup.
   *
   * @param cgf object referring to the cgroup to be deleted
   * @return Boolean indicating whether cgroup was deleted
   */
  private boolean checkAndDeleteCgroup(File cgf) throws InterruptedException {
    boolean deleted = false;
    // FileInputStream in = null;
    if (cgf.exists()) {
      try (FileInputStream in = new FileInputStream(cgf + Path.SEPARATOR + CGROUP_PROCS_FILE)) {
        if (in.read() == -1) {
          /*
           * "cgroup.procs" file is empty, sleep a bit more and then try to delete the
           * cgroup. Some versions of linux will occasionally panic due to a race
           * condition in this area, hence the paranoia.
           */
          Thread.sleep(deleteCGroupDelay);
          deleted = cgf.delete();
          if (!deleted) {
            LOG.warn("Failed attempt to delete cgroup: " + cgf);
          }
        } else {
          logLineFromProcsFile(cgf);
        }
      } catch (IOException e) {
        LOG.warn("Failed to read cgroup tasks file. ", e);
      }
    } else {
      LOG.info("Parent Cgroups directory {} does not exist. Skipping "
          + "deletion", cgf.getPath());
      deleted = true;
    }
    return deleted;
  }

  @Override
  public void deleteCGroup(CGroupController controller, String cGroupId)
      throws ResourceHandlerException {
    boolean deleted = false;
    String cGroupPath = getPathForCGroup(controller, cGroupId);

    LOG.debug("deleteCGroup: {}", cGroupPath);

    long start = clock.getTime();

    do {
      try {
        deleted = checkAndDeleteCgroup(new File(cGroupPath));
        if (!deleted) {
          Thread.sleep(deleteCGroupDelay);
        }
      } catch (InterruptedException ex) {
        // NOP
      }
    } while (!deleted && (clock.getTime() - start) < deleteCGroupTimeout);

    if (!deleted) {
      LOG.warn(String.format("Unable to delete  %s, tried to delete for %d ms",
          cGroupPath, deleteCGroupTimeout));
    }
  }

  @Override
  public void updateCGroupParam(CGroupController controller, String cGroupId,
                                String param, String value) throws ResourceHandlerException {
    String cGroupParamPath = getPathForCGroupParam(controller, cGroupId, param);
    PrintWriter pw = null;

    LOG.debug("updateCGroupParam for path: {} with value {}",
        cGroupParamPath, value);

    try {
      File file = new File(cGroupParamPath);
      Writer w = new OutputStreamWriter(Files.newOutputStream(file.toPath()),
          StandardCharsets.UTF_8);
      pw = new PrintWriter(w);
      pw.write(value);
    } catch (IOException e) {
      throw new ResourceHandlerException(
          String.format("Unable to write to %s with value: %s",
              cGroupParamPath, value), e);
    } finally {
      if (pw != null) {
        boolean hasError = pw.checkError();
        pw.close();
        if (hasError) {
          throw new ResourceHandlerException(
              String.format("PrintWriter unable to write to %s with value: %s",
                  cGroupParamPath, value));
        }
        if (pw.checkError()) {
          throw new ResourceHandlerException(
              String.format("Error while closing cgroup file %s",
                  cGroupParamPath));
        }
      }
    }
  }

  @Override
  public String getCGroupParam(CGroupController controller, String cGroupId,
                               String param) throws ResourceHandlerException {
    String cGroupParamPath =
        param.equals(CGROUP_PROCS_FILE) ?
            getPathForCGroup(controller, cGroupId)
                + Path.SEPARATOR + param :
            getPathForCGroupParam(controller, cGroupId, param);

    try {
      byte[] contents = Files.readAllBytes(Paths.get(cGroupParamPath));
      return new String(contents, StandardCharsets.UTF_8).trim();
    } catch (IOException e) {
      throw new ResourceHandlerException(
          "Unable to read from " + cGroupParamPath);
    }
  }

  @Override
  public String getCGroupMountPath() {
    return this.cGroupsMountConfig.getMountPath();
  }

  @Override
  public String getCGroupV2MountPath() {
    return this.cGroupsMountConfig.getV2MountPath();
  }

  @Override
  public String toString() {
    return CGroupsHandlerImpl.class.getName() + "{" +
        "mtabFile='" + mtabFile + '\'' +
        ", cGroupPrefix='" + cGroupPrefix + '\'' +
        ", cGroupsMountConfig=" + cGroupsMountConfig +
        ", deleteCGroupTimeout=" + deleteCGroupTimeout +
        ", deleteCGroupDelay=" + deleteCGroupDelay +
        '}';
  }
}
