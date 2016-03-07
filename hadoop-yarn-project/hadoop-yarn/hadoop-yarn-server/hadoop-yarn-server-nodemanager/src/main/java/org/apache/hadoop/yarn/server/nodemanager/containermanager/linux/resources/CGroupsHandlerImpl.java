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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Support for interacting with various CGroup subsystems. Thread-safe.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
class CGroupsHandlerImpl implements CGroupsHandler {

  private static final Log LOG = LogFactory.getLog(CGroupsHandlerImpl.class);
  private static final String MTAB_FILE = "/proc/mounts";
  private static final String CGROUPS_FSTYPE = "cgroup";

  private final String cGroupPrefix;
  private final boolean enableCGroupMount;
  private final String cGroupMountPath;
  private final long deleteCGroupTimeout;
  private final long deleteCGroupDelay;
  private Map<CGroupController, String> controllerPaths;
  private final ReadWriteLock rwLock;
  private final PrivilegedOperationExecutor privilegedOperationExecutor;
  private final Clock clock;

  public CGroupsHandlerImpl(Configuration conf, PrivilegedOperationExecutor
      privilegedOperationExecutor) throws ResourceHandlerException {
    this.cGroupPrefix = conf.get(YarnConfiguration.
        NM_LINUX_CONTAINER_CGROUPS_HIERARCHY, "/hadoop-yarn")
        .replaceAll("^/", "").replaceAll("$/", "");
    this.enableCGroupMount = conf.getBoolean(YarnConfiguration.
        NM_LINUX_CONTAINER_CGROUPS_MOUNT, false);
    this.cGroupMountPath = conf.get(YarnConfiguration.
        NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH, null);
    this.deleteCGroupTimeout = conf.getLong(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT,
        YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT);
    this.deleteCGroupDelay =
        conf.getLong(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_DELETE_DELAY,
            YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_DELAY);
    this.controllerPaths = new HashMap<>();
    this.rwLock = new ReentrantReadWriteLock();
    this.privilegedOperationExecutor = privilegedOperationExecutor;
    this.clock = SystemClock.getInstance();

    init();
  }

  private void init() throws ResourceHandlerException {
    initializeControllerPaths();
  }

  private String getControllerPath(CGroupController controller) {
    try {
      rwLock.readLock().lock();
      return controllerPaths.get(controller);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  private void initializeControllerPaths() throws ResourceHandlerException {
    if (enableCGroupMount) {
      // nothing to do here - we support 'deferred' mounting of specific
      // controllers - we'll populate the path for a given controller when an
      // explicit mountCGroupController request is issued.
      LOG.info("CGroup controller mounting enabled.");
    } else {
      // cluster admins are expected to have mounted controllers in specific
      // locations - we'll attempt to figure out mount points

      Map<CGroupController, String> cPaths =
          initializeControllerPathsFromMtab(MTAB_FILE, this.cGroupPrefix);
      // we want to do a bulk update without the paths changing concurrently
      try {
        rwLock.writeLock().lock();
        controllerPaths = cPaths;
      } finally {
        rwLock.writeLock().unlock();
      }
    }
  }

  @VisibleForTesting
  static Map<CGroupController, String> initializeControllerPathsFromMtab(
      String mtab, String cGroupPrefix) throws ResourceHandlerException {
    try {
      Map<String, List<String>> parsedMtab = parseMtab(mtab);
      Map<CGroupController, String> ret = new HashMap<>();

      for (CGroupController controller : CGroupController.values()) {
        String name = controller.getName();
        String controllerPath = findControllerInMtab(name, parsedMtab);

        if (controllerPath != null) {
          File f = new File(controllerPath + "/" + cGroupPrefix);

          if (FileUtil.canWrite(f)) {
            ret.put(controller, controllerPath);
          } else {
            String error =
                new StringBuffer("Mount point Based on mtab file: ")
                    .append(mtab)
                    .append(". Controller mount point not writable for: ")
                    .append(name).toString();

            LOG.error(error);
            throw new ResourceHandlerException(error);
          }
        } else {
          LOG.warn("Controller not mounted but automount disabled: " + name);
        }
      }
      return ret;
    } catch (IOException e) {
      LOG.warn("Failed to initialize controller paths! Exception: " + e);
      throw new ResourceHandlerException(
        "Failed to initialize controller paths!");
    }
  }

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
  private static Map<String, List<String>> parseMtab(String mtab)
      throws IOException {
    Map<String, List<String>> ret = new HashMap<String, List<String>>();
    BufferedReader in = null;

    try {
      FileInputStream fis = new FileInputStream(new File(mtab));
      in = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

      for (String str = in.readLine(); str != null;
           str = in.readLine()) {
        Matcher m = MTAB_FILE_FORMAT.matcher(str);
        boolean mat = m.find();
        if (mat) {
          String path = m.group(1);
          String type = m.group(2);
          String options = m.group(3);

          if (type.equals(CGROUPS_FSTYPE)) {
            List<String> value = Arrays.asList(options.split(","));
            ret.put(path, value);
          }
        }
      }
    } catch (IOException e) {
      throw new IOException("Error while reading " + mtab, e);
    } finally {
      IOUtils.cleanup(LOG, in);
    }

    return ret;
  }

  private static String findControllerInMtab(String controller,
      Map<String, List<String>> entries) {
    for (Map.Entry<String, List<String>> e : entries.entrySet()) {
      if (e.getValue().contains(controller))
        return e.getKey();
    }

    return null;
  }

  @Override
  public void mountCGroupController(CGroupController controller)
      throws ResourceHandlerException {
    if (!enableCGroupMount) {
      LOG.warn("CGroup mounting is disabled - ignoring mount request for: " +
          controller.getName());
      return;
    }

    String path = getControllerPath(controller);

    if (path == null) {
      try {
        //lock out other readers/writers till we are done
        rwLock.writeLock().lock();

        String hierarchy = cGroupPrefix;
        StringBuffer controllerPath = new StringBuffer()
            .append(cGroupMountPath).append('/').append(controller.getName());
        StringBuffer cGroupKV = new StringBuffer()
            .append(controller.getName()).append('=').append(controllerPath);
        PrivilegedOperation.OperationType opType = PrivilegedOperation
            .OperationType.MOUNT_CGROUPS;
        PrivilegedOperation op = new PrivilegedOperation(opType);

        op.appendArgs(hierarchy, cGroupKV.toString());
        LOG.info("Mounting controller " + controller.getName() + " at " +
              controllerPath);
        privilegedOperationExecutor.executePrivilegedOperation(op, false);

        //if privileged operation succeeds, update controller paths
        controllerPaths.put(controller, controllerPath.toString());

        return;
      } catch (PrivilegedOperationException e) {
        LOG.error("Failed to mount controller: " + controller.getName());
        throw new ResourceHandlerException("Failed to mount controller: "
            + controller.getName());
      } finally {
        rwLock.writeLock().unlock();
      }
    } else {
      LOG.info("CGroup controller already mounted at: " + path);
      return;
    }
  }

  @Override
  public String getRelativePathForCGroup(String cGroupId) {
    return new StringBuffer(cGroupPrefix).append("/")
        .append(cGroupId).toString();
  }

  @Override
  public String getPathForCGroup(CGroupController controller, String cGroupId) {
    return new StringBuffer(getControllerPath(controller))
        .append('/').append(cGroupPrefix).append("/")
        .append(cGroupId).toString();
  }

  @Override
  public String getPathForCGroupTasks(CGroupController controller,
      String cGroupId) {
    return new StringBuffer(getPathForCGroup(controller, cGroupId))
        .append('/').append(CGROUP_FILE_TASKS).toString();
  }

  @Override
  public String getPathForCGroupParam(CGroupController controller,
      String cGroupId, String param) {
    return new StringBuffer(getPathForCGroup(controller, cGroupId))
        .append('/').append(controller.getName()).append('.')
        .append(param).toString();
  }

  @Override
  public String createCGroup(CGroupController controller, String cGroupId)
      throws ResourceHandlerException {
    String path = getPathForCGroup(controller, cGroupId);

    if (LOG.isDebugEnabled()) {
      LOG.debug("createCgroup: " + path);
    }

    if (!new File(path).mkdir()) {
      throw new ResourceHandlerException("Failed to create cgroup at " + path);
    }

    return path;
  }

  /*
  * Utility routine to print first line from cgroup tasks file
  */
  private void logLineFromTasksFile(File cgf) {
    String str;
    if (LOG.isDebugEnabled()) {
      try (BufferedReader inl =
          new BufferedReader(new InputStreamReader(new FileInputStream(cgf
              + "/tasks"), "UTF-8"))) {
        if ((str = inl.readLine()) != null) {
          LOG.debug("First line in cgroup tasks file: " + cgf + " " + str);
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
  boolean checkAndDeleteCgroup(File cgf) throws InterruptedException {
    boolean deleted = false;
    // FileInputStream in = null;
    try (FileInputStream in = new FileInputStream(cgf + "/tasks")) {
      if (in.read() == -1) {
        /*
         * "tasks" file is empty, sleep a bit more and then try to delete the
         * cgroup. Some versions of linux will occasionally panic due to a race
         * condition in this area, hence the paranoia.
         */
        Thread.sleep(deleteCGroupDelay);
        deleted = cgf.delete();
        if (!deleted) {
          LOG.warn("Failed attempt to delete cgroup: " + cgf);
        }
      } else {
        logLineFromTasksFile(cgf);
      }
    } catch (IOException e) {
      LOG.warn("Failed to read cgroup tasks file. ", e);
    }
    return deleted;
  }

  @Override
  public void deleteCGroup(CGroupController controller, String cGroupId)
      throws ResourceHandlerException {
    boolean deleted = false;
    String cGroupPath = getPathForCGroup(controller, cGroupId);

    if (LOG.isDebugEnabled()) {
      LOG.debug("deleteCGroup: " + cGroupPath);
    }

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
      LOG.warn("Unable to delete  " + cGroupPath +
          ", tried to delete for " + deleteCGroupTimeout + "ms");
    }
  }

  @Override
  public void updateCGroupParam(CGroupController controller, String cGroupId,
      String param, String value) throws ResourceHandlerException {
    String cGroupParamPath = getPathForCGroupParam(controller, cGroupId, param);
    PrintWriter pw = null;

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "updateCGroupParam for path: " + cGroupParamPath + " with value " +
              value);
    }

    try {
      File file = new File(cGroupParamPath);
      Writer w = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
      pw = new PrintWriter(w);
      pw.write(value);
    } catch (IOException e) {
      throw new ResourceHandlerException(new StringBuffer("Unable to write to ")
          .append(cGroupParamPath).append(" with value: ").append(value)
          .toString(), e);
    } finally {
      if (pw != null) {
        boolean hasError = pw.checkError();
        pw.close();
        if (hasError) {
          throw new ResourceHandlerException(
              new StringBuffer("Unable to write to ")
                  .append(cGroupParamPath).append(" with value: ").append(value)
                  .toString());
        }
        if (pw.checkError()) {
          throw new ResourceHandlerException("Error while closing cgroup file" +
              " " + cGroupParamPath);
        }
      }
    }
  }

  @Override
  public String getCGroupParam(CGroupController controller, String cGroupId,
      String param) throws ResourceHandlerException {
    String cGroupParamPath = getPathForCGroupParam(controller, cGroupId, param);

    try {
      byte[] contents = Files.readAllBytes(Paths.get(cGroupParamPath));
      return new String(contents, "UTF-8").trim();
    } catch (IOException e) {
      throw new ResourceHandlerException(
          "Unable to read from " + cGroupParamPath);
    }
  }
}