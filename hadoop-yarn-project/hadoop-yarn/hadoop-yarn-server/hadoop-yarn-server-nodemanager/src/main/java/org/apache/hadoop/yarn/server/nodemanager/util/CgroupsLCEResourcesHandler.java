/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;

public class CgroupsLCEResourcesHandler implements LCEResourcesHandler {

  final static Log LOG = LogFactory
      .getLog(CgroupsLCEResourcesHandler.class);

  private Configuration conf;
  private String cgroupPrefix;
  private boolean cgroupMount;
  private String cgroupMountPath;

  private boolean cpuWeightEnabled = true;

  private final String MTAB_FILE = "/proc/mounts";
  private final String CGROUPS_FSTYPE = "cgroup";
  private final String CONTROLLER_CPU = "cpu";
  private final int CPU_DEFAULT_WEIGHT = 1024; // set by kernel
  private final Map<String, String> controllerPaths; // Controller -> path

  public CgroupsLCEResourcesHandler() {
    this.controllerPaths = new HashMap<String, String>();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  public synchronized void init(LinuxContainerExecutor lce) throws IOException {

    this.cgroupPrefix = conf.get(YarnConfiguration.
            NM_LINUX_CONTAINER_CGROUPS_HIERARCHY, "/hadoop-yarn");
    this.cgroupMount = conf.getBoolean(YarnConfiguration.
            NM_LINUX_CONTAINER_CGROUPS_MOUNT, false);
    this.cgroupMountPath = conf.get(YarnConfiguration.
            NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH, null);

    // remove extra /'s at end or start of cgroupPrefix
    if (cgroupPrefix.charAt(0) == '/') {
      cgroupPrefix = cgroupPrefix.substring(1);
    }

    int len = cgroupPrefix.length();
    if (cgroupPrefix.charAt(len - 1) == '/') {
      cgroupPrefix = cgroupPrefix.substring(0, len - 1);
    }
  
    // mount cgroups if requested
    if (cgroupMount && cgroupMountPath != null) {
      ArrayList<String> cgroupKVs = new ArrayList<String>();
      cgroupKVs.add(CONTROLLER_CPU + "=" + cgroupMountPath + "/" +
                    CONTROLLER_CPU);
      lce.mountCgroups(cgroupKVs, cgroupPrefix);
    }

    initializeControllerPaths();
  }


  boolean isCpuWeightEnabled() {
    return this.cpuWeightEnabled;
  }

  /*
   * Next four functions are for an individual cgroup.
   */

  private String pathForCgroup(String controller, String groupName) {
    String controllerPath = controllerPaths.get(controller);
    return controllerPath + "/" + cgroupPrefix + "/" + groupName;
  }

  private void createCgroup(String controller, String groupName)
        throws IOException {
    String path = pathForCgroup(controller, groupName);

    if (LOG.isDebugEnabled()) {
      LOG.debug("createCgroup: " + path);
    }

    if (! new File(path).mkdir()) {
      throw new IOException("Failed to create cgroup at " + path);
    }
  }

  private void updateCgroup(String controller, String groupName, String param,
                            String value) throws IOException {
    FileWriter f = null;
    String path = pathForCgroup(controller, groupName);
    param = controller + "." + param;

    if (LOG.isDebugEnabled()) {
      LOG.debug("updateCgroup: " + path + ": " + param + "=" + value);
    }

    try {
      f = new FileWriter(path + "/" + param, false);
      f.write(value);
    } catch (IOException e) {
      throw new IOException("Unable to set " + param + "=" + value +
          " for cgroup at: " + path, e);
    } finally {
      if (f != null) {
        try {
          f.close();
        } catch (IOException e) {
          LOG.warn("Unable to close cgroup file: " +
              path, e);
        }
      }
    }
  }

  private void deleteCgroup(String controller, String groupName) {
    String path = pathForCgroup(controller, groupName);

    LOG.debug("deleteCgroup: " + path);

    if (! new File(path).delete()) {
      LOG.warn("Unable to delete cgroup at: " + path);
    }
  }

  /*
   * Next three functions operate on all the resources we are enforcing.
   */

  /*
   * TODO: After YARN-2 is committed, we should call containerResource.getCpus()
   * (or equivalent) to multiply the weight by the number of requested cpus.
   */
  private void setupLimits(ContainerId containerId,
                           Resource containerResource) throws IOException {
    String containerName = containerId.toString();

    if (isCpuWeightEnabled()) {
      createCgroup(CONTROLLER_CPU, containerName);
      updateCgroup(CONTROLLER_CPU, containerName, "shares",
          String.valueOf(CPU_DEFAULT_WEIGHT));
    }
  }

  private void clearLimits(ContainerId containerId) {
    String containerName = containerId.toString();

    // Based on testing, ApplicationMaster executables don't terminate until
    // a little after the container appears to have finished. Therefore, we
    // wait a short bit for the cgroup to become empty before deleting it.
    if (containerId.getId() == 1) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        // not a problem, continue anyway
      }
    }

    if (isCpuWeightEnabled()) {
      deleteCgroup(CONTROLLER_CPU, containerName);
    }
  }

  /*
   * LCE Resources Handler interface
   */

  public void preExecute(ContainerId containerId, Resource containerResource)
              throws IOException {
    setupLimits(containerId, containerResource);
  }

  public void postExecute(ContainerId containerId) {
    clearLimits(containerId);
  }

  public String getResourcesOption(ContainerId containerId) {
    String containerName = containerId.toString();

    StringBuilder sb = new StringBuilder("cgroups=");

    if (isCpuWeightEnabled()) {
      sb.append(pathForCgroup(CONTROLLER_CPU, containerName) + "/cgroup.procs");
      sb.append(",");
    }

    if (sb.charAt(sb.length() - 1) == ',') {
      sb.deleteCharAt(sb.length() - 1);
    }

    return sb.toString();
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
  private Map<String, List<String>> parseMtab() throws IOException {
    Map<String, List<String>> ret = new HashMap<String, List<String>>();
    BufferedReader in = null;

    try {
      in = new BufferedReader(new FileReader(new File(MTAB_FILE)));

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
      throw new IOException("Error while reading " + MTAB_FILE, e);
    } finally {
      // Close the streams
      try {
        in.close();
      } catch (IOException e2) {
        LOG.warn("Error closing the stream: " + MTAB_FILE, e2);
      }
    }

    return ret;
  }

  private String findControllerInMtab(String controller,
                                      Map<String, List<String>> entries) {
    for (Entry<String, List<String>> e : entries.entrySet()) {
      if (e.getValue().contains(controller))
        return e.getKey();
    }

    return null;
  }

  private void initializeControllerPaths() throws IOException {
    String controllerPath;
    Map<String, List<String>> parsedMtab = parseMtab();

    // CPU

    controllerPath = findControllerInMtab(CONTROLLER_CPU, parsedMtab);

    if (controllerPath != null) {
      File f = new File(controllerPath + "/" + this.cgroupPrefix);

      if (f.canWrite()) {
        controllerPaths.put(CONTROLLER_CPU, controllerPath);
      } else {
        throw new IOException("Not able to enforce cpu weights; cannot write "
            + "to cgroup at: " + controllerPath);
      }
    } else {
      throw new IOException("Not able to enforce cpu weights; cannot find "
          + "cgroup for cpu controller in " + MTAB_FILE);
    }
  }
}
