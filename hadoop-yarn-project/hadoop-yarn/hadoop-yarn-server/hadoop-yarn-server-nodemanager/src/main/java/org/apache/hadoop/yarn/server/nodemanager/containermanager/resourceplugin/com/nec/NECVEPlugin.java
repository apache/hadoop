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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.com.nec;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.CommandExecutor;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePluginScheduler;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRegisterRequest;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRuntimeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.YarnRuntimeType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A device framework plugin which supports NEC Vector Engine.
 *
 */
public class NECVEPlugin implements DevicePlugin, DevicePluginScheduler {
  private static final String HADOOP_COMMON_HOME = "HADOOP_COMMON_HOME";
  private static final String ENV_SCRIPT_PATH = "NEC_VE_GET_SCRIPT_PATH";
  private static final String ENV_SCRIPT_NAME = "NEC_VE_GET_SCRIPT_NAME";
  private static final String ENV_USE_UDEV = "NEC_USE_UDEV";
  private static final String DEFAULT_SCRIPT_NAME = "nec-ve-get.py";
  private static final Logger LOG = LoggerFactory.getLogger(NECVEPlugin.class);
  private static final String[] DEFAULT_BINARY_SEARCH_DIRS = new String[]{
      "/usr/bin", "/bin", "/opt/nec/ve/bin"};

  private String binaryPath;
  private boolean useUdev;
  private VEDeviceDiscoverer discoverer;

  private Function<String[], CommandExecutor>
      commandExecutorProvider = this::createCommandExecutor;

  public NECVEPlugin() throws ResourceHandlerException {
    this(System::getenv, DEFAULT_BINARY_SEARCH_DIRS, new UdevUtil());
  }

  @VisibleForTesting
  NECVEPlugin(Function<String, String> envProvider, String[] scriptPaths,
      UdevUtil udev) throws ResourceHandlerException {
    if (Boolean.parseBoolean(envProvider.apply(ENV_USE_UDEV))) {
      LOG.info("Using libudev to retrieve syspath & device status");
      useUdev = true;
      udev.init();
      discoverer = new VEDeviceDiscoverer(udev);
    } else {
      scriptBasedInit(envProvider, scriptPaths);
    }
  }

  private void scriptBasedInit(Function<String, String> envProvider,
      String[] scriptPaths) throws ResourceHandlerException {
    String binaryName = DEFAULT_SCRIPT_NAME;

    String envScriptName = envProvider.apply(ENV_SCRIPT_NAME);
    if (envScriptName != null) {
      binaryName = envScriptName;
    }
    LOG.info("Use {} as script name.", binaryName);

    // Try to find the script based on an environment variable, if set
    boolean found = false;
    String envBinaryPath = envProvider.apply(ENV_SCRIPT_PATH);
    if (envBinaryPath != null) {
      this.binaryPath = getScriptFromEnvSetting(envBinaryPath);
      found = binaryPath != null;
    }

    // Try $HADOOP_COMMON_HOME
    if (!found) {
      // print a warning only if the env variable was defined
      if (envBinaryPath != null) {
        LOG.warn("Script {} does not exist, falling back " +
            "to $HADOOP_COMMON_HOME/sbin/DevicePluginScript/", envBinaryPath);
      }

      this.binaryPath = getScriptFromHadoopCommon(envProvider, binaryName);
      found = binaryPath != null;
    }

    // Try the default search directories
    if (!found) {
      LOG.info("Script not found under" +
          " $HADOOP_COMMON_HOME/sbin/DevicePluginScript/," +
          " falling back to default search directories");

      this.binaryPath = getScriptFromSearchDirs(binaryName, scriptPaths);
      found = binaryPath != null;
    }

    // Script not found
    if (!found) {
      LOG.error("Script not found in "
          + Arrays.toString(scriptPaths));
      throw new ResourceHandlerException(
          "No binary found for " + NECVEPlugin.class.getName());
    }
  }

  @Override
  public DeviceRegisterRequest getRegisterRequestInfo() {
    return DeviceRegisterRequest.Builder.newInstance()
        .setResourceName("nec.com/ve").build();
  }

  @Override
  public Set<Device> getDevices() {
    Set<Device> devices = null;

    if (useUdev) {
      try {
        devices = discoverer.getDevicesFromPath("/dev");
      } catch (IOException e) {
        LOG.error("Error during scanning devices", e);
      }
    } else {
      CommandExecutor executor =
          commandExecutorProvider.apply(new String[]{this.binaryPath});
      try {
        executor.execute();
        String output = executor.getOutput();
        devices = parseOutput(output);
      } catch (IOException e) {
        LOG.error("Error during executing external binary", e);
      }
    }

    if (devices != null) {
      LOG.info("Found devices:");
      devices.forEach(dev -> LOG.info("{}", dev));
    }

    return devices;
  }

  @Override
  public DeviceRuntimeSpec onDevicesAllocated(Set<Device> set,
      YarnRuntimeType yarnRuntimeType) {
    return null;
  }

  /**
   * Parses the output of the external Python script.
   *
   * Sample line:
   * id=0, dev=/dev/ve0, state=ONLINE, busId=0000:65:00.0, major=243, minor=0
   */
  private Set<Device> parseOutput(String output) {
    Set<Device> devices = new HashSet<>();

    LOG.info("Parsing output: {}", output);
    String[] lines = output.split("\n");
    outer:
    for (String line : lines) {
      Device.Builder builder = Device.Builder.newInstance();

      // map key --> builder calls
      Map<String, Consumer<String>> builderInvocations =
          getBuilderInvocationsMap(builder);

      String[] keyValues = line.trim().split(",");
      for (String keyValue : keyValues) {
        String[] tokens = keyValue.trim().split("=");
        if (tokens.length != 2) {
          LOG.error("Unknown format of script output! Skipping this line");
          continue outer;
        }

        final String key = tokens[0];
        final String value = tokens[1];

        Consumer<String> builderInvocation = builderInvocations.get(key);
        if (builderInvocation != null) {
          builderInvocation.accept(value);
        } else {
          LOG.warn("Unknown key {}, ignored", key);
        }
      }// for key value pairs
      Device device = builder.build();
      if (device.isHealthy()) {
        devices.add(device);
      } else {
        LOG.warn("Skipping device {} because it's not healthy", device);
      }
    }

    return devices;
  }

  @Override
  public void onDevicesReleased(Set<Device> releasedDevices) {
    // nop
  }

  @Override
  public Set<Device> allocateDevices(Set<Device> availableDevices, int count,
      Map<String, String> env) {
    // Can consider topology, utilization.etc
    Set<Device> allocated = new HashSet<>();
    int number = 0;
    for (Device d : availableDevices) {
      allocated.add(d);
      number++;
      if (number == count) {
        break;
      }
    }
    return allocated;
  }

  private CommandExecutor createCommandExecutor(String[] command) {
    return new Shell.ShellCommandExecutor(
        command);
  }

  private String getScriptFromEnvSetting(String envBinaryPath) {
    LOG.info("Checking script path: {}", envBinaryPath);
    File f = new File(envBinaryPath);

    if (!f.exists()) {
      LOG.warn("Script {} does not exist", envBinaryPath);
      return null;
    }

    if (f.isDirectory()) {
      LOG.warn("Specified path {} is a directory", envBinaryPath);
      return null;
    }

    if (!FileUtil.canExecute(f)) {
      LOG.warn("Script {} is not executable", envBinaryPath);
      return null;
    }

    LOG.info("Found script: {}", envBinaryPath);

    return envBinaryPath;
  }

  private String getScriptFromHadoopCommon(
      Function<String, String> envProvider, String binaryName) {
    String scriptPath = null;
    String hadoopCommon = envProvider.apply(HADOOP_COMMON_HOME);

    if (hadoopCommon != null) {
      String targetPath = hadoopCommon +
          "/sbin/DevicePluginScript/" + binaryName;
      LOG.info("Checking script {}: ", targetPath);
      if (new File(targetPath).exists()) {
        LOG.info("Found script: {}", targetPath);
        scriptPath = targetPath;
      }
    } else {
      LOG.info("$HADOOP_COMMON_HOME is not set");
    }

    return scriptPath;
  }

  private String getScriptFromSearchDirs(String binaryName,
      String[] scriptPaths) {
    String scriptPath = null;

    for (String dir : scriptPaths) {
      File f = new File(dir, binaryName);
      if (f.exists()) {
        LOG.info("Found script: {}", dir);
        scriptPath = f.getAbsolutePath();
        break;
      }
    }

    return scriptPath;
  }

  private Map<String, Consumer<String>> getBuilderInvocationsMap(
      Device.Builder builder) {
    Map<String, Consumer<String>> builderInvocations = new HashMap<>();
    builderInvocations.put("id", v -> builder.setId(Integer.parseInt(v)));
    builderInvocations.put("dev", v -> builder.setDevPath(v));
    builderInvocations.put("state", v -> {
      if (v.equals("ONLINE")) {
        builder.setHealthy(true);
      }
      builder.setStatus(v);
    });
    builderInvocations.put("busId", v -> builder.setBusID(v));
    builderInvocations.put("major",
        v -> builder.setMajorNumber(Integer.parseInt(v)));
    builderInvocations.put("minor",
        v -> builder.setMinorNumber(Integer.parseInt(v)));

    return builderInvocations;
  }

  @VisibleForTesting
  void setCommandExecutorProvider(
      Function<String[], CommandExecutor> provider) {
    this.commandExecutorProvider = provider;
  }

  @VisibleForTesting
  void setVeDeviceDiscoverer(VEDeviceDiscoverer veDeviceDiscoverer) {
    this.discoverer = veDeviceDiscoverer;
  }

  @VisibleForTesting
  String getBinaryPath() {
    return binaryPath;
  }
}
