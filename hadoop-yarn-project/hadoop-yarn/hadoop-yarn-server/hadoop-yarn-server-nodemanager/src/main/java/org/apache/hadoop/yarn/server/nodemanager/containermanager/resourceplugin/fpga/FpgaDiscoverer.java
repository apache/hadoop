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


package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.discovery.AoclOutputBasedDiscoveryStrategy;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.discovery.FPGADiscoveryStrategy;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.discovery.ScriptBasedFPGADiscoveryStrategy;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.discovery.SettingsBasedFPGADiscoveryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;

public class FpgaDiscoverer extends Configured {
  private static final Logger LOG = LoggerFactory.getLogger(
      FpgaDiscoverer.class);

  private AbstractFpgaVendorPlugin plugin = null;
  private List<FpgaDevice> currentFpgaInfo = null;

  private Function<String, Optional<String>> scriptRunner = this::runScript;

  // shell command timeout
  public static final int MAX_EXEC_TIMEOUT_MS = 10 * 1000;

  @VisibleForTesting
  void setScriptRunner(Function<String, Optional<String>> scriptRunner) {
    this.scriptRunner = scriptRunner;
  }

  public List<FpgaDevice> getCurrentFpgaInfo() {
    return currentFpgaInfo;
  }

  public void setResourceHanderPlugin(AbstractFpgaVendorPlugin vendorPlugin) {
    this.plugin = vendorPlugin;
  }

  public boolean diagnose() {
    return this.plugin.diagnose(MAX_EXEC_TIMEOUT_MS);
  }

  public void initialize(Configuration config) throws YarnException {
    setConf(config);
    this.plugin.initPlugin(config);
    // Try to diagnose FPGA
    LOG.info("Trying to diagnose FPGA information ...");
    if (!diagnose()) {
      LOG.warn("Failed to pass FPGA devices diagnose");
    }
  }

  /**
   * Get available devices minor numbers from toolchain or static configuration.
   *
   * @return the list of FPGA devices
   * @throws ResourceHandlerException if there's any error during discovery
   **/
  public List<FpgaDevice> discover()
      throws ResourceHandlerException {
    List<FpgaDevice> list;
    String allowed = getConf().get(YarnConfiguration.NM_FPGA_ALLOWED_DEVICES);

    String availableDevices = getConf().get(
        YarnConfiguration.NM_FPGA_AVAILABLE_DEVICES);
    String discoveryScript = getConf().get(
        YarnConfiguration.NM_FPGA_DEVICE_DISCOVERY_SCRIPT);

    FPGADiscoveryStrategy discoveryStrategy;
    if (availableDevices != null) {
      discoveryStrategy =
          new SettingsBasedFPGADiscoveryStrategy(
              plugin.getFpgaType(), availableDevices);
    } else if (discoveryScript != null) {
      discoveryStrategy =
          new ScriptBasedFPGADiscoveryStrategy(
              plugin.getFpgaType(), scriptRunner, discoveryScript);
    } else {
      discoveryStrategy = new AoclOutputBasedDiscoveryStrategy(plugin);
    }

    list = discoveryStrategy.discover();

    if (allowed == null || allowed.equalsIgnoreCase(
        YarnConfiguration.AUTOMATICALLY_DISCOVER_GPU_DEVICES)) {
      currentFpgaInfo = ImmutableList.copyOf(list);
      return list;
    } else if (allowed.matches("(\\d,)*\\d")){
      Set<String> minors = Sets.newHashSet(allowed.split(","));

      // Replace list with a filtered one
      list = list
        .stream()
        .filter(dev -> minors.contains(String.valueOf(dev.getMinor())))
        .collect(Collectors.toList());

      currentFpgaInfo = ImmutableList.copyOf(list);

      // if the count of user configured is still larger than actual
      if (list.size() != minors.size()) {
        LOG.warn("We continue although there're mistakes in user's configuration " +
            YarnConfiguration.NM_FPGA_ALLOWED_DEVICES +
            "user configured:" + allowed + ", while the real:" + list.toString());
      }
    } else {
      throw new ResourceHandlerException("Invalid value configured for " +
          YarnConfiguration.NM_FPGA_ALLOWED_DEVICES + ":\"" + allowed + "\"");
    }

    return list;
  }

  private Optional<String> runScript(String path) {
    if (path == null || path.trim().isEmpty()) {
      LOG.error("Undefined script");
      return Optional.empty();
    }

    File f = new File(path);
    if (!f.exists()) {
      LOG.error("Script does not exist");
      return Optional.empty();
    }

    if (!FileUtil.canExecute(f)) {
      LOG.error("Script is not executable");
      return Optional.empty();
    }

    ShellCommandExecutor shell = new ShellCommandExecutor(
        new String[] {path},
        null,
        null,
        MAX_EXEC_TIMEOUT_MS);
    try {
      shell.execute();
      String output = shell.getOutput();
      return Optional.of(output);
    } catch (IOException e) {
      LOG.error("Cannot execute script", e);
      return Optional.empty();
    }
  }
}
