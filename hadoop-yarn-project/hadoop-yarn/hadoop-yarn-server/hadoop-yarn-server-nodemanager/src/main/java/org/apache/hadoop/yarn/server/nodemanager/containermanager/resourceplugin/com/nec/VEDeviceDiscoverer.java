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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.CommandExecutor;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

class VEDeviceDiscoverer {
  private static final String STATE_TERMINATING = "TERMINATING";
  private static final String STATE_INITIALIZING = "INITIALIZING";
  private static final String STATE_OFFLINE = "OFFLINE";
  private static final String STATE_ONLINE = "ONLINE";
  private static final Logger LOG =
      LoggerFactory.getLogger(VEDeviceDiscoverer.class);

  private static final String[] DEVICE_STATE = {STATE_ONLINE, STATE_OFFLINE,
      STATE_INITIALIZING, STATE_TERMINATING};

  private UdevUtil udev;
  private Function<String[], CommandExecutor>
      commandExecutorProvider = this::createCommandExecutor;

  VEDeviceDiscoverer(UdevUtil udevUtil) {
    udev = udevUtil;
  }

  public Set<Device> getDevicesFromPath(String path) throws IOException {
    MutableInt counter = new MutableInt(0);

    return Files.walk(Paths.get(path), 1)
      .filter(p -> p.toFile().getName().startsWith("veslot"))
      .map(p -> toDevice(p, counter))
      .collect(Collectors.toSet());
  }

  private Device toDevice(Path p, MutableInt counter) {
    CommandExecutor executor =
        commandExecutorProvider.apply(
            new String[]{"stat", "-L", "-c", "%t:%T:%F", p.toString()});

    try {
      LOG.info("Checking device file: {}", p);
      executor.execute();
      String statOutput = executor.getOutput();
      String[] stat = statOutput.trim().split(":");

      int major = Integer.parseInt(stat[0], 16);
      int minor = Integer.parseInt(stat[1], 16);
      char devType = getDevType(p, stat[2]);
      int deviceNumber = makeDev(major, minor);
      LOG.info("Device: major: {}, minor: {}, devNo: {}, type: {}",
          major, minor, deviceNumber, devType);
      String sysPath = udev.getSysPath(deviceNumber, devType);
      LOG.info("Device syspath: {}", sysPath);
      String deviceState = getDeviceState(sysPath);

      Device.Builder builder = Device.Builder.newInstance();
      builder.setId(counter.getAndIncrement())
        .setMajorNumber(major)
        .setMinorNumber(minor)
        .setHealthy(STATE_ONLINE.equalsIgnoreCase(deviceState))
        .setStatus(deviceState)
        .setDevPath(p.toAbsolutePath().toString());

      return builder.build();
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot execute stat command", e);
    }
  }

  private int makeDev(int major, int minor) {
    return major * 256 + minor;
  }

  private char getDevType(Path p, String fromStat) {
    if (fromStat.contains("character")) {
      return 'c';
    } else if (fromStat.contains("block")) {
      return 'b';
    } else {
      throw new IllegalArgumentException(
          "File is neither a char nor block device: " + p);
    }
  }

  private String getDeviceState(String sysPath) throws IOException {
    Path statePath = Paths.get(sysPath, "os_state");

    try (FileInputStream fis =
        new FileInputStream(statePath.toString())) {
      byte state = (byte) fis.read();

      if (state < 0 || DEVICE_STATE.length <= state) {
        return String.format("Unknown (%d)", state);
      } else {
        return DEVICE_STATE[state];
      }
    }
  }

  private CommandExecutor createCommandExecutor(String[] command) {
    return new Shell.ShellCommandExecutor(
        command);
  }

  @VisibleForTesting
  void setCommandExecutorProvider(
      Function<String[], CommandExecutor> provider) {
    this.commandExecutorProvider = provider;
  }
}