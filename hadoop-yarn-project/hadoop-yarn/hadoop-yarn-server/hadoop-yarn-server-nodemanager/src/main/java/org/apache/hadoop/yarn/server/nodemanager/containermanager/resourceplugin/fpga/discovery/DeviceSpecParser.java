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


package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.discovery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.FpgaDevice;

/**
 * Parses a string which specifies FPGA devices. Multiple devices should be
 * separated by a comma. A device specification should contain the
 * symbolic name of the device, major and minor device numbers.
 *
 * Example: "acl0/243:0,acl1/243:1".
 */
public final class DeviceSpecParser {
  private static final String DEVICE_SPEC_REGEX =
      "(\\w+[0-31])(\\/)(\\d+)(\\:)(\\d+)";

  private static final Pattern DEVICE_PATTERN =
      Pattern.compile(DEVICE_SPEC_REGEX);

  private DeviceSpecParser() {
    // no instances
  }

  static List<FpgaDevice> getDevicesFromString(String type, String devices)
      throws ResourceHandlerException {
    if (devices.trim().isEmpty()) {
      return Collections.emptyList();
    }

    String[] deviceList = devices.split(",");

    List<FpgaDevice> fpgaDevices = new ArrayList<>();

    for (final String deviceSpec : deviceList) {
      Matcher matcher = DEVICE_PATTERN.matcher(deviceSpec);
      if (matcher.matches()) {
        try {
          String devName = matcher.group(1);
          int major = Integer.parseInt(matcher.group(3));
          int minor = Integer.parseInt(matcher.group(5));
          fpgaDevices.add(new FpgaDevice(type,
              major,
              minor,
              devName));
        } catch (NumberFormatException e) {
          throw new ResourceHandlerException(
              "Cannot parse major/minor number: " + deviceSpec);
        }
      } else {
        throw new ResourceHandlerException(
            "Illegal device specification string: " + deviceSpec);
      }
    }

    return fpgaDevices;
  }
}
