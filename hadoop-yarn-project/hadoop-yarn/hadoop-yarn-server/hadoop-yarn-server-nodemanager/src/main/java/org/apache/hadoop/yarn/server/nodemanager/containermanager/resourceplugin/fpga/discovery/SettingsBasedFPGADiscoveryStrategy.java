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

import java.util.List;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga.FpgaResourceAllocator.FpgaDevice;

/**
 * FPGA device discovery strategy which parses a string.
 * The string must consist of a single line and be in a specific format.
 *
 * See DeviceSpecParser for details.
 */
public class SettingsBasedFPGADiscoveryStrategy
    implements FPGADiscoveryStrategy {

  private final String type;
  private final String availableDevices;

  public SettingsBasedFPGADiscoveryStrategy(
      String fpgaType, String devices) {
    this.type = fpgaType;
    this.availableDevices = devices;
  }

  @Override
  public List<FpgaDevice> discover() throws ResourceHandlerException {
    List<FpgaDevice> list =
        DeviceSpecParser.getDevicesFromString(type, availableDevices);
    if (list.isEmpty()) {
      throw new ResourceHandlerException("No FPGA devices were specified");
    }
    return list;
  }
}