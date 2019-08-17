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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.AbstractFpgaVendorPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.FpgaDiscoverer;

/**
 * FPGA device discovery strategy which invokes the "aocl" SDK command
 * to retrieve the list of available FPGA cards.
 */
public class AoclOutputBasedDiscoveryStrategy
    implements FPGADiscoveryStrategy {

  private final AbstractFpgaVendorPlugin plugin;

  public AoclOutputBasedDiscoveryStrategy(AbstractFpgaVendorPlugin fpgaPlugin) {
    this.plugin = fpgaPlugin;
  }

  @Override
  public List<FpgaDevice> discover() throws ResourceHandlerException {
    List<FpgaDevice> list =
        plugin.discover(FpgaDiscoverer.MAX_EXEC_TIMEOUT_MS);
    if (list.isEmpty()) {
      throw new ResourceHandlerException("No FPGA devices detected!");
    }

    return list;
  }
}