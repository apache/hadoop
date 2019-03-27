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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga.FpgaResourceAllocator;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga.FpgaResourceAllocator.FpgaDevice;

import java.util.List;
import java.util.Map;


/**
 * FPGA plugin interface for vendor to implement. Used by {@link FpgaDiscoverer} and
 * {@link org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga.FpgaResourceHandlerImpl}
 * to discover devices/download IP/configure IP
 * */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface AbstractFpgaVendorPlugin {

  /**
   * Check vendor's toolchain and required environment
   * @param conf Hadoop configuration
   * @return true if the initialization was successful
   * */
  boolean initPlugin(Configuration conf);

  /**
   * Diagnose the devices using vendor toolchain but no need to parse device information
   *
   * @param timeout timeout in milliseconds
   * @return true if the diagnostics was successful
   * */
  boolean diagnose(int timeout);

  /**
   * Discover the vendor's FPGA devices with execution time constraint
   * @param timeout The vendor plugin should return result during this time
   * @return The result will be added to FPGAResourceAllocator for later scheduling
   * */
  List<FpgaResourceAllocator.FpgaDevice> discover(int timeout);

  /**
   * Since all vendor plugins share a {@link org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga.FpgaResourceAllocator}
   * which distinguish FPGA devices by type. Vendor plugin must report this.
   *
   * @return the type of FPGA plugin represented as a string
   * */
  String getFpgaType();

  /**
   * The vendor plugin download required IP files to a required directory.
   * It should check if the IP file has already been downloaded.
   * @param id The identifier for IP file. Comes from application, ie. matrix_multi_v1
   * @param dstDir The plugin should download IP file to this directory
   * @param localizedResources The container localized resource can be searched for IP file. Key is
   * localized file path and value is soft link names
   * @return The absolute path string of IP file
   * */
  String retrieveIPfilePath(String id, String dstDir,
      Map<Path, List<String>> localizedResources);

  /**
   * The vendor plugin configure an IP file to a device
   * @param ipPath The absolute path of the IP file
   * @param device The FPGA device object
   * @return configure device ok or not
   * */
  boolean configureIP(String ipPath, FpgaDevice device);
}
