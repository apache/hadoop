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


import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga.FpgaResourceAllocator;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.NodeResourceUpdaterPlugin;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.FPGA_URI;

public class FpgaNodeResourceUpdateHandler extends NodeResourceUpdaterPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(
      FpgaNodeResourceUpdateHandler.class);

  @Override
  public void updateConfiguredResource(Resource res) throws YarnException {
    LOG.info("Initializing configured FPGA resources for the NodeManager.");
    List<FpgaResourceAllocator.FpgaDevice> list = FpgaDiscoverer.getInstance().getCurrentFpgaInfo();
    List<Integer> minors = new LinkedList<>();
    for (FpgaResourceAllocator.FpgaDevice device : list) {
      minors.add(device.getMinor());
    }
    if (minors.isEmpty()) {
      LOG.info("Didn't find any usable FPGAs on the NodeManager.");
      return;
    }
    long count = minors.size();

    Map<String, ResourceInformation> configuredResourceTypes =
        ResourceUtils.getResourceTypes();
    if (!configuredResourceTypes.containsKey(FPGA_URI)) {
      throw new YarnException("Wrong configurations, found " + count +
          " usable FPGAs, however " + FPGA_URI
          + " resource-type is not configured inside"
          + " resource-types.xml, please configure it to enable FPGA feature or"
          + " remove " + FPGA_URI + " from "
          + YarnConfiguration.NM_RESOURCE_PLUGINS);
    }

    res.setResourceValue(FPGA_URI, count);
  }
}
