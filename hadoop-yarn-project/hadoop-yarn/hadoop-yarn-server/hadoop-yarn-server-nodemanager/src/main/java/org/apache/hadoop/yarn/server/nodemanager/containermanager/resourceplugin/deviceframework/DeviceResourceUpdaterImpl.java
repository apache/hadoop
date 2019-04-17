/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.NodeResourceUpdaterPlugin;

import java.util.Set;

/**
 * Hooks into NodeStatusUpdater to update resource.
 * */
public class DeviceResourceUpdaterImpl extends NodeResourceUpdaterPlugin {

  final static Logger LOG = LoggerFactory.
      getLogger(DeviceResourceUpdaterImpl.class);

  private String resourceName;
  private DevicePlugin devicePlugin;

  public DeviceResourceUpdaterImpl(String resourceName,
      DevicePlugin devicePlugin) {
    this.devicePlugin = devicePlugin;
    this.resourceName = resourceName;
  }

  @Override
  public void updateConfiguredResource(Resource res)
      throws YarnException {
    LOG.info(resourceName + " plugin update resource ");
    Set<Device> devices = null;
    try {
      devices = devicePlugin.getDevices();
    } catch (Exception e) {
      throw new YarnException("Exception thrown from plugin's getDevices"
          + e.getMessage());
    }
    if (null == devices) {
      LOG.warn(resourceName
          + " plugin failed to discover resource ( null value got).");
      return;
    }
    res.setResourceValue(resourceName, devices.size());
  }

}
