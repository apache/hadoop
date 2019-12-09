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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.FpgaResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuDiscoverer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuNodeResourceUpdateHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuResourcePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.FPGA_URI;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.GPU_URI;

/**
 * Manages {@link ResourcePlugin} configured on this NodeManager.
 */
public class ResourcePluginManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(ResourcePluginManager.class);
  private static final Set<String> SUPPORTED_RESOURCE_PLUGINS = ImmutableSet.of(
      GPU_URI, FPGA_URI);

  private Map<String, ResourcePlugin> configuredPlugins =
          Collections.emptyMap();

  public void initialize(Context context)
      throws YarnException {
    Configuration conf = context.getConf();

    String[] plugins = getPluginsFromConfig(conf);

    Map<String, ResourcePlugin> pluginMap = Maps.newHashMap();
    if (plugins != null) {
      pluginMap = initializePlugins(conf, context, plugins);
    }

    configuredPlugins = Collections.unmodifiableMap(pluginMap);
  }

  private String[] getPluginsFromConfig(Configuration conf) {
    String[] plugins = conf.getStrings(YarnConfiguration.NM_RESOURCE_PLUGINS);
    if (plugins == null || plugins.length == 0) {
      LOG.info("No Resource plugins found from configuration!");
    }
    LOG.info("Found Resource plugins from configuration: "
        + Arrays.toString(plugins));

    return plugins;
  }

  private Map<String, ResourcePlugin> initializePlugins(Configuration conf,
      Context context, String[] plugins) throws YarnException {
    Map<String, ResourcePlugin> pluginMap = Maps.newHashMap();

    for (String resourceName : plugins) {
      resourceName = resourceName.trim();
      ensurePluginIsSupported(resourceName);

      if (!isPluginDuplicate(pluginMap, resourceName)) {
        ResourcePlugin plugin = null;
        if (resourceName.equals(GPU_URI)) {
          final GpuDiscoverer gpuDiscoverer = new GpuDiscoverer();
          final GpuNodeResourceUpdateHandler updateHandler =
              new GpuNodeResourceUpdateHandler(gpuDiscoverer, conf);
          plugin = new GpuResourcePlugin(updateHandler, gpuDiscoverer);
        } else if (resourceName.equals(FPGA_URI)) {
          plugin = new FpgaResourcePlugin();
        }

        if (plugin == null) {
          throw new YarnException(
              "This shouldn't happen, plugin=" + resourceName
                  + " should be loaded and initialized");
        }
        plugin.initialize(context);
        LOG.info("Initialized plugin {}", plugin);
        pluginMap.put(resourceName, plugin);
      }
    }
    return pluginMap;
  }

  private void ensurePluginIsSupported(String resourceName)
      throws YarnException {
    if (!SUPPORTED_RESOURCE_PLUGINS.contains(resourceName)) {
      String msg =
          "Trying to initialize resource plugin with name=" + resourceName
              + ", it is not supported, list of supported plugins:"
              + StringUtils.join(",", SUPPORTED_RESOURCE_PLUGINS);
      LOG.error(msg);
      throw new YarnException(msg);
    }
  }

  private boolean isPluginDuplicate(Map<String, ResourcePlugin> pluginMap,
      String resourceName) {
    if (pluginMap.containsKey(resourceName)) {
      LOG.warn("Ignoring duplicate Resource plugin definition: " +
          resourceName);
      return true;
    }
    return false;
  }


  public void cleanup() throws YarnException {
    for (ResourcePlugin plugin : configuredPlugins.values()) {
      plugin.cleanup();
    }
  }

  /**
   * Get resource name (such as gpu/fpga) to plugin references.
   * @return read-only map of resource name to plugins.
   */
  public synchronized Map<String, ResourcePlugin> getNameToPlugins() {
    return configuredPlugins;
  }
}
