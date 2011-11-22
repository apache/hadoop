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
package org.apache.hadoop.mapred.gridmix.emulators.resourceusage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.gridmix.Progressive;
import org.apache.hadoop.util.ResourceCalculatorPlugin;
import org.apache.hadoop.tools.rumen.ResourceUsageMetrics;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * <p>This is the driver class for managing all the resource usage emulators.
 * {@link ResourceUsageMatcher} expects a comma separated list of 
 * {@link ResourceUsageEmulatorPlugin} implementations specified using 
 * {@link #RESOURCE_USAGE_EMULATION_PLUGINS} as the configuration parameter.</p>
 * 
 * <p>Note that the order in which the emulators are invoked is same as the 
 * order in which they are configured.
 */
public class ResourceUsageMatcher implements Progressive {
  /**
   * Configuration key to set resource usage emulators.
   */
  public static final String RESOURCE_USAGE_EMULATION_PLUGINS =
    "gridmix.emulators.resource-usage.plugins";
  
  private List<ResourceUsageEmulatorPlugin> emulationPlugins = 
    new ArrayList<ResourceUsageEmulatorPlugin>();
  
  /**
   * Configure the {@link ResourceUsageMatcher} to load the configured plugins
   * and initialize them.
   */
  @SuppressWarnings("unchecked")
  public void configure(Configuration conf, ResourceCalculatorPlugin monitor, 
                        ResourceUsageMetrics metrics, Progressive progress) {
    Class[] plugins = conf.getClasses(RESOURCE_USAGE_EMULATION_PLUGINS);
//, null, ResourceUsageEmulatorPlugin.class);
    if (plugins == null) {
      System.out.println("No resource usage emulator plugins configured.");
    } else {
      for (Class<? extends ResourceUsageEmulatorPlugin> plugin : plugins) {
        if (plugin != null) {
          emulationPlugins.add(ReflectionUtils.newInstance(plugin, conf));
        }
      }
    }

    // initialize the emulators once all the configured emulator plugins are
    // loaded
    for (ResourceUsageEmulatorPlugin emulator : emulationPlugins) {
      emulator.initialize(conf, metrics, monitor, progress);
    }
  }
  
  public void matchResourceUsage() throws IOException, InterruptedException {
    for (ResourceUsageEmulatorPlugin emulator : emulationPlugins) {
      // match the resource usage
      emulator.emulate();
    }
  }
  
  /**
   * Returns the average progress.
   */
  @Override
  public float getProgress() {
    if (emulationPlugins.size() > 0) {
      // return the average progress
      float progress = 0f;
      for (ResourceUsageEmulatorPlugin emulator : emulationPlugins) {
        // consider weighted progress of each emulator
        progress += emulator.getProgress();
      }

      return progress / emulationPlugins.size();
    }
    
    // if no emulators are configured then return 1
    return 1f;
    
  }
}
