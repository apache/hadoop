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

import org.apache.hadoop.mapred.gridmix.Progressive;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.apache.hadoop.tools.rumen.ResourceUsageMetrics;
import org.apache.hadoop.conf.Configuration;

/**
 * <p>Each resource to be emulated should have a corresponding implementation 
 * class that implements {@link ResourceUsageEmulatorPlugin}.</p>
 * <br><br>
 * {@link ResourceUsageEmulatorPlugin} will be configured using the 
 * {@link #initialize(Configuration, ResourceUsageMetrics, 
 *                    ResourceCalculatorPlugin, Progressive)} call.
 * Every 
 * {@link ResourceUsageEmulatorPlugin} is also configured with a feedback module
 * i.e a {@link ResourceCalculatorPlugin}, to monitor the current resource 
 * usage. {@link ResourceUsageMetrics} decides the final resource usage value to
 * emulate. {@link Progressive} keeps track of the task's progress.
 * 
 * <br><br>
 * 
 * For configuring GridMix to load and and use a resource usage emulator, 
 * see {@link ResourceUsageMatcher}. 
 */
public interface ResourceUsageEmulatorPlugin extends Progressive {
  /**
   * Initialize the plugin. This might involve
   *   - initializing the variables
   *   - calibrating the plugin
   */
  void initialize(Configuration conf, ResourceUsageMetrics metrics, 
                  ResourceCalculatorPlugin monitor,
                  Progressive progress);

  /**
   * Emulate the resource usage to match the usage target. The plugin can use
   * the given {@link ResourceCalculatorPlugin} to query for the current 
   * resource usage.
   * @throws IOException
   * @throws InterruptedException
   */
  void emulate() throws IOException, InterruptedException;
}
