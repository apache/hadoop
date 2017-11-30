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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;

/**
 * Factory to create GpuDocker Command Plugin instance
 */
public class GpuDockerCommandPluginFactory {
  public static DockerCommandPlugin createGpuDockerCommandPlugin(
      Configuration conf) throws YarnException {
    String impl = conf.get(YarnConfiguration.NM_GPU_DOCKER_PLUGIN_IMPL,
        YarnConfiguration.DEFAULT_NM_GPU_DOCKER_PLUGIN_IMPL);
    if (impl.equals(YarnConfiguration.NVIDIA_DOCKER_V1)) {
      return new NvidiaDockerV1CommandPlugin(conf);
    }

    throw new YarnException(
        "Unkown implementation name for Gpu docker plugin, impl=" + impl);
  }
}
