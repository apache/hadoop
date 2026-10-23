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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestGpuDockerCommandPluginFactory {

  @Test
  public void testUnknownImplementation() {
    Configuration conf = new Configuration(false);
    conf.set(YarnConfiguration.NM_GPU_DOCKER_PLUGIN_IMPL, "unsupported");

    YarnException exception = assertThrows(YarnException.class,
        () -> GpuDockerCommandPluginFactory.createGpuDockerCommandPlugin(conf));

    assertEquals("Unknown implementation name for Gpu docker plugin, "
        + "impl=unsupported", exception.getMessage());
  }
}
