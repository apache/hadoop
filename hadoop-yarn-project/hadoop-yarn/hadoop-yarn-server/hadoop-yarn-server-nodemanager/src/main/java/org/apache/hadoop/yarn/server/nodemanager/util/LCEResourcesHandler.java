/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.nodemanager.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;

@Deprecated
public interface LCEResourcesHandler extends Configurable {

  void init(LinuxContainerExecutor lce) throws IOException;

  /**
   * Called by the LinuxContainerExecutor before launching the executable
   * inside the container.
   * @param containerId the id of the container being launched
   * @param containerResource the node resources the container will be using
   */
  void preExecute(ContainerId containerId, Resource containerResource)
       throws IOException;

  /**
   * Called by the LinuxContainerExecutor after the executable inside the
   * container has exited (successfully or not).
   * @param containerId the id of the container which was launched
   */
  void postExecute(ContainerId containerId);
  
  String getResourcesOption(ContainerId containerId);
}
