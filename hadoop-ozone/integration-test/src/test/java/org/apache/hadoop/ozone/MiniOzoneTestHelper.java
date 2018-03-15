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
package org.apache.hadoop.ozone;

import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;
import org.apache.hadoop.ozone.container.common.statemachine
    .DatanodeStateMachine;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.util.ServicePlugin;

/**
 * Stateless helper functions for MiniOzone based tests.
 */
public class MiniOzoneTestHelper {

  private MiniOzoneTestHelper() {
  }

  public static OzoneContainer getOzoneContainer(DataNode dataNode) {
    return findHdslPlugin(dataNode).getDatanodeStateMachine()
        .getContainer();
  }

  public static ContainerManager getOzoneContainerManager(DataNode dataNode) {
    return findHdslPlugin(dataNode).getDatanodeStateMachine()
        .getContainer().getContainerManager();

  }
  public static DatanodeStateMachine getStateMachine(DataNode dataNode) {
    return findHdslPlugin(dataNode).getDatanodeStateMachine();
  }

  private static HdslServerPlugin findHdslPlugin(DataNode dataNode) {
    for (ServicePlugin plugin : dataNode.getPlugins()) {
      if (plugin instanceof HdslServerPlugin) {
        return (HdslServerPlugin) plugin;
      }
    }
    throw new IllegalStateException("Can't find the Hdsl server plugin in the"
        + " plugin collection of datanode");
  }


}
