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

package org.apache.hadoop.yarn.logaggregation;

import java.util.ArrayList;
import java.util.List;

/**
 * The ContainerLogMeta includes:
 * <ul>
 *   <li>The Container Id.</li>
 *   <li>The NodeManager Id.</li>
 *   <li>A list of {@link ContainerLogFileInfo}.</li>
 * </ul>
 *
 */
public class ContainerLogMeta {
  private String containerId;
  private String nodeId;
  private List<ContainerLogFileInfo> logMeta;

  public ContainerLogMeta(String containerId, String nodeId) {
    this.containerId = containerId;
    this.nodeId = nodeId;
    logMeta = new ArrayList<>();
  }

  public String getNodeId() {
    return this.nodeId;
  }

  public String getContainerId() {
    return this.containerId;
  }

  public void addLogMeta(String fileName, String fileSize,
      String lastModificationTime) {
    logMeta.add(new ContainerLogFileInfo(fileName, fileSize,
        lastModificationTime));
  }

  public List<ContainerLogFileInfo> getContainerLogMeta() {
    return this.logMeta;
  }
}
