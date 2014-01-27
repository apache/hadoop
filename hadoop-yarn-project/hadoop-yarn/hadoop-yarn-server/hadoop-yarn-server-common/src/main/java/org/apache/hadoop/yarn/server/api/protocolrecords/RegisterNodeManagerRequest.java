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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

public abstract class RegisterNodeManagerRequest {
  
  public static RegisterNodeManagerRequest newInstance(NodeId nodeId,
      int httpPort, Resource resource, String nodeManagerVersionId,
      List<ContainerStatus> containerStatuses) {
    RegisterNodeManagerRequest request =
        Records.newRecord(RegisterNodeManagerRequest.class);
    request.setHttpPort(httpPort);
    request.setResource(resource);
    request.setNodeId(nodeId);
    request.setNMVersion(nodeManagerVersionId);
    request.setContainerStatuses(containerStatuses);
    return request;
  }
  
  public abstract NodeId getNodeId();
  public abstract int getHttpPort();
  public abstract Resource getResource();
  public abstract String getNMVersion();
  public abstract List<ContainerStatus> getContainerStatuses();
  
  public abstract void setNodeId(NodeId nodeId);
  public abstract void setHttpPort(int port);
  public abstract void setResource(Resource resource);
  public abstract void setNMVersion(String version);
  public abstract void setContainerStatuses(List<ContainerStatus> containerStatuses);
}
