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
package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;


public interface Store extends NodeStore, ApplicationsStore {
  public interface ApplicationInfo {
    public ApplicationMaster getApplicationMaster();
    public Container getMasterContainer();
    public ApplicationSubmissionContext getApplicationSubmissionContext();
    public List<Container> getContainers();
  }
  public interface RMState {
    public List<RMNode> getStoredNodeManagers() ;
    public Map<ApplicationId, ApplicationInfo> getStoredApplications();
    public NodeId getLastLoggedNodeId();
  }
  public RMState restore() throws IOException;
  public void doneWithRecovery();
}