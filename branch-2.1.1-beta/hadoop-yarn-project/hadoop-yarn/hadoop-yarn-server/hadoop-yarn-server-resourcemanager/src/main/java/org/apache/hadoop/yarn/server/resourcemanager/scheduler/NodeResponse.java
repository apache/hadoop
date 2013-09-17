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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;


/**
 * The class that encapsulates response from clusterinfo for 
 * updates from the node managers.
 */
public class NodeResponse {
  private final List<Container> completed;
  private final List<Container> toCleanUp;
  private final List<ApplicationId> finishedApplications;
  
  public NodeResponse(List<ApplicationId> finishedApplications,
      List<Container> completed, List<Container> toKill) {
    this.finishedApplications = finishedApplications;
    this.completed = completed;
    this.toCleanUp = toKill;
  }
  public List<ApplicationId> getFinishedApplications() {
    return this.finishedApplications;
  }
  public List<Container> getCompletedContainers() {
    return this.completed;
  }
  public List<Container> getContainersToCleanUp() {
    return this.toCleanUp;
  }
}