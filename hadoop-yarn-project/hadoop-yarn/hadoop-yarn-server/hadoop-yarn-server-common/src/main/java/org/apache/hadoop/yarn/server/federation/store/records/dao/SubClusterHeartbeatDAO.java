/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.federation.store.records.dao;

import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * SubClusterHeartbeatRequest is a report of the runtime information of the
 * subcluster that is participating in federation.
 *
 * It includes information such as:
 * {@link SubClusterId}
 * The URL of the subcluster
 * The timestamp representing the last start time of the subCluster
 * {@code FederationsubClusterState}
 * The current capacity and utilization of the subCluster
*/
@XmlRootElement(name = "heartbeatDAO")
@XmlAccessorType(XmlAccessType.FIELD)
public class SubClusterHeartbeatDAO {
  protected SubClusterIdDAO scId;
  protected SubClusterStateDAO scState;
  protected String capability;

  public SubClusterHeartbeatDAO() {
  } // JAXB needs this

  public SubClusterHeartbeatDAO(SubClusterId id, SubClusterState state,
      String capability) {
    this.scId = new SubClusterIdDAO(id);
    this.scState = new SubClusterStateDAO(state);
    this.capability = capability;
  }

  public SubClusterHeartbeatDAO(SubClusterHeartbeatRequest request) {
    this(request.getSubClusterId(), request.getState(),
        request.getCapability());
  }

  public SubClusterIdDAO getSubClusterId() {
    return scId;
  }

  public SubClusterStateDAO getScState() {
    return scState;
  }

  public String getCapability() {
    return capability;
  }
}
