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

import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * The request sent to set the state of a subcluster to either
 * SC_DECOMMISSIONED, SC_LOST, or SC_DEREGISTERED.
 *
 * The update includes details such as:
 * {@link SubClusterId}
 * {@link SubClusterState}
 */
@XmlRootElement(name = "deregisterDAO")
@XmlAccessorType(XmlAccessType.FIELD)
public class SubClusterDeregisterDAO {

  protected SubClusterIdDAO scId;
  protected SubClusterStateDAO scState;

  public SubClusterDeregisterDAO(){
  } // JAXB needs this

  public SubClusterDeregisterDAO(SubClusterId id, SubClusterState state){
    this.scId = new SubClusterIdDAO(id);
    this.scState = new SubClusterStateDAO(state);
  }

  public SubClusterDeregisterDAO(SubClusterDeregisterRequest request) {
    this(request.getSubClusterId(), request.getState());
  }

  public SubClusterIdDAO getSubClusterId(){
    return scId;
  }

  public SubClusterStateDAO getScState() {
    return scState;
  }
}
