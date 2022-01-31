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

import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * SubClusterInfo is a report of the runtime information of the subcluster that
 * is participating in federation.
 *
 * It includes information such as:
 * {@link SubClusterId}
 * The URL of the subcluster
 * The timestamp representing the last start time of the subCluster
 * {@code FederationsubClusterState}
 * The current capacity and utilization of the subCluster
*/
@XmlRootElement(name = "subClusterInfoDAO")
@XmlAccessorType(XmlAccessType.FIELD)
public class SubClusterInfoDAO {
  public String subClusterId;
  public String amRMServiceAddress;
  public String clientRMServiceAddress;
  public String rmAdminServiceAddress;
  public String rmWebServiceAddress;
  public long lastHeartBeat;
  public SubClusterStateDAO state;
  public long lastStartTime;
  public String capability;

  public SubClusterInfoDAO() {
  } // JAXB needs this

  public SubClusterInfoDAO(SubClusterInfo scinfo) {
    this.subClusterId = scinfo.getSubClusterId().toString();
    this.amRMServiceAddress = scinfo.getAMRMServiceAddress();
    this.clientRMServiceAddress = scinfo.getClientRMServiceAddress();
    this.rmAdminServiceAddress = scinfo.getRMAdminServiceAddress();
    this.rmWebServiceAddress = scinfo.getRMWebServiceAddress();
    this.lastHeartBeat = scinfo.getLastHeartBeat();
    this.state = new SubClusterStateDAO(scinfo.getState());
    this.lastStartTime = scinfo.getLastStartTime();
    this.capability = scinfo.getCapability();
  }

  public SubClusterInfo toSubClusterInfo() {
    return SubClusterInfo.newInstance(
        SubClusterId.newInstance(subClusterId),
        amRMServiceAddress,
        clientRMServiceAddress,
        rmAdminServiceAddress,
        rmWebServiceAddress,
        this.lastHeartBeat,
        state.getSubClusterState(),
        lastStartTime,
        capability);
  }
}
