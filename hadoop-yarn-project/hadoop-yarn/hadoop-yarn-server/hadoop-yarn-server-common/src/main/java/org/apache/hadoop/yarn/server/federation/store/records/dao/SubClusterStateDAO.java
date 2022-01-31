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

import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * State of a <code>SubCluster</code>
 */
@XmlRootElement(name = "subClusterStateDAO")
@XmlAccessorType(XmlAccessType.FIELD)
public class SubClusterStateDAO {

  protected enum SCState {
    SC_NEW,

    /**
     * Subcluster is registered and the RM sent a heartbeat recently.
     */
    SC_RUNNING,

    /**
     * Subcluster is unhealthy.
     */
    SC_UNHEALTHY,

    /**
     * Subcluster is in the process of being out of service.
     */
    SC_DECOMMISSIONING,

    /**
     * Subcluster is out of service.
     */
    SC_DECOMMISSIONED,

    /**
     * RM has not sent a heartbeat for some configured time threshold.
     */
    SC_LOST,

    /**
     * Subcluster has unregistered.
     */
    SC_UNREGISTERED;
  }

  ;

  protected SCState scState;

  public SubClusterStateDAO() {
  } // JAXB needs this

  public SubClusterStateDAO(SubClusterState state) {
    scState = SCState.valueOf(state.toString());
  }

  public SubClusterState getSubClusterState() {
    return SubClusterState.valueOf(scState.toString());
  }
}
