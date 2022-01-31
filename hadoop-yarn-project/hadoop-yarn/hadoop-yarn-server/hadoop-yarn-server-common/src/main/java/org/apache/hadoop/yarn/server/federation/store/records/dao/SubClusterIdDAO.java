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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * SubClusterId represents the <em>globally unique</em> identifier for a
 * subcluster that is participating in federation.
 *
 * The globally unique nature of the identifier is obtained from the
 * <code>FederationMembershipStateStore</code> on initialization.
 */
@XmlRootElement(name = "subClustersIdDAO")
@XmlAccessorType(XmlAccessType.FIELD)
public class SubClusterIdDAO {
  public String subClusterId;

  public SubClusterIdDAO() {
  } // JAXB needs this

  public SubClusterIdDAO(SubClusterId scId) {
    subClusterId = scId.getId();
  }

  public SubClusterId toSubClusterId(){
    return SubClusterId.newInstance(subClusterId);
  }
}
