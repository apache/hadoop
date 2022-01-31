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

import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.util.Apps;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * ApplicationHomeSubCluster is a report of the runtime information of the
 * application that is running in the federated cluster.
 *
 * It includes information such as: ApplicationId, SubClusterId
 */
@XmlRootElement(name = "applicationHomeSubClusterDAO")
@XmlAccessorType(XmlAccessType.FIELD)
public class ApplicationHomeSubClusterDAO {

  protected String appId;
  protected SubClusterIdDAO subClusterId;

  public ApplicationHomeSubClusterDAO(){
  } // JAXB needs this

  public ApplicationHomeSubClusterDAO(ApplicationHomeSubCluster appHome){
    appId = appHome.getApplicationId().toString();
    subClusterId = new SubClusterIdDAO(appHome.getHomeSubCluster());
  }

  public ApplicationHomeSubClusterDAO(
      AddApplicationHomeSubClusterRequest request){
    this(request.getApplicationHomeSubCluster());
  }

  public ApplicationHomeSubClusterDAO(
      UpdateApplicationHomeSubClusterRequest request){
    this(request.getApplicationHomeSubCluster());
  }

  public ApplicationHomeSubCluster toApplicationHomeSubCluster(){
    return ApplicationHomeSubCluster.newInstance(Apps.toAppID(appId),
        subClusterId.toSubClusterId());
  }

  public String getAppId(){
    return appId;
  }

  public SubClusterIdDAO getSubClusterId(){
    return subClusterId;
  }
}
