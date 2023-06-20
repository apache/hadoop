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
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * The YARN UI doesn't have centralized login mechanism. While accessing UI2 from kerberized shell, user who is
 * placed the request to YARN need to be displayed in UI. Given requests from UI2 is routed via Proxy, only RM can provide
 * the user who has placed the request. This DAO object help to provide the requested user and also RM logged in user.
 * the response sent by RM is authenticated user instead of proxy user.
 * It is always good to display authenticated user in browser which eliminates lot of confusion to end use.
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@InterfaceStability.Unstable
public class ClusterUserInfo {

    // User who has started the RM
    protected String rmLoginUser;
    // User who has placed the request
    protected String requestedUser;

  private String subClusterId;

    public ClusterUserInfo() {
    }

    public ClusterUserInfo(ResourceManager rm, UserGroupInformation ugi) {
        this.rmLoginUser = rm.getRMLoginUser();
        if (ugi != null) {
            this.requestedUser = ugi.getShortUserName();
        } else {
            this.requestedUser = "UNKNOWN_USER";
        }
    }

    public String getRmLoginUser() {
        return rmLoginUser;
    }

    public String getRequestedUser() {
        return requestedUser;
    }

  public String getSubClusterId() {
    return subClusterId;
  }

  public void setSubClusterId(String subClusterId) {
    this.subClusterId = subClusterId;
  }
}
