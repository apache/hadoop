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

package org.apache.hadoop.yarn.security;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.List;

/**
 * This request object contains all the context information to determine whether
 * a user has permission to access the target entity.
 * user       : the user who's currently accessing
 * accessType : the access type against the entity.
 * entity     : the target object user is accessing.
 * appId      : the associated app Id for current access. This could be null
 *              if no app is associated.
 * appName    : the associated app name for current access. This could be null if
 *              no app is associated.
 * remoteAddress : The caller's remote ip address.
 * forwardedAddresses : In case this is an http request, this contains the
 *                    originating IP address of a client connecting to a web
 *                    server through an HTTP proxy or load balancer. This
 *                    parameter is null, if it's a RPC request.
 */
@Public
@Unstable
public class AccessRequest {

  private PrivilegedEntity entity;
  private UserGroupInformation user;
  private AccessType accessType;
  private String appId;
  private String appName;
  private String remoteAddress;
  private List<String> forwardedAddresses;

  public AccessRequest(PrivilegedEntity entity, UserGroupInformation user,
      AccessType accessType, String appId, String appName, String remoteAddress,
      List<String> forwardedAddresses) {
    this.entity = entity;
    this.user = user;
    this.accessType = accessType;
    this.appId = appId;
    this.appName = appName;
    this.remoteAddress = remoteAddress;
    this.forwardedAddresses = forwardedAddresses;
  }

  public UserGroupInformation getUser() {
    return user;
  }

  public AccessType getAccessType() {
    return accessType;
  }

  public String getAppId() {
    return appId;
  }

  public String getAppName() {
    return appName;
  }

  public PrivilegedEntity getEntity() {
    return entity;
  }


  public List<String> getForwardedAddresses() {
    return forwardedAddresses;
  }

  public String getRemoteAddress() {
    return remoteAddress;
  }
}
