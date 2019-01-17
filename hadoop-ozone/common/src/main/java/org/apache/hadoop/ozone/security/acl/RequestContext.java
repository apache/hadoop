/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.security.acl;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;

import java.net.InetAddress;

/**
 * This class encapsulates information required for Ozone ACLs.
 * */
public class RequestContext {
  private final String host;
  private final InetAddress ip;
  private final UserGroupInformation clientUgi;
  private final String serviceId;
  private final ACLIdentityType aclType;
  private final ACLType aclRights;

  public RequestContext(String host, InetAddress ip,
      UserGroupInformation clientUgi, String serviceId,
      ACLIdentityType aclType, ACLType aclRights) {
    this.host = host;
    this.ip = ip;
    this.clientUgi = clientUgi;
    this.serviceId = serviceId;
    this.aclType = aclType;
    this.aclRights = aclRights;
  }

  /**
   * Builder class for @{@link RequestContext}.
   */
  public static class Builder {
    private String host;
    private InetAddress ip;
    private UserGroupInformation clientUgi;
    private String serviceId;
    private IAccessAuthorizer.ACLIdentityType aclType;
    private IAccessAuthorizer.ACLType aclRights;

    public Builder setHost(String bHost) {
      this.host = bHost;
      return this;
    }

    public Builder setIp(InetAddress cIp) {
      this.ip = cIp;
      return this;
    }

    public Builder setClientUgi(UserGroupInformation cUgi) {
      this.clientUgi = cUgi;
      return this;
    }

    public Builder setServiceId(String sId) {
      this.serviceId = sId;
      return this;
    }

    public Builder setAclType(ACLIdentityType acl) {
      this.aclType = acl;
      return this;
    }

    public Builder setAclRights(ACLType aclRight) {
      this.aclRights = aclRight;
      return this;
    }

    public RequestContext build() {
      return new RequestContext(host, ip, clientUgi, serviceId, aclType,
          aclRights);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public String getHost() {
    return host;
  }

  public InetAddress getIp() {
    return ip;
  }

  public UserGroupInformation getClientUgi() {
    return clientUgi;
  }

  public String getServiceId() {
    return serviceId;
  }

  public ACLIdentityType getAclType() {
    return aclType;
  }

  public ACLType getAclRights() {
    return aclRights;
  }

}
