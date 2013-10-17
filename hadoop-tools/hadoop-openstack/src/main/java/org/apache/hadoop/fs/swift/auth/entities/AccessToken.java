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

package org.apache.hadoop.fs.swift.auth.entities;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

/**
 * Access token representation of Openstack Keystone authentication.
 * Class holds token id, tenant and expiration time.
 * THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON.
 * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 *
 * Example:
 * <pre>
 * "token" : {
 *   "RAX-AUTH:authenticatedBy" : [ "APIKEY" ],
 *   "expires" : "2013-07-12T05:19:24.685-05:00",
 *   "id" : "8bbea4215113abdab9d4c8fb0d37",
 *   "tenant" : { "id" : "01011970",
 *   "name" : "77777"
 *   }
 *  }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)

public class AccessToken {
  /**
   * token expiration time
   */
  private String expires;
  /**
   * token id
   */
  private String id;
  /**
   * tenant name for whom id is attached
   */
  private Tenant tenant;

  /**
   * @return token expiration time
   */
  public String getExpires() {
    return expires;
  }

  /**
   * @param expires the token expiration time
   */
  public void setExpires(String expires) {
    this.expires = expires;
  }

  /**
   * @return token value
   */
  public String getId() {
    return id;
  }

  /**
   * @param id token value
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * @return tenant authenticated in Openstack Keystone
   */
  public Tenant getTenant() {
    return tenant;
  }

  /**
   * @param tenant tenant authenticated in Openstack Keystone
   */
  public void setTenant(Tenant tenant) {
    this.tenant = tenant;
  }

  @Override
  public String toString() {
    return "AccessToken{" +
            "id='" + id + '\'' +
            ", tenant=" + tenant +
            ", expires='" + expires + '\'' +
            '}';
  }
}
