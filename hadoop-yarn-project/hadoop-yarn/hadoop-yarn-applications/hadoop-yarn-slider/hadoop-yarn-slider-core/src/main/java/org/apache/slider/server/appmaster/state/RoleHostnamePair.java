/*
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

package org.apache.slider.server.appmaster.state;

import java.util.Objects;

public class RoleHostnamePair {

  /**
   * requested role
   */
  public final int roleId;

  /**
   * hostname -will be null if node==null
   */
  public final String hostname;

  public RoleHostnamePair(int roleId, String hostname) {
    this.roleId = roleId;
    this.hostname = hostname;
  }

  public int getRoleId() {
    return roleId;
  }

  public String getHostname() {
    return hostname;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RoleHostnamePair)) {
      return false;
    }
    RoleHostnamePair that = (RoleHostnamePair) o;
    return Objects.equals(roleId, that.roleId) &&
        Objects.equals(hostname, that.hostname);
  }

  @Override
  public int hashCode() {
    return Objects.hash(roleId, hostname);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "RoleHostnamePair{");
    sb.append("roleId=").append(roleId);
    sb.append(", hostname='").append(hostname).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
