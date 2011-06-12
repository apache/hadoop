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
package org.apache.hadoop.security.authorize;

import java.security.Permission;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * {@link Permission} to initiate a connection to a given service.
 */
public class ConnectionPermission extends Permission {

  private static final long serialVersionUID = 1L;
  private final Class<?> protocol;

  /**
   * {@link ConnectionPermission} for a given service.
   * @param protocol service to be accessed
   */
  public ConnectionPermission(Class<?> protocol) {
    super(protocol.getName());
    this.protocol = protocol;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ConnectionPermission) {
      return protocol == ((ConnectionPermission)obj).protocol;
    }
    return false;
  }

  @Override
  public String getActions() {
    return "ALLOW";
  }

  @Override
  public int hashCode() {
    return protocol.hashCode();
  }

  @Override
  public boolean implies(Permission permission) {
    if (permission instanceof ConnectionPermission) {
      ConnectionPermission that = (ConnectionPermission)permission;
      if (that.protocol.equals(VersionedProtocol.class)) {
        return true;
      }
      return this.protocol.equals(that.protocol);
    }
    return false;
  }

  public String toString() {
    return "ConnectionPermission(" + protocol.getName() + ")";
  }
}
