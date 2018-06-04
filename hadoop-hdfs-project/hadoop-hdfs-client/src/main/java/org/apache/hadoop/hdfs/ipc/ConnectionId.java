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
package org.apache.hadoop.hdfs.ipc;

import java.net.InetSocketAddress;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.UserGroupInformation;

@InterfaceAudience.Private
class ConnectionId {

  private static final int PRIME = 16777619;

  private final UserGroupInformation ticket;
  private final String protocolName;
  private final InetSocketAddress address;

  public ConnectionId(UserGroupInformation ticket, String protocolName,
      InetSocketAddress address) {
    this.ticket = ticket;
    this.protocolName = protocolName;
    this.address = address;
  }

  UserGroupInformation getTicket() {
    return ticket;
  }

  String getProtocolName() {
    return protocolName;
  }

  InetSocketAddress getAddress() {
    return address;
  }

  @Override
  public int hashCode() {
    int h = ticket == null ? 0 : ticket.hashCode();
    h = PRIME * h + protocolName.hashCode();
    h = PRIME * h + address.hashCode();
    return h;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ConnectionId) {
      ConnectionId id = (ConnectionId) obj;
      return address.equals(id.address) &&
          ((ticket != null && ticket.equals(id.ticket)) ||
              (ticket == id.ticket)) &&
          protocolName.equals(id.protocolName);
    }
    return false;
  }
}