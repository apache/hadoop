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
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.security.UserGroupInformation;

import java.net.InetSocketAddress;

public class FederationConnectionId extends Client.ConnectionId {
  private final int index;

  public FederationConnectionId(InetSocketAddress address, Class<?> protocol,
      UserGroupInformation ticket, int rpcTimeout,
      RetryPolicy connectionRetryPolicy, Configuration conf, int index) {
    super(address, protocol, ticket, rpcTimeout, connectionRetryPolicy, conf);
    this.index = index;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(super.hashCode())
        .append(this.index)
        .toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    if (obj instanceof FederationConnectionId) {
      FederationConnectionId other = (FederationConnectionId)obj;
      return new EqualsBuilder()
          .append(this.index, other.index)
          .isEquals();
    }
    return false;
  }
}
