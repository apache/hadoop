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
package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterStoreTokenProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class RouterStoreToken {
  private RouterStoreTokenProto.Builder builder = RouterStoreTokenProto.newBuilder();

  public RouterStoreToken() {}

  public RouterStoreToken(YARNDelegationTokenIdentifier identifier, Long renewdate) {
    builder.setTokenIdentifier(identifier.getProto());
    builder.setRenewDate(renewdate);
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static RouterStoreToken newInstance(YARNDelegationTokenIdentifier identifier,
      Long renewdate) {
    RouterStoreToken storeToken = Records.newRecord(RouterStoreToken.class);
    storeToken.setIdentifier(identifier);
    storeToken.setRenewDate(renewdate);
    return storeToken;
  }

  public void readFields(DataInput in) throws IOException {
    builder.mergeFrom((DataInputStream) in);
  }

  public byte[] toByteArray() throws IOException {
    return builder.build().toByteArray();
  }

  public RMDelegationTokenIdentifier getTokenIdentifier() throws IOException {
    ByteArrayInputStream in =
        new ByteArrayInputStream(builder.getTokenIdentifier().toByteArray());
    RMDelegationTokenIdentifier identifier = new RMDelegationTokenIdentifier();
    identifier.readFields(new DataInputStream(in));
    return identifier;
  }

  public Long getRenewDate() {
    return builder.getRenewDate();
  }

  public void setIdentifier(YARNDelegationTokenIdentifier identifier) {
    builder.setTokenIdentifier(identifier.getProto());
  }

  public void setRenewDate(Long renewDate) {
    builder.setRenewDate(renewDate);
  }
}
