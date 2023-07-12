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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;

import java.io.DataInput;
import java.io.IOException;

@Private
@Unstable
public abstract class RouterStoreToken {

  @Private
  @Unstable
  public static RouterStoreToken newInstance(YARNDelegationTokenIdentifier identifier,
      Long renewdate) {
    RouterStoreToken storeToken = Records.newRecord(RouterStoreToken.class);
    storeToken.setIdentifier(identifier);
    storeToken.setRenewDate(renewdate);
    return storeToken;
  }

  @Private
  @Unstable
  public static RouterStoreToken newInstance(YARNDelegationTokenIdentifier identifier,
      Long renewdate, String tokenInfo) {
    RouterStoreToken storeToken = Records.newRecord(RouterStoreToken.class);
    storeToken.setIdentifier(identifier);
    storeToken.setRenewDate(renewdate);
    storeToken.setTokenInfo(tokenInfo);
    return storeToken;
  }

  @Private
  @Unstable
  public abstract YARNDelegationTokenIdentifier getTokenIdentifier() throws IOException;

  @Private
  @Unstable
  public abstract void setIdentifier(YARNDelegationTokenIdentifier identifier);

  @Public
  @Stable
  public abstract Long getRenewDate();

  @Private
  @Unstable
  public abstract void setRenewDate(Long renewDate);

  @Private
  @Unstable
  public abstract byte[] toByteArray() throws IOException;

  @Private
  @Unstable
  public abstract void readFields(DataInput in) throws IOException;

  @Public
  @Stable
  public abstract String getTokenInfo();

  @Private
  @Unstable
  public abstract void setTokenInfo(String tokenInfo);
}
