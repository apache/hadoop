/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.federation.store;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.federation.store.records.StoreNewMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.StoreNewMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RemoveStoredMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RemoveStoredMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreNewTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreNewTokenResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterUpdateStoredTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterUpdateStoredTokenResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRemoveStoredTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRemoveStoredTokenResponse;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface FederationDelegationTokenStateStore {

  StoreNewMasterKeyResponse storeNewMasterKey(StoreNewMasterKeyRequest request) throws Exception;

  RemoveStoredMasterKeyResponse removeStoredMasterKey(RemoveStoredMasterKeyRequest request)
      throws Exception;

  RouterStoreNewTokenResponse storeNewToken(RouterStoreNewTokenRequest request) throws Exception;

  RouterUpdateStoredTokenResponse updateStoredToken(RouterUpdateStoredTokenRequest request)
      throws Exception;

  RouterRemoveStoredTokenResponse removeStoredToken(RouterRemoveStoredTokenRequest request)
      throws Exception;
}
