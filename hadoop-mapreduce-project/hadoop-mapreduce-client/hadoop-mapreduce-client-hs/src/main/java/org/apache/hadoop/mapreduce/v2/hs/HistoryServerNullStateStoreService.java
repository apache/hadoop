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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.MRDelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.DelegationKey;

@Private
@Unstable
public class HistoryServerNullStateStoreService
    extends HistoryServerStateStoreService {

  @Override
  protected void initStorage(Configuration conf) throws IOException {
    // Do nothing
  }

  @Override
  protected void startStorage() throws IOException {
    // Do nothing
  }

  @Override
  protected void closeStorage() throws IOException {
    // Do nothing
  }

  @Override
  public HistoryServerState loadState() throws IOException {
    throw new UnsupportedOperationException(
        "Cannot load state from null store");
  }

  @Override
  public void storeToken(MRDelegationTokenIdentifier tokenId, Long renewDate)
      throws IOException {
    // Do nothing
  }

  @Override
  public void updateToken(MRDelegationTokenIdentifier tokenId, Long renewDate)
      throws IOException {
    // Do nothing
  }

  @Override
  public void removeToken(MRDelegationTokenIdentifier tokenId)
      throws IOException {
    // Do nothing
  }

  @Override
  public void storeTokenMasterKey(DelegationKey key) throws IOException {
    // Do nothing
  }

  @Override
  public void removeTokenMasterKey(DelegationKey key) throws IOException {
    // Do nothing
  }
}
