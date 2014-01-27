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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.MRDelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.service.AbstractService;

@Private
@Unstable
/**
 * Base class for history server state storage.
 * Storage implementations need to implement blocking store and load methods
 * to actually store and load the state.
 */
public abstract class HistoryServerStateStoreService extends AbstractService {

  public static class HistoryServerState {
    Map<MRDelegationTokenIdentifier, Long> tokenState =
        new HashMap<MRDelegationTokenIdentifier, Long>();
    Set<DelegationKey> tokenMasterKeyState = new HashSet<DelegationKey>();

    public Map<MRDelegationTokenIdentifier, Long> getTokenState() {
      return tokenState;
    }

    public Set<DelegationKey> getTokenMasterKeyState() {
      return tokenMasterKeyState;
    }
  }

  public HistoryServerStateStoreService() {
    super(HistoryServerStateStoreService.class.getName());
  }

  /**
   * Initialize the state storage
   *
   * @param conf the configuration
   * @throws IOException
   */
  @Override
  public void serviceInit(Configuration conf) throws IOException {
    initStorage(conf);
  }

  /**
   * Start the state storage for use
   *
   * @throws IOException
   */
  @Override
  public void serviceStart() throws IOException {
    startStorage();
  }

  /**
   * Shutdown the state storage.
   * 
   * @throws IOException
   */
  @Override
  public void serviceStop() throws IOException {
    closeStorage();
  }

  /**
   * Implementation-specific initialization.
   * 
   * @param conf the configuration
   * @throws IOException
   */
  protected abstract void initStorage(Configuration conf) throws IOException;

  /**
   * Implementation-specific startup.
   * 
   * @throws IOException
   */
  protected abstract void startStorage() throws IOException;

  /**
   * Implementation-specific shutdown.
   * 
   * @throws IOException
   */
  protected abstract void closeStorage() throws IOException;

  /**
   * Load the history server state from the state storage.
   * 
   * @throws IOException
   */
  public abstract HistoryServerState loadState() throws IOException;

  /**
   * Blocking method to store a delegation token along with the current token
   * sequence number to the state storage.
   * 
   * Implementations must not return from this method until the token has been
   * committed to the state store.
   * 
   * @param tokenId the token to store
   * @param renewDate the token renewal deadline
   * @throws IOException
   */
  public abstract void storeToken(MRDelegationTokenIdentifier tokenId,
      Long renewDate) throws IOException;

  /**
   * Blocking method to update the expiration of a delegation token
   * in the state storage.
   * 
   * Implementations must not return from this method until the expiration
   * date of the token has been updated in the state store.
   * 
   * @param tokenId the token to update
   * @param renewDate the new token renewal deadline
   * @throws IOException
   */
  public abstract void updateToken(MRDelegationTokenIdentifier tokenId,
      Long renewDate) throws IOException;

  /**
   * Blocking method to remove a delegation token from the state storage.
   * 
   * Implementations must not return from this method until the token has been
   * removed from the state store.
   * 
   * @param tokenId the token to remove
   * @throws IOException
   */
  public abstract void removeToken(MRDelegationTokenIdentifier tokenId)
      throws IOException;

  /**
   * Blocking method to store a delegation token master key.
   * 
   * Implementations must not return from this method until the key has been
   * committed to the state store.
   * 
   * @param key the master key to store
   * @throws IOException
   */
  public abstract void storeTokenMasterKey(
      DelegationKey key) throws IOException;

  /**
   * Blocking method to remove a delegation token master key.
   * 
   * Implementations must not return from this method until the key has been
   * removed from the state store.
   * 
   * @param key the master key to remove
   * @throws IOException
   */
  public abstract void removeTokenMasterKey(DelegationKey key)
      throws IOException;
}
