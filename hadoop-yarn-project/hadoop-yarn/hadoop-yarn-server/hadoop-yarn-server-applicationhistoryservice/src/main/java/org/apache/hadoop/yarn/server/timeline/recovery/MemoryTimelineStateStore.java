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
package org.apache.hadoop.yarn.server.timeline.recovery;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;

/**
 * A state store backed by memory for unit tests
 */
public class MemoryTimelineStateStore
    extends TimelineStateStore {

  private TimelineServiceState state;

  @Override
  protected void initStorage(Configuration conf) throws IOException {
  }

  @Override
  protected void startStorage() throws IOException {
    state = new TimelineServiceState();
  }

  @Override
  protected void closeStorage() throws IOException {
    state = null;
  }

  @Override
  public TimelineServiceState loadState() throws IOException {
    TimelineServiceState result = new TimelineServiceState();
    result.tokenState.putAll(state.tokenState);
    result.tokenMasterKeyState.addAll(state.tokenMasterKeyState);
    result.latestSequenceNumber = state.latestSequenceNumber;
    return result;
  }

  @Override
  public void storeToken(TimelineDelegationTokenIdentifier tokenId,
      Long renewDate) throws IOException {
    if (state.tokenState.containsKey(tokenId)) {
      throw new IOException("token " + tokenId + " was stored twice");
    }
    state.tokenState.put(tokenId, renewDate);
    state.latestSequenceNumber = tokenId.getSequenceNumber();
  }

  @Override
  public void updateToken(TimelineDelegationTokenIdentifier tokenId,
      Long renewDate) throws IOException {
    if (!state.tokenState.containsKey(tokenId)) {
      throw new IOException("token " + tokenId + " not in store");
    }
    state.tokenState.put(tokenId, renewDate);
  }

  @Override
  public void removeToken(TimelineDelegationTokenIdentifier tokenId)
      throws IOException {
    state.tokenState.remove(tokenId);
  }

  @Override
  public void storeTokenMasterKey(DelegationKey key)
      throws IOException {
    if (state.tokenMasterKeyState.contains(key)) {
      throw new IOException("token master key " + key + " was stored twice");
    }
    state.tokenMasterKeyState.add(key);
  }

  @Override
  public void removeTokenMasterKey(DelegationKey key)
      throws IOException {
    state.tokenMasterKeyState.remove(key);
  }
}
