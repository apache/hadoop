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
package org.apache.hadoop.ozone.security;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.utils.db.Table.KeyValue;
import org.apache.hadoop.utils.db.TableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * SecretStore for Ozone Master.
 */
public class OzoneSecretStore implements Closeable {

  private static final Logger LOG = LoggerFactory
      .getLogger(OzoneSecretStore.class);
  private OMMetadataManager omMetadataManager;
  @Override
  public void close() throws IOException {
    if (omMetadataManager != null) {
      try {
        omMetadataManager.getDelegationTokenTable().close();
      } catch (Exception e) {
        throw new IOException("Error while closing OzoneSecretStore.", e);
      }
    }
  }


  /**
   * Support class to maintain state of OzoneSecretStore.
   */
  public static class OzoneManagerSecretState<T> {
    private Map<T, Long> tokenState = new HashMap<>();
    public Map<T, Long> getTokenState() {
      return tokenState;
    }
  }

  public OzoneSecretStore(OzoneConfiguration conf,
      OMMetadataManager omMetadataManager) {
    this.omMetadataManager = omMetadataManager;
  }

  public OzoneManagerSecretState loadState() throws IOException {
    OzoneManagerSecretState<Integer> state = new OzoneManagerSecretState();
    int numTokens = loadTokens(state);
    LOG.info("Loaded " + numTokens + " tokens");
    return state;
  }

  public void storeToken(OzoneTokenIdentifier tokenId, long renewDate)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing token {}", tokenId.getSequenceNumber());
    }

    try {
      omMetadataManager.getDelegationTokenTable().put(tokenId, renewDate);
    } catch (IOException e) {
      LOG.error("Unable to store token " + tokenId.toString(), e);
      throw e;
    }
  }

  public void updateToken(OzoneTokenIdentifier tokenId, long renewDate)
      throws IOException {
    storeToken(tokenId, renewDate);
  }

  public void removeToken(OzoneTokenIdentifier tokenId) throws IOException {
    try {
      omMetadataManager.getDelegationTokenTable().delete(tokenId);
    } catch (IOException e) {
      LOG.error("Unable to remove token {}", tokenId.toString(), e);
      throw e;
    }
  }

  public int loadTokens(OzoneManagerSecretState state) throws IOException {
    int loadedToken = 0;
    try (TableIterator<OzoneTokenIdentifier, ? extends
        KeyValue<OzoneTokenIdentifier, Long>> iterator =
             omMetadataManager.getDelegationTokenTable().iterator()){
      iterator.seekToFirst();
      while(iterator.hasNext()) {
        KeyValue<OzoneTokenIdentifier, Long> kv = iterator.next();
        state.tokenState.put(kv.getKey(), kv.getValue());
        loadedToken++;
      }
    }
    return loadedToken;
  }
}
