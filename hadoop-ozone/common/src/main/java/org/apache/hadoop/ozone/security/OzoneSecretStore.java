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
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.utils.MetadataKeyFilters;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.MetadataStoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hdds.server.ServerUtils.getOzoneMetaDirPath;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_MANAGER_TOKEN_DB_NAME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_CACHE_SIZE_MB;

/**
 * SecretStore for Ozone Master.
 */
public class OzoneSecretStore<T extends OzoneTokenIdentifier>
    implements Closeable {

  private static final Logger LOG = LoggerFactory
      .getLogger(OzoneSecretStore.class);
  private static final String TOKEN_MASTER_KEY_KEY_PREFIX = "tokens/key_";
  private static final String TOKEN_STATE_KEY_PREFIX = "tokens/token_";

  @Override
  public void close() throws IOException {
    if (store != null) {
      store.close();
    }
  }


  /**
   * Support class to maintain state of OzoneSecretStore.
   */
  public static class OzoneManagerSecretState<T> {

    private Map<T, Long> tokenState = new HashMap<>();
    private Set<OzoneSecretKey> tokenMasterKeyState = new HashSet<>();

    public Map<T, Long> getTokenState() {
      return tokenState;
    }

    public Set<OzoneSecretKey> ozoneManagerSecretState() {
      return tokenMasterKeyState;
    }
  }

  private MetadataStore store;

  public OzoneSecretStore(OzoneConfiguration conf)
      throws IOException {
    File metaDir = getOzoneMetaDirPath(conf);
    final int cacheSize = conf.getInt(OZONE_OM_DB_CACHE_SIZE_MB,
        OZONE_OM_DB_CACHE_SIZE_DEFAULT);
    File omTokenDBFile = new File(metaDir.getPath(),
        OZONE_MANAGER_TOKEN_DB_NAME);
    this.store = MetadataStoreBuilder.newBuilder()
        .setConf(conf)
        .setDbFile(omTokenDBFile)
        .setCacheSize(cacheSize * OzoneConsts.MB)
        .build();
  }

  public OzoneManagerSecretState loadState() throws IOException {
    OzoneManagerSecretState state = new OzoneManagerSecretState();
    int numKeys = loadMasterKeys(state);
    LOG.info("Loaded " + numKeys + " token master keys");
    int numTokens = loadTokens(state);
    LOG.info("Loaded " + numTokens + " tokens");
    return state;
  }

  public void storeTokenMasterKey(OzoneSecretKey key) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing master key " + key.getKeyId());
    }
    ByteArrayOutputStream memStream = new ByteArrayOutputStream();
    DataOutputStream dataStream = new DataOutputStream(memStream);
    try {
      key.write(dataStream);
      dataStream.close();
      dataStream = null;
    } finally {
      IOUtils.cleanupWithLogger(LOG, dataStream);
    }
    try {
      byte[] dbKey = getMasterKeyDBKey(key);
      store.put(dbKey, memStream.toByteArray());
    } catch (IOException e) {
      LOG.error("Unable to store master key " + key.getKeyId(), e);
      throw e;
    }
  }


  public void removeTokenMasterKey(OzoneSecretKey key)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing master key " + key.getKeyId());
    }

    byte[] dbKey = getMasterKeyDBKey(key);
    try {
      store.delete(dbKey);
    } catch (IOException e) {
      LOG.error("Unable to delete master key " + key.getKeyId(), e);
      throw e;
    }
  }

  public void storeToken(T tokenId, Long renewDate)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing token " + tokenId.getSequenceNumber());
    }

    ByteArrayOutputStream memStream = new ByteArrayOutputStream();
    DataOutputStream dataStream = new DataOutputStream(memStream);
    try {
      tokenId.write(dataStream);
      dataStream.writeLong(renewDate);
      dataStream.close();
      dataStream = null;
    } finally {
      IOUtils.cleanupWithLogger(LOG, dataStream);
    }

    byte[] dbKey = getTokenDBKey(tokenId);
    try {
      store.put(dbKey, memStream.toByteArray());
    } catch (IOException e) {
      LOG.error("Unable to store token " + tokenId.toString(), e);
      throw e;
    }
  }

  public void updateToken(T tokenId, Long renewDate)
      throws IOException {
    storeToken(tokenId, renewDate);
  }

  public void removeToken(T tokenId)
      throws IOException {
    byte[] dbKey = getTokenDBKey(tokenId);
    try {
      store.delete(dbKey);
    } catch (IOException e) {
      LOG.error("Unable to remove token " + tokenId.toString(), e);
      throw e;
    }
  }

  public int loadMasterKeys(OzoneManagerSecretState state) throws IOException {
    MetadataKeyFilters.MetadataKeyFilter filter =
        (preKey, currentKey, nextKey) -> DFSUtil.bytes2String(currentKey)
            .startsWith(TOKEN_MASTER_KEY_KEY_PREFIX);
    List<Map.Entry<byte[], byte[]>> kvs = store
        .getRangeKVs(null, Integer.MAX_VALUE, filter);
    kvs.forEach(entry -> {
      try {
        loadTokenMasterKey(state, entry.getValue());
      } catch (IOException e) {
        LOG.warn("Failed to load master key ",
            DFSUtil.bytes2String(entry.getKey()), e);
      }
    });
    return kvs.size();
  }

  private void loadTokenMasterKey(OzoneManagerSecretState state, byte[] data)
      throws IOException {
    OzoneSecretKey key = OzoneSecretKey.readProtoBuf(data);
    state.tokenMasterKeyState.add(key);
  }

  public int loadTokens(OzoneManagerSecretState state) throws IOException {
    MetadataKeyFilters.MetadataKeyFilter filter =
        (preKey, currentKey, nextKey) -> DFSUtil.bytes2String(currentKey)
            .startsWith(TOKEN_STATE_KEY_PREFIX);
    List<Map.Entry<byte[], byte[]>> kvs =
        store.getRangeKVs(null, Integer.MAX_VALUE, filter);
    kvs.forEach(entry -> {
      try {
        loadToken(state, entry.getValue());
      } catch (IOException e) {
        LOG.warn("Failed to load token ",
            DFSUtil.bytes2String(entry.getKey()), e);
      }
    });
    return kvs.size();
  }

  private void loadToken(OzoneManagerSecretState state, byte[] data)
      throws IOException {
    long renewDate;
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
    T tokenId = (T) T.readProtoBuf(in);
    try {
      tokenId.readFields(in);
      renewDate = in.readLong();
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }
    state.tokenState.put(tokenId, renewDate);
  }

  private byte[] getMasterKeyDBKey(OzoneSecretKey masterKey) {
    return DFSUtil.string2Bytes(
        TOKEN_MASTER_KEY_KEY_PREFIX + masterKey.getKeyId());
  }

  private byte[] getTokenDBKey(T tokenId) {
    return DFSUtil.string2Bytes(
        TOKEN_STATE_KEY_PREFIX + tokenId.getSequenceNumber());
  }
}
