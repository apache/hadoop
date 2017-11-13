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

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.v2.api.MRDelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.utils.LeveldbIterator;
import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HistoryServerLeveldbStateStoreService extends
    HistoryServerStateStoreService {

  private static final String DB_NAME = "mr-jhs-state";
  private static final String DB_SCHEMA_VERSION_KEY = "jhs-schema-version";
  private static final String TOKEN_MASTER_KEY_KEY_PREFIX = "tokens/key_";
  private static final String TOKEN_STATE_KEY_PREFIX = "tokens/token_";

  private static final Version CURRENT_VERSION_INFO =
      Version.newInstance(1, 0);

  private DB db;

  public static final Logger LOG =
      LoggerFactory.getLogger(HistoryServerLeveldbStateStoreService.class);

  @Override
  protected void initStorage(Configuration conf) throws IOException {
  }

  @Override
  protected void startStorage() throws IOException {
    Path storeRoot = createStorageDir(getConfig());
    Options options = new Options();
    options.createIfMissing(false);
    options.logger(new LeveldbLogger());
    LOG.info("Using state database at " + storeRoot + " for recovery");
    File dbfile = new File(storeRoot.toString());
    try {
      db = JniDBFactory.factory.open(dbfile, options);
    } catch (NativeDB.DBException e) {
      if (e.isNotFound() || e.getMessage().contains(" does not exist ")) {
        LOG.info("Creating state database at " + dbfile);
        options.createIfMissing(true);
        try {
          db = JniDBFactory.factory.open(dbfile, options);
          // store version
          storeVersion();
        } catch (DBException dbErr) {
          throw new IOException(dbErr.getMessage(), dbErr);
        }
      } else {
        throw e;
      }
    }
    checkVersion();
  }

  @Override
  protected void closeStorage() throws IOException {
    if (db != null) {
      db.close();
      db = null;
    }
  }

  @Override
  public HistoryServerState loadState() throws IOException {
    HistoryServerState state = new HistoryServerState();
    int numKeys = loadTokenMasterKeys(state);
    LOG.info("Recovered " + numKeys + " token master keys");
    int numTokens = loadTokens(state);
    LOG.info("Recovered " + numTokens + " tokens");
    return state;
  }

  private int loadTokenMasterKeys(HistoryServerState state)
      throws IOException {
    int numKeys = 0;
    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      iter.seek(bytes(TOKEN_MASTER_KEY_KEY_PREFIX));
      while (iter.hasNext()) {
        Entry<byte[],byte[]> entry = iter.next();
        String key = asString(entry.getKey());
        if (!key.startsWith(TOKEN_MASTER_KEY_KEY_PREFIX)) {
          break;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Loading master key from " + key);
        }
        try {
          loadTokenMasterKey(state, entry.getValue());
        } catch (IOException e) {
          throw new IOException("Error loading token master key from " + key,
              e);
        }
        ++numKeys;
      }
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }
    return numKeys;
  }

  private void loadTokenMasterKey(HistoryServerState state, byte[] data)
      throws IOException {
    DelegationKey key = new DelegationKey();
    DataInputStream in =
        new DataInputStream(new ByteArrayInputStream(data));
    try {
      key.readFields(in);
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }
    state.tokenMasterKeyState.add(key);
  }

  private int loadTokens(HistoryServerState state) throws IOException {
    int numTokens = 0;
    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      iter.seek(bytes(TOKEN_STATE_KEY_PREFIX));
      while (iter.hasNext()) {
        Entry<byte[],byte[]> entry = iter.next();
        String key = asString(entry.getKey());
        if (!key.startsWith(TOKEN_STATE_KEY_PREFIX)) {
          break;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Loading token from " + key);
        }
        try {
          loadToken(state, entry.getValue());
        } catch (IOException e) {
          throw new IOException("Error loading token state from " + key, e);
        }
        ++numTokens;
      }
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }
    return numTokens;
  }

  private void loadToken(HistoryServerState state, byte[] data)
      throws IOException {
    MRDelegationTokenIdentifier tokenId = new MRDelegationTokenIdentifier();
    long renewDate;
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
    try {
      tokenId.readFields(in);
      renewDate = in.readLong();
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }
    state.tokenState.put(tokenId, renewDate);
  }

  @Override
  public void storeToken(MRDelegationTokenIdentifier tokenId, Long renewDate)
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

    String dbKey = getTokenDatabaseKey(tokenId);
    try {
      db.put(bytes(dbKey), memStream.toByteArray());
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void updateToken(MRDelegationTokenIdentifier tokenId, Long renewDate)
      throws IOException {
    storeToken(tokenId, renewDate);
  }

  @Override
  public void removeToken(MRDelegationTokenIdentifier tokenId)
      throws IOException {
    String dbKey = getTokenDatabaseKey(tokenId);
    try {
      db.delete(bytes(dbKey));
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  private String getTokenDatabaseKey(MRDelegationTokenIdentifier tokenId) {
    return TOKEN_STATE_KEY_PREFIX + tokenId.getSequenceNumber();
  }

  @Override
  public void storeTokenMasterKey(DelegationKey masterKey)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing master key " + masterKey.getKeyId());
    }

    ByteArrayOutputStream memStream = new ByteArrayOutputStream();
    DataOutputStream dataStream = new DataOutputStream(memStream);
    try {
      masterKey.write(dataStream);
      dataStream.close();
      dataStream = null;
    } finally {
      IOUtils.cleanupWithLogger(LOG, dataStream);
    }

    String dbKey = getTokenMasterKeyDatabaseKey(masterKey);
    try {
      db.put(bytes(dbKey), memStream.toByteArray());
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void removeTokenMasterKey(DelegationKey masterKey)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing master key " + masterKey.getKeyId());
    }

    String dbKey = getTokenMasterKeyDatabaseKey(masterKey);
    try {
      db.delete(bytes(dbKey));
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  private String getTokenMasterKeyDatabaseKey(DelegationKey masterKey) {
    return TOKEN_MASTER_KEY_KEY_PREFIX + masterKey.getKeyId();
  }

  private Path createStorageDir(Configuration conf) throws IOException {
    String confPath = conf.get(JHAdminConfig.MR_HS_LEVELDB_STATE_STORE_PATH);
    if (confPath == null) {
      throw new IOException("No store location directory configured in " +
          JHAdminConfig.MR_HS_LEVELDB_STATE_STORE_PATH);
    }
    Path root = new Path(confPath, DB_NAME);
    FileSystem fs = FileSystem.getLocal(conf);
    fs.mkdirs(root, new FsPermission((short)0700));
    return root;
  }

  Version loadVersion() throws IOException {
    byte[] data = db.get(bytes(DB_SCHEMA_VERSION_KEY));
    // if version is not stored previously, treat it as 1.0.
    if (data == null || data.length == 0) {
      return Version.newInstance(1, 0);
    }
    Version version =
        new VersionPBImpl(VersionProto.parseFrom(data));
    return version;
  }

  private void storeVersion() throws IOException {
    dbStoreVersion(CURRENT_VERSION_INFO);
  }

  void dbStoreVersion(Version state) throws IOException {
    String key = DB_SCHEMA_VERSION_KEY;
    byte[] data =
        ((VersionPBImpl) state).getProto().toByteArray();
    try {
      db.put(bytes(key), data);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  /**
   * 1) Versioning scheme: major.minor. For e.g. 1.0, 1.1, 1.2...1.25, 2.0 etc.
   * 2) Any incompatible change of state-store is a major upgrade, and any
   *    compatible change of state-store is a minor upgrade.
   * 3) Within a minor upgrade, say 1.1 to 1.2:
   *    overwrite the version info and proceed as normal.
   * 4) Within a major upgrade, say 1.2 to 2.0:
   *    throw exception and indicate user to use a separate upgrade tool to
   *    upgrade state or remove incompatible old state.
   */
  private void checkVersion() throws IOException {
    Version loadedVersion = loadVersion();
    LOG.info("Loaded state version info " + loadedVersion);
    if (loadedVersion.equals(getCurrentVersion())) {
      return;
    }
    if (loadedVersion.isCompatibleTo(getCurrentVersion())) {
      LOG.info("Storing state version info " + getCurrentVersion());
      storeVersion();
    } else {
      throw new IOException(
        "Incompatible version for state: expecting state version "
            + getCurrentVersion() + ", but loading version " + loadedVersion);
    }
  }

  private static class LeveldbLogger implements org.iq80.leveldb.Logger {
    private static final Logger LOG =
        LoggerFactory.getLogger(LeveldbLogger.class);

    @Override
    public void log(String message) {
      LOG.info(message);
    }
  }
}
