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

import static org.apache.hadoop.yarn.server.timeline.util.LeveldbUtils.prefixMatches;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.timeline.recovery.records.TimelineDelegationTokenIdentifierData;
import org.apache.hadoop.yarn.server.timeline.util.LeveldbUtils.KeyBuilder;
import org.apache.hadoop.yarn.server.utils.LeveldbIterator;
import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

/**
 * A timeline service state storage implementation that supports any persistent
 * storage that adheres to the LevelDB interface.
 */
public class LeveldbTimelineStateStore extends
    TimelineStateStore {

  public static final Log LOG =
      LogFactory.getLog(LeveldbTimelineStateStore.class);

  private static final String DB_NAME = "timeline-state-store.ldb";
  private static final FsPermission LEVELDB_DIR_UMASK = FsPermission
      .createImmutable((short) 0700);

  private static final byte[] TOKEN_ENTRY_PREFIX = bytes("t");
  private static final byte[] TOKEN_MASTER_KEY_ENTRY_PREFIX = bytes("k");
  private static final byte[] LATEST_SEQUENCE_NUMBER_KEY = bytes("s");

  private static final Version CURRENT_VERSION_INFO = Version.newInstance(1, 0);
  private static final byte[] TIMELINE_STATE_STORE_VERSION_KEY = bytes("v");

  private DB db;

  public LeveldbTimelineStateStore() {
    super(LeveldbTimelineStateStore.class.getName());
  }

  @Override
  protected void initStorage(Configuration conf) throws IOException {
  }

  @Override
  protected void startStorage() throws IOException {
    Options options = new Options();
    Path dbPath =
        new Path(
            getConfig().get(
                YarnConfiguration.TIMELINE_SERVICE_LEVELDB_STATE_STORE_PATH),
            DB_NAME);
    FileSystem localFS = null;
    try {
      localFS = FileSystem.getLocal(getConfig());
      if (!localFS.exists(dbPath)) {
        if (!localFS.mkdirs(dbPath)) {
          throw new IOException("Couldn't create directory for leveldb " +
              "timeline store " + dbPath);
        }
        localFS.setPermission(dbPath, LEVELDB_DIR_UMASK);
      }
    } finally {
      IOUtils.cleanup(LOG, localFS);
    }
    JniDBFactory factory = new JniDBFactory();
    try {
      options.createIfMissing(false);
      db = factory.open(new File(dbPath.toString()), options);
      LOG.info("Loading the existing database at th path: " + dbPath.toString());
      checkVersion();
    } catch (NativeDB.DBException e) {
      if (e.isNotFound() || e.getMessage().contains(" does not exist ")) {
        try {
          options.createIfMissing(true);
          db = factory.open(new File(dbPath.toString()), options);
          LOG.info("Creating a new database at th path: " + dbPath.toString());
          storeVersion(CURRENT_VERSION_INFO);
        } catch (DBException ex) {
          throw new IOException(ex);
        }
      } else {
        throw new IOException(e);
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void closeStorage() throws IOException {
    IOUtils.cleanup(LOG, db);
  }

  @Override
  public TimelineServiceState loadState() throws IOException {
    LOG.info("Loading timeline service state from leveldb");
    TimelineServiceState state = new TimelineServiceState();
    int numKeys = loadTokenMasterKeys(state);
    int numTokens = loadTokens(state);
    loadLatestSequenceNumber(state);
    LOG.info("Loaded " + numKeys + " master keys and " + numTokens
        + " tokens from leveldb, and latest sequence number is "
        + state.getLatestSequenceNumber());
    return state;
  }

  @Override
  public void storeToken(TimelineDelegationTokenIdentifier tokenId,
      Long renewDate) throws IOException {
    DataOutputStream ds = null;
    WriteBatch batch = null;
    try {
      byte[] k = createTokenEntryKey(tokenId.getSequenceNumber());
      if (db.get(k) != null) {
        throw new IOException(tokenId + " already exists");
      }
      byte[] v = buildTokenData(tokenId, renewDate);
      ByteArrayOutputStream bs = new ByteArrayOutputStream();
      ds = new DataOutputStream(bs);
      ds.writeInt(tokenId.getSequenceNumber());
      batch = db.createWriteBatch();
      batch.put(k, v);
      batch.put(LATEST_SEQUENCE_NUMBER_KEY, bs.toByteArray());
      db.write(batch);
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      IOUtils.cleanup(LOG, ds);
      IOUtils.cleanup(LOG, batch);
    }
  }

  @Override
  public void updateToken(TimelineDelegationTokenIdentifier tokenId,
      Long renewDate) throws IOException {
    try {
      byte[] k = createTokenEntryKey(tokenId.getSequenceNumber());
      if (db.get(k) == null) {
        throw new IOException(tokenId + " doesn't exist");
      }
      byte[] v = buildTokenData(tokenId, renewDate);
      db.put(k, v);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void removeToken(TimelineDelegationTokenIdentifier tokenId)
      throws IOException {
    try {
      byte[] key = createTokenEntryKey(tokenId.getSequenceNumber());
      db.delete(key);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void storeTokenMasterKey(DelegationKey key) throws IOException {
    try {
      byte[] k = createTokenMasterKeyEntryKey(key.getKeyId());
      if (db.get(k) != null) {
        throw new IOException(key + " already exists");
      }
      byte[] v = buildTokenMasterKeyData(key);
      db.put(k, v);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void removeTokenMasterKey(DelegationKey key) throws IOException {
    try {
      byte[] k = createTokenMasterKeyEntryKey(key.getKeyId());
      db.delete(k);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  private static byte[] buildTokenData(
      TimelineDelegationTokenIdentifier tokenId, Long renewDate)
      throws IOException {
    TimelineDelegationTokenIdentifierData data =
        new TimelineDelegationTokenIdentifierData(tokenId, renewDate);
    return data.toByteArray();
  }

  private static byte[] buildTokenMasterKeyData(DelegationKey key)
      throws IOException {
    ByteArrayOutputStream memStream = new ByteArrayOutputStream();
    DataOutputStream dataStream = new DataOutputStream(memStream);
    try {
      key.write(dataStream);
      dataStream.close();
    } finally {
      IOUtils.cleanup(LOG, dataStream);
    }
    return memStream.toByteArray();
  }

  private static void loadTokenMasterKeyData(TimelineServiceState state,
      byte[] keyData)
      throws IOException {
    DelegationKey key = new DelegationKey();
    DataInputStream in =
        new DataInputStream(new ByteArrayInputStream(keyData));
    try {
      key.readFields(in);
    } finally {
      IOUtils.cleanup(LOG, in);
    }
    state.tokenMasterKeyState.add(key);
  }

  private static void loadTokenData(TimelineServiceState state, byte[] tokenData)
      throws IOException {
    TimelineDelegationTokenIdentifierData data =
        new TimelineDelegationTokenIdentifierData();
    DataInputStream in =
        new DataInputStream(new ByteArrayInputStream(tokenData));
    try {
      data.readFields(in);
    } finally {
      IOUtils.cleanup(LOG, in);
    }
    state.tokenState.put(data.getTokenIdentifier(), data.getRenewDate());
  }

  private int loadTokenMasterKeys(TimelineServiceState state)
      throws IOException {
    byte[] base = KeyBuilder.newInstance().add(TOKEN_MASTER_KEY_ENTRY_PREFIX)
        .getBytesForLookup();
    int numKeys = 0;
    LeveldbIterator iterator = null;
    try {
      for (iterator = new LeveldbIterator(db), iterator.seek(base);
          iterator.hasNext(); iterator.next()) {
        byte[] k = iterator.peekNext().getKey();
        if (!prefixMatches(base, base.length, k)) {
          break;
        }
        byte[] v = iterator.peekNext().getValue();
        loadTokenMasterKeyData(state, v);
        ++numKeys;
      }
    } finally {
      IOUtils.cleanup(LOG, iterator);
    }
    return numKeys;
  }

  private int loadTokens(TimelineServiceState state) throws IOException {
    byte[] base = KeyBuilder.newInstance().add(TOKEN_ENTRY_PREFIX)
        .getBytesForLookup();
    int numTokens = 0;
    LeveldbIterator iterator = null;
    try {
      for (iterator = new LeveldbIterator(db), iterator.seek(base);
          iterator.hasNext(); iterator.next()) {
        byte[] k = iterator.peekNext().getKey();
        if (!prefixMatches(base, base.length, k)) {
          break;
        }
        byte[] v = iterator.peekNext().getValue();
        loadTokenData(state, v);
        ++numTokens;
      }
    } catch (DBException e) {
      throw new IOException(e);
    } finally {
      IOUtils.cleanup(LOG, iterator);
    }
    return numTokens;
  }

  private void loadLatestSequenceNumber(TimelineServiceState state)
      throws IOException {
    byte[] data = null;
    try {
      data = db.get(LATEST_SEQUENCE_NUMBER_KEY);
    } catch (DBException e) {
      throw new IOException(e);
    }
    if (data != null) {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
      try {
        state.latestSequenceNumber = in.readInt();
      } finally {
        IOUtils.cleanup(LOG, in);
      }
    }
  }
  /**
   * Creates a domain entity key with column name suffix, of the form
   * TOKEN_ENTRY_PREFIX + sequence number.
   */
  private static byte[] createTokenEntryKey(int seqNum) throws IOException {
    return KeyBuilder.newInstance().add(TOKEN_ENTRY_PREFIX)
        .add(Integer.toString(seqNum)).getBytes();
  }

  /**
   * Creates a domain entity key with column name suffix, of the form
   * TOKEN_MASTER_KEY_ENTRY_PREFIX + sequence number.
   */
  private static byte[] createTokenMasterKeyEntryKey(int keyId)
      throws IOException {
    return KeyBuilder.newInstance().add(TOKEN_MASTER_KEY_ENTRY_PREFIX)
        .add(Integer.toString(keyId)).getBytes();
  }

  @VisibleForTesting
  Version loadVersion() throws IOException {
    try {
      byte[] data = db.get(TIMELINE_STATE_STORE_VERSION_KEY);
      // if version is not stored previously, treat it as CURRENT_VERSION_INFO.
      if (data == null || data.length == 0) {
        return getCurrentVersion();
      }
      Version version =
          new VersionPBImpl(
              YarnServerCommonProtos.VersionProto.parseFrom(data));
      return version;
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @VisibleForTesting
  void storeVersion(Version state) throws IOException {
    byte[] data =
        ((VersionPBImpl) state).getProto().toByteArray();
    try {
      db.put(TIMELINE_STATE_STORE_VERSION_KEY, data);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @VisibleForTesting
  Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  /**
   * 1) Versioning timeline state store:
   * major.minor. For e.g. 1.0, 1.1, 1.2...1.25, 2.0 etc.
   * 2) Any incompatible change of TS-store is a major upgrade, and any
   * compatible change of TS-store is a minor upgrade.
   * 3) Within a minor upgrade, say 1.1 to 1.2:
   * overwrite the version info and proceed as normal.
   * 4) Within a major upgrade, say 1.2 to 2.0:
   * throw exception and indicate user to use a separate upgrade tool to
   * upgrade timeline store or remove incompatible old state.
   */
  private void checkVersion() throws IOException {
    Version loadedVersion = loadVersion();
    LOG.info("Loaded timeline state store version info " + loadedVersion);
    if (loadedVersion.equals(getCurrentVersion())) {
      return;
    }
    if (loadedVersion.isCompatibleTo(getCurrentVersion())) {
      LOG.info("Storing timeline state store version info " + getCurrentVersion());
      storeVersion(CURRENT_VERSION_INFO);
    } else {
      String incompatibleMessage =
          "Incompatible version for timeline state store: expecting version "
              + getCurrentVersion() + ", but loading version " + loadedVersion;
      LOG.fatal(incompatibleMessage);
      throw new IOException(incompatibleMessage);
    }
  }

}
