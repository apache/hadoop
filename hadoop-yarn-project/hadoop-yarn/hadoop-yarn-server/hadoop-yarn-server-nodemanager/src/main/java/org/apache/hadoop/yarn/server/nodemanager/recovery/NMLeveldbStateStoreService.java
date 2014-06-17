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

package org.apache.hadoop.yarn.server.nodemanager.recovery;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto;
import org.apache.hadoop.yarn.server.utils.LeveldbIterator;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Logger;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

public class NMLeveldbStateStoreService extends NMStateStoreService {

  public static final Log LOG =
      LogFactory.getLog(NMLeveldbStateStoreService.class);

  private static final String DB_NAME = "yarn-nm-state";
  private static final String DB_SCHEMA_VERSION_KEY = "schema-version";
  private static final String DB_SCHEMA_VERSION = "1.0";

  private static final String DELETION_TASK_KEY_PREFIX =
      "DeletionService/deltask_";

  private static final String LOCALIZATION_KEY_PREFIX = "Localization/";
  private static final String LOCALIZATION_PUBLIC_KEY_PREFIX =
      LOCALIZATION_KEY_PREFIX + "public/";
  private static final String LOCALIZATION_PRIVATE_KEY_PREFIX =
      LOCALIZATION_KEY_PREFIX + "private/";
  private static final String LOCALIZATION_STARTED_SUFFIX = "started/";
  private static final String LOCALIZATION_COMPLETED_SUFFIX = "completed/";
  private static final String LOCALIZATION_FILECACHE_SUFFIX = "filecache/";
  private static final String LOCALIZATION_APPCACHE_SUFFIX = "appcache/";

  private DB db;

  public NMLeveldbStateStoreService() {
    super(NMLeveldbStateStoreService.class.getName());
  }

  @Override
  protected void startStorage() throws IOException {
  }

  @Override
  protected void closeStorage() throws IOException {
    if (db != null) {
      db.close();
    }
  }


  @Override
  public RecoveredLocalizationState loadLocalizationState()
      throws IOException {
    RecoveredLocalizationState state = new RecoveredLocalizationState();

    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      iter.seek(bytes(LOCALIZATION_PUBLIC_KEY_PREFIX));
      state.publicTrackerState = loadResourceTrackerState(iter,
          LOCALIZATION_PUBLIC_KEY_PREFIX);

      iter.seek(bytes(LOCALIZATION_PRIVATE_KEY_PREFIX));
      while (iter.hasNext()) {
        Entry<byte[],byte[]> entry = iter.peekNext();
        String key = asString(entry.getKey());
        if (!key.startsWith(LOCALIZATION_PRIVATE_KEY_PREFIX)) {
          break;
        }

        int userEndPos = key.indexOf('/',
            LOCALIZATION_PRIVATE_KEY_PREFIX.length());
        if (userEndPos < 0) {
          throw new IOException("Unable to determine user in resource key: "
              + key);
        }
        String user = key.substring(
            LOCALIZATION_PRIVATE_KEY_PREFIX.length(), userEndPos);
        state.userResources.put(user, loadUserLocalizedResources(iter,
            key.substring(0, userEndPos+1)));
      }
    } catch (DBException e) {
      throw new IOException(e.getMessage(), e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }

    return state;
  }

  private LocalResourceTrackerState loadResourceTrackerState(
      LeveldbIterator iter, String keyPrefix) throws IOException {
    final String completedPrefix = keyPrefix + LOCALIZATION_COMPLETED_SUFFIX;
    final String startedPrefix = keyPrefix + LOCALIZATION_STARTED_SUFFIX;
    LocalResourceTrackerState state = new LocalResourceTrackerState();
    while (iter.hasNext()) {
      Entry<byte[],byte[]> entry = iter.peekNext();
      String key = asString(entry.getKey());
      if (!key.startsWith(keyPrefix)) {
        break;
      }

      if (key.startsWith(completedPrefix)) {
        state.localizedResources = loadCompletedResources(iter,
            completedPrefix);
      } else if (key.startsWith(startedPrefix)) {
        state.inProgressResources = loadStartedResources(iter, startedPrefix);
      } else {
        throw new IOException("Unexpected key in resource tracker state: "
            + key);
      }
    }

    return state;
  }

  private List<LocalizedResourceProto> loadCompletedResources(
      LeveldbIterator iter, String keyPrefix) throws IOException {
    List<LocalizedResourceProto> rsrcs =
        new ArrayList<LocalizedResourceProto>();
    while (iter.hasNext()) {
      Entry<byte[],byte[]> entry = iter.peekNext();
      String key = asString(entry.getKey());
      if (!key.startsWith(keyPrefix)) {
        break;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Loading completed resource from " + key);
      }
      rsrcs.add(LocalizedResourceProto.parseFrom(entry.getValue()));
      iter.next();
    }

    return rsrcs;
  }

  private Map<LocalResourceProto, Path> loadStartedResources(
      LeveldbIterator iter, String keyPrefix) throws IOException {
    Map<LocalResourceProto, Path> rsrcs =
        new HashMap<LocalResourceProto, Path>();
    while (iter.hasNext()) {
      Entry<byte[],byte[]> entry = iter.peekNext();
      String key = asString(entry.getKey());
      if (!key.startsWith(keyPrefix)) {
        break;
      }

      Path localPath = new Path(key.substring(keyPrefix.length()));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Loading in-progress resource at " + localPath);
      }
      rsrcs.put(LocalResourceProto.parseFrom(entry.getValue()), localPath);
      iter.next();
    }

    return rsrcs;
  }

  private RecoveredUserResources loadUserLocalizedResources(
      LeveldbIterator iter, String keyPrefix) throws IOException {
    RecoveredUserResources userResources = new RecoveredUserResources();
    while (iter.hasNext()) {
      Entry<byte[],byte[]> entry = iter.peekNext();
      String key = asString(entry.getKey());
      if (!key.startsWith(keyPrefix)) {
        break;
      }

      if (key.startsWith(LOCALIZATION_FILECACHE_SUFFIX, keyPrefix.length())) {
        userResources.privateTrackerState = loadResourceTrackerState(iter,
            keyPrefix + LOCALIZATION_FILECACHE_SUFFIX);
      } else if (key.startsWith(LOCALIZATION_APPCACHE_SUFFIX,
          keyPrefix.length())) {
        int appIdStartPos = keyPrefix.length() +
            LOCALIZATION_APPCACHE_SUFFIX.length();
        int appIdEndPos = key.indexOf('/', appIdStartPos);
        if (appIdEndPos < 0) {
          throw new IOException("Unable to determine appID in resource key: "
              + key);
        }
        ApplicationId appId = ConverterUtils.toApplicationId(
            key.substring(appIdStartPos, appIdEndPos));
        userResources.appTrackerStates.put(appId,
            loadResourceTrackerState(iter, key.substring(0, appIdEndPos+1)));
      } else {
        throw new IOException("Unexpected user resource key " + key);
      }
    }
    return userResources;
  }

  @Override
  public void startResourceLocalization(String user, ApplicationId appId,
      LocalResourceProto proto, Path localPath) throws IOException {
    String key = getResourceStartedKey(user, appId, localPath.toString());
    try {
      db.put(bytes(key), proto.toByteArray());
    } catch (DBException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public void finishResourceLocalization(String user, ApplicationId appId,
      LocalizedResourceProto proto) throws IOException {
    String localPath = proto.getLocalPath();
    String startedKey = getResourceStartedKey(user, appId, localPath);
    String completedKey = getResourceCompletedKey(user, appId, localPath);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing localized resource to " + completedKey);
    }
    try {
      WriteBatch batch = db.createWriteBatch();
      try {
        batch.delete(bytes(startedKey));
        batch.put(bytes(completedKey), proto.toByteArray());
        db.write(batch);
      } finally {
        batch.close();
      }
    } catch (DBException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public void removeLocalizedResource(String user, ApplicationId appId,
      Path localPath) throws IOException {
    String localPathStr = localPath.toString();
    String startedKey = getResourceStartedKey(user, appId, localPathStr);
    String completedKey = getResourceCompletedKey(user, appId, localPathStr);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing local resource at " + localPathStr);
    }
    try {
      WriteBatch batch = db.createWriteBatch();
      try {
        batch.delete(bytes(startedKey));
        batch.delete(bytes(completedKey));
        db.write(batch);
      } finally {
        batch.close();
      }
    } catch (DBException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  private String getResourceStartedKey(String user, ApplicationId appId,
      String localPath) {
    return getResourceTrackerKeyPrefix(user, appId)
        + LOCALIZATION_STARTED_SUFFIX + localPath;
  }

  private String getResourceCompletedKey(String user, ApplicationId appId,
      String localPath) {
    return getResourceTrackerKeyPrefix(user, appId)
        + LOCALIZATION_COMPLETED_SUFFIX + localPath;
  }

  private String getResourceTrackerKeyPrefix(String user,
      ApplicationId appId) {
    if (user == null) {
      return LOCALIZATION_PUBLIC_KEY_PREFIX;
    }
    if (appId == null) {
      return LOCALIZATION_PRIVATE_KEY_PREFIX + user + "/"
          + LOCALIZATION_FILECACHE_SUFFIX;
    }
    return LOCALIZATION_PRIVATE_KEY_PREFIX + user + "/"
        + LOCALIZATION_APPCACHE_SUFFIX + appId + "/";
  }


  @Override
  public RecoveredDeletionServiceState loadDeletionServiceState()
      throws IOException {
    RecoveredDeletionServiceState state = new RecoveredDeletionServiceState();
    state.tasks = new ArrayList<DeletionServiceDeleteTaskProto>();
    LeveldbIterator iter = null;
    try {
      iter = new LeveldbIterator(db);
      iter.seek(bytes(DELETION_TASK_KEY_PREFIX));
      while (iter.hasNext()) {
        Entry<byte[], byte[]> entry = iter.next();
        String key = asString(entry.getKey());
        if (!key.startsWith(DELETION_TASK_KEY_PREFIX)) {
          break;
        }
        state.tasks.add(
            DeletionServiceDeleteTaskProto.parseFrom(entry.getValue()));
      }
    } catch (DBException e) {
      throw new IOException(e.getMessage(), e);
    } finally {
      if (iter != null) {
        iter.close();
      }
    }
    return state;
  }

  @Override
  public void storeDeletionTask(int taskId,
      DeletionServiceDeleteTaskProto taskProto) throws IOException {
    String key = DELETION_TASK_KEY_PREFIX + taskId;
    try {
      db.put(bytes(key), taskProto.toByteArray());
    } catch (DBException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public void removeDeletionTask(int taskId) throws IOException {
    String key = DELETION_TASK_KEY_PREFIX + taskId;
    try {
      db.delete(bytes(key));
    } catch (DBException e) {
      throw new IOException(e.getMessage(), e);
    }
  }


  @Override
  protected void initStorage(Configuration conf)
      throws IOException {
    Path storeRoot = createStorageDir(conf);
    Options options = new Options();
    options.createIfMissing(false);
    options.logger(new LeveldbLogger());
    LOG.info("Using state database at " + storeRoot + " for recovery");
    File dbfile = new File(storeRoot.toString());
    byte[] schemaVersionData = null;
    try {
      db = JniDBFactory.factory.open(dbfile, options);
      try {
        schemaVersionData = db.get(bytes(DB_SCHEMA_VERSION_KEY));
      } catch (DBException e) {
        throw new IOException(e.getMessage(), e);
      }
    } catch (NativeDB.DBException e) {
      if (e.isNotFound() || e.getMessage().contains(" does not exist ")) {
        LOG.info("Creating state database at " + dbfile);
        options.createIfMissing(true);
        try {
          db = JniDBFactory.factory.open(dbfile, options);
          schemaVersionData = bytes(DB_SCHEMA_VERSION);
          db.put(bytes(DB_SCHEMA_VERSION_KEY), schemaVersionData);
        } catch (DBException dbErr) {
          throw new IOException(dbErr.getMessage(), dbErr);
        }
      } else {
        throw e;
      }
    }
    if (schemaVersionData != null) {
      String schemaVersion = asString(schemaVersionData);
      // only support exact schema matches for now
      if (!DB_SCHEMA_VERSION.equals(schemaVersion)) {
        throw new IOException("Incompatible state database schema, found "
            + schemaVersion + " expected " + DB_SCHEMA_VERSION);
      }
    } else {
      throw new IOException("State database schema version not found");
    }
  }

  private Path createStorageDir(Configuration conf) throws IOException {
    final String storeUri = conf.get(YarnConfiguration.NM_RECOVERY_DIR);
    if (storeUri == null) {
      throw new IOException("No store location directory configured in " +
          YarnConfiguration.NM_RECOVERY_DIR);
    }

    Path root = new Path(storeUri, DB_NAME);
    FileSystem fs = FileSystem.getLocal(conf);
    fs.mkdirs(root, new FsPermission((short)0700));
    return root;
  }


  private static class LeveldbLogger implements Logger {
    private static final Log LOG = LogFactory.getLog(LeveldbLogger.class);

    @Override
    public void log(String message) {
      LOG.info(message);
    }
  }
}
