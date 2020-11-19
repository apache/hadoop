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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.server.resourcemanager.DBManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

/**
 * A LevelDB implementation of {@link YarnConfigurationStore}.
 */
public class LeveldbConfigurationStore extends YarnConfigurationStore {

  public static final Logger LOG =
      LoggerFactory.getLogger(LeveldbConfigurationStore.class);

  private static final String DB_NAME = "yarn-conf-store";
  private static final String LOG_KEY = "log";
  private static final String VERSION_KEY = "version";
  private static final String CONF_VERSION_NAME = "conf-version-store";
  private static final String CONF_VERSION_KEY = "conf-version";
  private DB db;
  private DBManager dbManager;
  private DBManager versionDbManager;
  private DB versionDb;
  private long maxLogs;
  private Configuration conf;
  private Configuration initSchedConf;
  @VisibleForTesting
  protected static final Version CURRENT_VERSION_INFO = Version
      .newInstance(0, 1);

  @Override
  public void initialize(Configuration config, Configuration schedConf,
      RMContext rmContext) throws IOException {
    this.conf = config;
    this.initSchedConf = schedConf;
    this.dbManager = new DBManager();
    this.versionDbManager = new DBManager();
    try {
      initDatabase();
      this.maxLogs = config.getLong(
          YarnConfiguration.RM_SCHEDCONF_MAX_LOGS,
          YarnConfiguration.DEFAULT_RM_SCHEDCONF_LEVELDB_MAX_LOGS);
      long compactionIntervalMsec = config.getLong(
          YarnConfiguration.RM_SCHEDCONF_LEVELDB_COMPACTION_INTERVAL_SECS,
          YarnConfiguration
              .DEFAULT_RM_SCHEDCONF_LEVELDB_COMPACTION_INTERVAL_SECS) * 1000;
      dbManager.startCompactionTimer(compactionIntervalMsec,
          this.getClass().getSimpleName());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void format() throws Exception {
    close();
    FileSystem fs = FileSystem.getLocal(conf);
    fs.delete(getStorageDir(DB_NAME), true);
  }

  private void initDatabase() throws Exception {
    Path confVersion = createStorageDir(CONF_VERSION_NAME);
    Options confOptions = new Options();
    confOptions.createIfMissing(false);
    File confVersionFile = new File(confVersion.toString());

    versionDb = versionDbManager.initDatabase(confVersionFile, confOptions,
        this::initVersionDb);

    Path storeRoot = createStorageDir(DB_NAME);
    Options options = new Options();
    options.createIfMissing(false);
    options.comparator(new DBComparator() {
      @Override
      public int compare(byte[] key1, byte[] key2) {
        String key1Str = new String(key1, StandardCharsets.UTF_8);
        String key2Str = new String(key2, StandardCharsets.UTF_8);
        if (key1Str.equals(key2Str)) {
          return 0;
        } else if (key1Str.equals(VERSION_KEY)) {
          return 1;
        } else if (key2Str.equals(VERSION_KEY)) {
          return -1;
        } else if (key1Str.equals(LOG_KEY)) {
          return 1;
        } else if (key2Str.equals(LOG_KEY)) {
          return -1;
        }
        return key1Str.compareTo(key2Str);
      }

      @Override
      public String name() {
        return "keyComparator";
      }

      public byte[] findShortestSeparator(byte[] start, byte[] limit) {
        return start;
      }

      public byte[] findShortSuccessor(byte[] key) {
        return key;
      }
    });
    LOG.info("Using conf database at {}", storeRoot);
    File dbFile = new File(storeRoot.toString());
    db = dbManager.initDatabase(dbFile, options, this::initDb);
  }

  private void initVersionDb(DB database) {
    database.put(bytes(CONF_VERSION_KEY), bytes(String.valueOf(0)));
  }

  private void initDb(DB database) {
    WriteBatch initBatch = database.createWriteBatch();
    for (Map.Entry<String, String> kv : initSchedConf) {
      initBatch.put(bytes(kv.getKey()), bytes(kv.getValue()));
    }
    database.write(initBatch);
    increaseConfigVersion();
  }

  private Path createStorageDir(String storageName) throws IOException {
    Path root = getStorageDir(storageName);
    FileSystem fs = FileSystem.getLocal(conf);
    fs.mkdirs(root, new FsPermission((short) 0700));
    return root;
  }

  private Path getStorageDir(String storageName) throws IOException {
    String storePath = conf.get(YarnConfiguration.RM_SCHEDCONF_STORE_PATH);
    if (storePath == null) {
      throw new IOException("No store location directory configured in " +
          YarnConfiguration.RM_SCHEDCONF_STORE_PATH);
    }
    return new Path(storePath, storageName);
  }

  @Override
  public void close() throws IOException {
    dbManager.close();
    versionDbManager.close();
  }

  @Override
  public void logMutation(LogMutation logMutation) throws IOException {
    if (maxLogs > 0) {
      LinkedList<LogMutation> logs = deserLogMutations(db.get(bytes(LOG_KEY)));
      logs.add(logMutation);
      if (logs.size() > maxLogs) {
        logs.removeFirst();
      }
      db.put(bytes(LOG_KEY), serLogMutations(logs));
    }
  }

  @Override
  public void confirmMutation(LogMutation pendingMutation,
      boolean isValid) {
    WriteBatch updateBatch = db.createWriteBatch();
    if (isValid) {
      for (Map.Entry<String, String> changes :
          pendingMutation.getUpdates().entrySet()) {
        if (changes.getValue() == null || changes.getValue().isEmpty()) {
          updateBatch.delete(bytes(changes.getKey()));
        } else {
          updateBatch.put(bytes(changes.getKey()), bytes(changes.getValue()));
        }
      }
      increaseConfigVersion();
    }
    db.write(updateBatch);
  }

  private byte[] serLogMutations(LinkedList<LogMutation> mutations) throws
      IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutput oos = new ObjectOutputStream(baos)) {
      oos.writeObject(mutations);
      oos.flush();
      return baos.toByteArray();
    }
  }

  // Because of type erasure casting to LinkedList<LogMutation> will be
  // unchecked. A way around that would be to iterate over the logMutations
  // which is overkill in this case.
  @SuppressWarnings("unchecked")
  private LinkedList<LogMutation> deserLogMutations(byte[] mutations) throws
      IOException {
    if (mutations == null) {
      return new LinkedList<>();
    }

    try (ObjectInput input = new ObjectInputStream(
        new ByteArrayInputStream(mutations))) {
      return (LinkedList<LogMutation>) input.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized Configuration retrieve() {
    DBIterator itr = db.iterator();
    itr.seekToFirst();
    Configuration config = new Configuration(false);
    while (itr.hasNext()) {
      Map.Entry<byte[], byte[]> entry = itr.next();
      String key = new String(entry.getKey(), StandardCharsets.UTF_8);
      String value = new String(entry.getValue(), StandardCharsets.UTF_8);
      if (key.equals(LOG_KEY) || key.equals(VERSION_KEY)) {
        break;
      }
      config.set(key, value);
    }
    return config;
  }

  private void increaseConfigVersion() {
    long configVersion = getConfigVersion() + 1L;
    versionDb.put(bytes(CONF_VERSION_KEY),
        bytes(String.valueOf(configVersion)));
  }

  @Override
  public long getConfigVersion() {
    String version = new String(versionDb.get(bytes(CONF_VERSION_KEY)),
        StandardCharsets.UTF_8);
    return Long.parseLong(version);
  }

  @Override
  public List<LogMutation> getConfirmedConfHistory(long fromId) {
    return null; // unimplemented
  }

  @Override
  public Version getConfStoreVersion() throws Exception {
    return dbManager.loadVersion(VERSION_KEY);
  }

  @VisibleForTesting
  @Override
  protected LinkedList<LogMutation> getLogs() throws Exception {
    return deserLogMutations(db.get(bytes(LOG_KEY)));
  }

  @VisibleForTesting
  protected DB getDB() {
    return db;
  }

  @Override
  public void storeVersion() throws Exception {
    try {
      storeVersion(CURRENT_VERSION_INFO);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @VisibleForTesting
  protected void storeVersion(Version version) {
    dbManager.storeVersion(VERSION_KEY, version);
  }

  @Override
  public Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }
}
