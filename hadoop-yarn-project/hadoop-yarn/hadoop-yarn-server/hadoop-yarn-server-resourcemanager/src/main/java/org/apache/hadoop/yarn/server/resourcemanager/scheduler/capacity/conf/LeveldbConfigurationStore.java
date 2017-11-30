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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB;
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
import java.util.Timer;
import java.util.TimerTask;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

/**
 * A LevelDB implementation of {@link YarnConfigurationStore}.
 */
public class LeveldbConfigurationStore extends YarnConfigurationStore {

  public static final Log LOG =
      LogFactory.getLog(LeveldbConfigurationStore.class);

  private static final String DB_NAME = "yarn-conf-store";
  private static final String LOG_KEY = "log";
  private static final String VERSION_KEY = "version";

  private DB db;
  private long maxLogs;
  private Configuration conf;
  private LogMutation pendingMutation;
  @VisibleForTesting
  protected static final Version CURRENT_VERSION_INFO = Version
      .newInstance(0, 1);
  private Timer compactionTimer;
  private long compactionIntervalMsec;

  @Override
  public void initialize(Configuration config, Configuration schedConf,
      RMContext rmContext) throws IOException {
    this.conf = config;
    try {
      initDatabase(schedConf);
      this.maxLogs = config.getLong(
          YarnConfiguration.RM_SCHEDCONF_MAX_LOGS,
          YarnConfiguration.DEFAULT_RM_SCHEDCONF_LEVELDB_MAX_LOGS);
      this.compactionIntervalMsec = config.getLong(
          YarnConfiguration.RM_SCHEDCONF_LEVELDB_COMPACTION_INTERVAL_SECS,
          YarnConfiguration
              .DEFAULT_RM_SCHEDCONF_LEVELDB_COMPACTION_INTERVAL_SECS) * 1000;
      startCompactionTimer();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void initDatabase(Configuration config) throws Exception {
    Path storeRoot = createStorageDir();
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

    LOG.info("Using conf database at " + storeRoot);
    File dbfile = new File(storeRoot.toString());
    try {
      db = JniDBFactory.factory.open(dbfile, options);
    } catch (NativeDB.DBException e) {
      if (e.isNotFound() || e.getMessage().contains(" does not exist ")) {
        LOG.info("Creating conf database at " + dbfile);
        options.createIfMissing(true);
        try {
          db = JniDBFactory.factory.open(dbfile, options);
          // Write the initial scheduler configuration
          WriteBatch initBatch = db.createWriteBatch();
          for (Map.Entry<String, String> kv : config) {
            initBatch.put(bytes(kv.getKey()), bytes(kv.getValue()));
          }
          db.write(initBatch);
        } catch (DBException dbErr) {
          throw new IOException(dbErr.getMessage(), dbErr);
        }
      } else {
        throw e;
      }
    }
  }

  private Path createStorageDir() throws IOException {
    Path root = getStorageDir();
    FileSystem fs = FileSystem.getLocal(conf);
    fs.mkdirs(root, new FsPermission((short) 0700));
    return root;
  }

  private Path getStorageDir() throws IOException {
    String storePath = conf.get(YarnConfiguration.RM_SCHEDCONF_STORE_PATH);
    if (storePath == null) {
      throw new IOException("No store location directory configured in " +
          YarnConfiguration.RM_SCHEDCONF_STORE_PATH);
    }
    return new Path(storePath, DB_NAME);
  }

  @Override
  public void close() throws IOException {
    if (db != null) {
      db.close();
    }
  }

  @Override
  public void logMutation(LogMutation logMutation) throws IOException {
    LinkedList<LogMutation> logs = deserLogMutations(db.get(bytes(LOG_KEY)));
    logs.add(logMutation);
    if (logs.size() > maxLogs) {
      logs.removeFirst();
    }
    db.put(bytes(LOG_KEY), serLogMutations(logs));
    pendingMutation = logMutation;
  }

  @Override
  public void confirmMutation(boolean isValid) throws IOException {
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
    }
    db.write(updateBatch);
    pendingMutation = null;
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

  @Override
  public List<LogMutation> getConfirmedConfHistory(long fromId) {
    return null; // unimplemented
  }

  // TODO below was taken from LeveldbRMStateStore, it can probably be
  // refactored
  private void startCompactionTimer() {
    if (compactionIntervalMsec > 0) {
      compactionTimer = new Timer(
          this.getClass().getSimpleName() + " compaction timer", true);
      compactionTimer.schedule(new CompactionTimerTask(),
          compactionIntervalMsec, compactionIntervalMsec);
    }
  }

  // TODO: following is taken from LeveldbRMStateStore
  @Override
  public Version getConfStoreVersion() throws Exception {
    Version version = null;
    try {
      byte[] data = db.get(bytes(VERSION_KEY));
      if (data != null) {
        version = new VersionPBImpl(YarnServerCommonProtos.VersionProto
            .parseFrom(data));
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
    return version;
  }

  @VisibleForTesting
  protected LinkedList<LogMutation> getLogs() throws Exception {
    return deserLogMutations(db.get(bytes(LOG_KEY)));
  }

  @Override
  public void storeVersion() throws Exception {
    String key = VERSION_KEY;
    byte[] data = ((VersionPBImpl) CURRENT_VERSION_INFO).getProto()
        .toByteArray();
    try {
      db.put(bytes(key), data);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  private class CompactionTimerTask extends TimerTask {
    @Override
    public void run() {
      long start = Time.monotonicNow();
      LOG.info("Starting full compaction cycle");
      try {
        db.compactRange(null, null);
      } catch (DBException e) {
        LOG.error("Error compacting database", e);
      }
      long duration = Time.monotonicNow() - start;
      LOG.info("Full compaction cycle completed in " + duration + " msec");
    }
  }
}
