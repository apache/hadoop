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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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
public class LeveldbConfigurationStore implements YarnConfigurationStore {

  public static final Log LOG =
      LogFactory.getLog(LeveldbConfigurationStore.class);

  private static final String DB_NAME = "yarn-conf-store";
  private static final String LOG_PREFIX = "log.";
  private static final String LOG_COMMITTED_TXN = "committedTxn";

  private DB db;
  // Txnid for the last transaction logged to the store.
  private long txnId = 0;
  private long minTxn = 0;
  private long maxLogs;
  private Configuration conf;
  private LinkedList<LogMutation> pendingMutations = new LinkedList<>();
  private Timer compactionTimer;
  private long compactionIntervalMsec;

  @Override
  public void initialize(Configuration config, Configuration schedConf)
      throws IOException {
    this.conf = config;
    try {
      this.db = initDatabase(schedConf);
      this.txnId = Long.parseLong(new String(db.get(bytes(LOG_COMMITTED_TXN)),
          StandardCharsets.UTF_8));
      DBIterator itr = db.iterator();
      itr.seek(bytes(LOG_PREFIX + txnId));
      // Seek to first uncommitted log
      itr.next();
      while (itr.hasNext()) {
        Map.Entry<byte[], byte[]> entry = itr.next();
        if (!new String(entry.getKey(), StandardCharsets.UTF_8)
            .startsWith(LOG_PREFIX)) {
          break;
        }
        pendingMutations.add(deserLogMutation(entry.getValue()));
        txnId++;
      }
      // Get the earliest txnId stored in logs
      itr.seekToFirst();
      if (itr.hasNext()) {
        Map.Entry<byte[], byte[]> entry = itr.next();
        byte[] key = entry.getKey();
        String logId = new String(key, StandardCharsets.UTF_8);
        if (logId.startsWith(LOG_PREFIX)) {
          minTxn = Long.parseLong(logId.substring(logId.indexOf('.') + 1));
        }
      }
      this.maxLogs = config.getLong(
          YarnConfiguration.RM_SCHEDCONF_LEVELDB_MAX_LOGS,
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

  private DB initDatabase(Configuration config) throws Exception {
    Path storeRoot = createStorageDir();
    Options options = new Options();
    options.createIfMissing(false);
    options.comparator(new DBComparator() {
      @Override
      public int compare(byte[] key1, byte[] key2) {
        String key1Str = new String(key1, StandardCharsets.UTF_8);
        String key2Str = new String(key2, StandardCharsets.UTF_8);
        int key1Txn = Integer.MAX_VALUE;
        int key2Txn = Integer.MAX_VALUE;
        if (key1Str.startsWith(LOG_PREFIX)) {
          key1Txn = Integer.parseInt(key1Str.substring(
              key1Str.indexOf('.') + 1));
        }
        if (key2Str.startsWith(LOG_PREFIX)) {
          key2Txn = Integer.parseInt(key2Str.substring(
              key2Str.indexOf('.') + 1));
        }
        // TODO txnId could overflow, in theory
        if (key1Txn == Integer.MAX_VALUE && key2Txn == Integer.MAX_VALUE) {
          if (key1Str.equals(key2Str) && key1Str.equals(LOG_COMMITTED_TXN)) {
            return 0;
          } else if (key1Str.equals(LOG_COMMITTED_TXN)) {
            return -1;
          } else if (key2Str.equals(LOG_COMMITTED_TXN)) {
            return 1;
          }
          return key1Str.compareTo(key2Str);
        }
        return key1Txn - key2Txn;
      }

      @Override
      public String name() {
        return "logComparator";
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
          initBatch.put(bytes(LOG_COMMITTED_TXN), bytes("0"));
          db.write(initBatch);
        } catch (DBException dbErr) {
          throw new IOException(dbErr.getMessage(), dbErr);
        }
      } else {
        throw e;
      }
    }
    return db;
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
  public synchronized long logMutation(LogMutation logMutation)
      throws IOException {
    logMutation.setId(++txnId);
    WriteBatch logBatch = db.createWriteBatch();
    logBatch.put(bytes(LOG_PREFIX + txnId), serLogMutation(logMutation));
    if (txnId - minTxn >= maxLogs) {
      logBatch.delete(bytes(LOG_PREFIX + minTxn));
      minTxn++;
    }
    db.write(logBatch);
    pendingMutations.add(logMutation);
    return txnId;
  }

  @Override
  public synchronized boolean confirmMutation(long id, boolean isValid)
      throws IOException {
    WriteBatch updateBatch = db.createWriteBatch();
    if (isValid) {
      LogMutation mutation = deserLogMutation(db.get(bytes(LOG_PREFIX + id)));
      for (Map.Entry<String, String> changes :
          mutation.getUpdates().entrySet()) {
        if (changes.getValue() == null || changes.getValue().isEmpty()) {
          updateBatch.delete(bytes(changes.getKey()));
        } else {
          updateBatch.put(bytes(changes.getKey()), bytes(changes.getValue()));
        }
      }
    }
    updateBatch.put(bytes(LOG_COMMITTED_TXN), bytes(String.valueOf(id)));
    db.write(updateBatch);
    // Assumes logMutation and confirmMutation are done in the same
    // synchronized method. For example,
    // {@link MutableCSConfigurationProvider#mutateConfiguration(
    // UserGroupInformation user, SchedConfUpdateInfo confUpdate)}
    pendingMutations.removeFirst();
    return true;
  }

  private byte[] serLogMutation(LogMutation mutation) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutput oos = new ObjectOutputStream(baos)) {
      oos.writeObject(mutation);
      oos.flush();
      return baos.toByteArray();
    }
  }
  private LogMutation deserLogMutation(byte[] mutation) throws IOException {
    try (ObjectInput input = new ObjectInputStream(
        new ByteArrayInputStream(mutation))) {
      return (LogMutation) input.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized Configuration retrieve() {
    DBIterator itr = db.iterator();
    itr.seek(bytes(LOG_COMMITTED_TXN));
    Configuration config = new Configuration(false);
    itr.next();
    while (itr.hasNext()) {
      Map.Entry<byte[], byte[]> entry = itr.next();
      config.set(new String(entry.getKey(), StandardCharsets.UTF_8),
          new String(entry.getValue(), StandardCharsets.UTF_8));
    }
    return config;
  }

  @Override
  public List<LogMutation> getPendingMutations() {
    return new LinkedList<>(pendingMutations);
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
