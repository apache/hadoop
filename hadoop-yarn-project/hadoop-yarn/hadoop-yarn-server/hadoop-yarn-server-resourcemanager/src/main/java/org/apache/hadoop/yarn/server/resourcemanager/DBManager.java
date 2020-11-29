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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Consumer;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

public class DBManager implements Closeable {
  public static final Logger LOG =
      LoggerFactory.getLogger(DBManager.class);
  private DB db;
  private Timer compactionTimer;

  public DB initDatabase(File configurationFile, Options options,
                         Consumer<DB> initMethod) throws Exception {
    try {
      db = JniDBFactory.factory.open(configurationFile, options);
    } catch (NativeDB.DBException e) {
      if (e.isNotFound() || e.getMessage().contains(" does not exist ")) {
        LOG.info("Creating configuration version/database at {}",
            configurationFile);
        options.createIfMissing(true);
        try {
          db = JniDBFactory.factory.open(configurationFile, options);
          initMethod.accept(db);
        } catch (DBException dbErr) {
          throw new IOException(dbErr.getMessage(), dbErr);
        }
      } else {
        throw e;
      }
    }

    return db;
  }

  public void close() throws IOException {
    if (compactionTimer != null) {
      compactionTimer.cancel();
      compactionTimer = null;
    }
    if (db != null) {
      db.close();
      db = null;
    }
  }

  public void storeVersion(String versionKey, Version versionValue) {
    byte[] data = ((VersionPBImpl) versionValue).getProto().toByteArray();
    db.put(bytes(versionKey), data);
  }

  public Version loadVersion(String versionKey) throws Exception {
    Version version = null;
    try {
      byte[] data = db.get(bytes(versionKey));
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
  public void setDb(DB db) {
    this.db = db;
  }

  public void startCompactionTimer(long compactionIntervalMsec,
                                    String className) {
    if (compactionIntervalMsec > 0) {
      compactionTimer = new Timer(
          className + " compaction timer", true);
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