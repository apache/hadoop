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

package org.apache.hadoop.yarn.server.timeline;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains the logic to lookup a leveldb by timestamp so that multiple smaller
 * databases can roll according to the configured period and evicted efficiently
 * via operating system directory removal.
 */
class RollingLevelDB {

  /** Logger for this class. */
  private static final Logger LOG = LoggerFactory.
      getLogger(RollingLevelDB.class);
  /** Factory to open and create new leveldb instances. */
  private static JniDBFactory factory = new JniDBFactory();
  /** Thread safe date formatter. */
  private FastDateFormat fdf;
  /** Date parser. */
  private SimpleDateFormat sdf;
  /** Calendar to calculate the current and next rolling period. */
  private GregorianCalendar cal = new GregorianCalendar(
      TimeZone.getTimeZone("GMT"));
  /** Collection of all active rolling leveldb instances. */
  private final TreeMap<Long, DB> rollingdbs;
  /** Collection of all rolling leveldb instances to evict. */
  private final TreeMap<Long, DB> rollingdbsToEvict;
  /** Name of this rolling level db. */
  private final String name;
  /** Calculated timestamp of when to roll a new leveldb instance. */
  private volatile long nextRollingCheckMillis = 0;
  /** File system instance to find and create new leveldb instances. */
  private FileSystem lfs = null;
  /** Directory to store rolling leveldb instances. */
  private Path rollingDBPath;
  /** Configuration for this object. */
  private Configuration conf;
  /** Rolling period. */
  private RollingPeriod rollingPeriod;
  /**
   * Rolling leveldb instances are evicted when their endtime is earlier than
   * the current time minus the time to live value.
   */
  private long ttl;
  /** Whether time to live is enabled. */
  private boolean ttlEnabled;

  /** Encapsulates the rolling period to date format lookup. */
  enum RollingPeriod {
    DAILY {
      @Override
      public String dateFormat() {
        return "yyyy-MM-dd";
      }
    },
    HALF_DAILY {
      @Override
      public String dateFormat() {
        return "yyyy-MM-dd-HH";
      }
    },
    QUARTER_DAILY {
      @Override
      public String dateFormat() {
        return "yyyy-MM-dd-HH";
      }
    },
    HOURLY {
      @Override
      public String dateFormat() {
        return "yyyy-MM-dd-HH";
      }
    },
    MINUTELY {
      @Override
      public String dateFormat() {
        return "yyyy-MM-dd-HH-mm";
      }
    };
    public abstract String dateFormat();
  }

  /**
   * Convenience class for associating a write batch with its rolling leveldb
   * instance.
   */
  public static class RollingWriteBatch {
    /** Leveldb object. */
    private final DB db;
    /** Write batch for the db object. */
    private final WriteBatch writeBatch;

    public RollingWriteBatch(final DB db, final WriteBatch writeBatch) {
      this.db = db;
      this.writeBatch = writeBatch;
    }

    public DB getDB() {
      return db;
    }

    public WriteBatch getWriteBatch() {
      return writeBatch;
    }

    public void write() {
      db.write(writeBatch);
    }

    public void close() {
      IOUtils.cleanupWithLogger(LOG, writeBatch);
    }
  }

  RollingLevelDB(String name) {
    this.name = name;
    this.rollingdbs = new TreeMap<Long, DB>();
    this.rollingdbsToEvict = new TreeMap<Long, DB>();
  }

  protected String getName() {
    return name;
  }

  protected long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  public long getNextRollingTimeMillis() {
    return nextRollingCheckMillis;
  }

  public long getTimeToLive() {
    return ttl;
  }

  public boolean getTimeToLiveEnabled() {
    return ttlEnabled;
  }

  protected void setNextRollingTimeMillis(final long timestamp) {
    this.nextRollingCheckMillis = timestamp;
    LOG.info("Next rolling time for " + getName() + " is "
        + fdf.format(nextRollingCheckMillis));
  }

  public void init(final Configuration config) throws Exception {
    LOG.info("Initializing RollingLevelDB for " + getName());
    this.conf = config;
    this.ttl = conf.getLong(YarnConfiguration.TIMELINE_SERVICE_TTL_MS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_TTL_MS);
    this.ttlEnabled = conf.getBoolean(
        YarnConfiguration.TIMELINE_SERVICE_TTL_ENABLE, true);
    this.rollingDBPath = new Path(
        conf.get(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH),
        RollingLevelDBTimelineStore.FILENAME);
    initFileSystem();
    initRollingPeriod();
    initHistoricalDBs();
  }

  protected void initFileSystem() throws IOException {
    lfs = FileSystem.getLocal(conf);
    boolean success = lfs.mkdirs(rollingDBPath,
        RollingLevelDBTimelineStore.LEVELDB_DIR_UMASK);
    if (!success) {
      throw new IOException("Failed to create leveldb root directory "
          + rollingDBPath);
    }
  }

  protected synchronized void initRollingPeriod() {
    final String lcRollingPeriod = conf.get(
        YarnConfiguration.TIMELINE_SERVICE_ROLLING_PERIOD,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ROLLING_PERIOD);
    this.rollingPeriod = RollingPeriod.valueOf(lcRollingPeriod
        .toUpperCase(Locale.ENGLISH));
    fdf = FastDateFormat.getInstance(rollingPeriod.dateFormat(),
        TimeZone.getTimeZone("GMT"));
    sdf = new SimpleDateFormat(rollingPeriod.dateFormat());
    sdf.setTimeZone(fdf.getTimeZone());
  }

  protected synchronized void initHistoricalDBs() throws IOException {
    Path rollingDBGlobPath = new Path(rollingDBPath, getName() + ".*");
    FileStatus[] statuses = lfs.globStatus(rollingDBGlobPath);
    for (FileStatus status : statuses) {
      String dbName = FilenameUtils.getExtension(status.getPath().toString());
      try {
        Long dbStartTime = sdf.parse(dbName).getTime();
        initRollingLevelDB(dbStartTime, status.getPath());
      } catch (ParseException pe) {
        LOG.warn("Failed to initialize rolling leveldb " + dbName + " for "
            + getName());
      }
    }
  }

  private void initRollingLevelDB(Long dbStartTime,
      Path rollingInstanceDBPath) {
    if (rollingdbs.containsKey(dbStartTime)) {
      return;
    }
    Options options = new Options();
    options.createIfMissing(true);
    options.cacheSize(conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE));
    options.maxOpenFiles(conf.getInt(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES));
    options.writeBufferSize(conf.getInt(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE));
    LOG.info("Initializing rolling leveldb instance :" + rollingInstanceDBPath
        + " for start time: " + dbStartTime);
    DB db = null;
    try {
      db = factory.open(
          new File(rollingInstanceDBPath.toUri().getPath()), options);
      rollingdbs.put(dbStartTime, db);
      String dbName = fdf.format(dbStartTime);
      LOG.info("Added rolling leveldb instance " + dbName + " to " + getName());
    } catch (IOException ioe) {
      LOG.warn("Failed to open rolling leveldb instance :"
          + new File(rollingInstanceDBPath.toUri().getPath()), ioe);
    }
  }

  synchronized DB getPreviousDB(DB db) {
    Iterator<DB> iterator = rollingdbs.values().iterator();
    DB prev = null;
    while (iterator.hasNext()) {
      DB cur = iterator.next();
      if (cur == db) {
        break;
      }
      prev = cur;
    }
    return prev;
  }

  synchronized long getStartTimeFor(DB db) {
    long startTime = -1;
    for (Map.Entry<Long, DB> entry : rollingdbs.entrySet()) {
      if (entry.getValue() == db) {
        startTime = entry.getKey();
      }
    }
    return startTime;
  }

  public synchronized DB getDBForStartTime(long startTime) {
    // make sure we sanitize this input
    startTime = Math.min(startTime, currentTimeMillis());

    if (startTime >= getNextRollingTimeMillis()) {
      roll(startTime);
    }
    Entry<Long, DB> entry = rollingdbs.floorEntry(startTime);
    if (entry == null) {
      return null;
    }
    return entry.getValue();
  }

  private void roll(long startTime) {
    LOG.info("Rolling new DB instance for " + getName());
    long currentStartTime = computeCurrentCheckMillis(startTime);
    setNextRollingTimeMillis(computeNextCheckMillis(currentStartTime));
    String currentRollingDBInstance = fdf.format(currentStartTime);
    String currentRollingDBName = getName() + "." + currentRollingDBInstance;
    Path currentRollingDBPath = new Path(rollingDBPath, currentRollingDBName);
    if (getTimeToLiveEnabled()) {
      scheduleOldDBsForEviction();
    }
    initRollingLevelDB(currentStartTime, currentRollingDBPath);
  }

  private synchronized void scheduleOldDBsForEviction() {
    // keep at least time to live amount of data
    long evictionThreshold = computeCurrentCheckMillis(currentTimeMillis()
        - getTimeToLive());

    LOG.info("Scheduling " + getName() + " DBs older than "
        + fdf.format(evictionThreshold) + " for eviction");
    Iterator<Entry<Long, DB>> iterator = rollingdbs.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<Long, DB> entry = iterator.next();
      // parse this in gmt time
      if (entry.getKey() < evictionThreshold) {
        LOG.info("Scheduling " + getName() + " eviction for "
            + fdf.format(entry.getKey()));
        iterator.remove();
        rollingdbsToEvict.put(entry.getKey(), entry.getValue());
      }
    }
  }

  public synchronized void evictOldDBs() {
    LOG.info("Evicting " + getName() + " DBs scheduled for eviction");
    Iterator<Entry<Long, DB>> iterator = rollingdbsToEvict.entrySet()
        .iterator();
    while (iterator.hasNext()) {
      Entry<Long, DB> entry = iterator.next();
      IOUtils.cleanupWithLogger(LOG, entry.getValue());
      String dbName = fdf.format(entry.getKey());
      Path path = new Path(rollingDBPath, getName() + "." + dbName);
      try {
        LOG.info("Removing old db directory contents in " + path);
        lfs.delete(path, true);
      } catch (IOException ioe) {
        LOG.warn("Failed to evict old db " + path, ioe);
      }
      iterator.remove();
    }
  }

  public void stop() throws Exception {
    for (DB db : rollingdbs.values()) {
      IOUtils.cleanupWithLogger(LOG, db);
    }
    IOUtils.cleanupWithLogger(LOG, lfs);
  }

  private long computeNextCheckMillis(long now) {
    return computeCheckMillis(now, true);
  }

  public long computeCurrentCheckMillis(long now) {
    return computeCheckMillis(now, false);
  }

  private synchronized long computeCheckMillis(long now, boolean next) {
    // needs to be called synchronously due to shared Calendar
    cal.setTimeInMillis(now);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);

    if (rollingPeriod == RollingPeriod.DAILY) {
      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.set(Calendar.MINUTE, 0);
      if (next) {
        cal.add(Calendar.DATE, 1);
      }
    } else if (rollingPeriod == RollingPeriod.HALF_DAILY) {
      // round down to 12 hour interval
      int hour = (cal.get(Calendar.HOUR) / 12) * 12;
      cal.set(Calendar.HOUR, hour);
      cal.set(Calendar.MINUTE, 0);
      if (next) {
        cal.add(Calendar.HOUR_OF_DAY, 12);
      }
    } else if (rollingPeriod == RollingPeriod.QUARTER_DAILY) {
      // round down to 6 hour interval
      int hour = (cal.get(Calendar.HOUR) / 6) * 6;
      cal.set(Calendar.HOUR, hour);
      cal.set(Calendar.MINUTE, 0);
      if (next) {
        cal.add(Calendar.HOUR_OF_DAY, 6);
      }
    } else if (rollingPeriod == RollingPeriod.HOURLY) {
      cal.set(Calendar.MINUTE, 0);
      if (next) {
        cal.add(Calendar.HOUR_OF_DAY, 1);
      }
    } else if (rollingPeriod == RollingPeriod.MINUTELY) {
      // round down to 5 minute interval
      int minute = (cal.get(Calendar.MINUTE) / 5) * 5;
      cal.set(Calendar.MINUTE, minute);
      if (next) {
        cal.add(Calendar.MINUTE, 5);
      }
    }
    return cal.getTimeInMillis();
  }
}
