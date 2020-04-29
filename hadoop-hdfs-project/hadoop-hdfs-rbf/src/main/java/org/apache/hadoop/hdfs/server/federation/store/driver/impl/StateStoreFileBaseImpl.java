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
package org.apache.hadoop.hdfs.server.federation.store.driver.impl;

import static org.apache.hadoop.hdfs.server.federation.store.StateStoreUtils.filterMultiple;
import static org.apache.hadoop.util.Time.monotonicNow;
import static org.apache.hadoop.util.Time.now;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hdfs.server.federation.metrics.StateStoreMetrics;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUnavailableException;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.Query;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * {@link StateStoreDriver} implementation based on files. In this approach, we
 * use temporary files for the writes and renaming "atomically" to the final
 * value. Instead of writing to the final location, it will go to a temporary
 * one and then rename to the final destination.
 */
public abstract class StateStoreFileBaseImpl
    extends StateStoreSerializableImpl {

  private static final Logger LOG =
      LoggerFactory.getLogger(StateStoreFileBaseImpl.class);

  /** File extension for temporary files. */
  private static final String TMP_MARK = ".tmp";
  /** We remove temporary files older than 10 seconds. */
  private static final long OLD_TMP_RECORD_MS = TimeUnit.SECONDS.toMillis(10);
  /** File pattern for temporary records: file.XYZ.tmp. */
  private static final Pattern OLD_TMP_RECORD_PATTERN =
      Pattern.compile(".+\\.(\\d+)\\.tmp");

  /** If it is initialized. */
  private boolean initialized = false;


  /**
   * Get the reader of a record for the file system.
   *
   * @param path Path of the record to read.
   * @return Reader for the record.
   */
  protected abstract <T extends BaseRecord> BufferedReader getReader(
      String path);

  /**
   * Get the writer of a record for the file system.
   *
   * @param path Path of the record to write.
   * @return Writer for the record.
   */
  protected abstract <T extends BaseRecord> BufferedWriter getWriter(
      String path);

  /**
   * Check if a path exists.
   *
   * @param path Path to check.
   * @return If the path exists.
   */
  protected abstract boolean exists(String path);

  /**
   * Make a directory.
   *
   * @param path Path of the directory to create.
   * @return If the directory was created.
   */
  protected abstract boolean mkdir(String path);

  /**
   * Rename a file. This should be atomic.
   *
   * @param src Source name.
   * @param dst Destination name.
   * @return If the rename was successful.
   */
  protected abstract boolean rename(String src, String dst);

  /**
   * Remove a file.
   *
   * @param path Path for the file to remove
   * @return If the file was removed.
   */
  protected abstract boolean remove(String path);

  /**
   * Get the children for a path.
   *
   * @param path Path to check.
   * @return List of children.
   */
  protected abstract List<String> getChildren(String path);

  /**
   * Get root directory.
   *
   * @return Root directory.
   */
  protected abstract String getRootDir();

  /**
   * Set the driver as initialized.
   *
   * @param ini If the driver is initialized.
   */
  public void setInitialized(boolean ini) {
    this.initialized = ini;
  }

  @Override
  public boolean initDriver() {
    String rootDir = getRootDir();
    try {
      if (rootDir == null) {
        LOG.error("Invalid root directory, unable to initialize driver.");
        return false;
      }

      // Check root path
      if (!exists(rootDir)) {
        if (!mkdir(rootDir)) {
          LOG.error("Cannot create State Store root directory {}", rootDir);
          return false;
        }
      }
    } catch (Exception ex) {
      LOG.error(
          "Cannot initialize filesystem using root directory {}", rootDir, ex);
      return false;
    }
    setInitialized(true);
    return true;
  }

  @Override
  public <T extends BaseRecord> boolean initRecordStorage(
      String className, Class<T> recordClass) {

    String dataDirPath = getRootDir() + "/" + className;
    try {
      // Create data directories for files
      if (!exists(dataDirPath)) {
        LOG.info("{} data directory doesn't exist, creating it", dataDirPath);
        if (!mkdir(dataDirPath)) {
          LOG.error("Cannot create data directory {}", dataDirPath);
          return false;
        }
      }
    } catch (Exception ex) {
      LOG.error("Cannot create data directory {}", dataDirPath, ex);
      return false;
    }
    return true;
  }

  @Override
  public <T extends BaseRecord> QueryResult<T> get(Class<T> clazz)
      throws IOException {
    verifyDriverReady();
    long start = monotonicNow();
    StateStoreMetrics metrics = getMetrics();
    List<T> ret = new ArrayList<>();
    try {
      String path = getPathForClass(clazz);
      List<String> children = getChildren(path);
      for (String child : children) {
        String pathRecord = path + "/" + child;
        if (child.endsWith(TMP_MARK)) {
          LOG.debug("There is a temporary file {} in {}", child, path);
          if (isOldTempRecord(child)) {
            LOG.warn("Removing {} as it's an old temporary record", child);
            remove(pathRecord);
          }
        } else {
          T record = getRecord(pathRecord, clazz);
          ret.add(record);
        }
      }
    } catch (Exception e) {
      if (metrics != null) {
        metrics.addFailure(monotonicNow() - start);
      }
      String msg = "Cannot fetch records for " + clazz.getSimpleName();
      LOG.error(msg, e);
      throw new IOException(msg, e);
    }

    if (metrics != null) {
      metrics.addRead(monotonicNow() - start);
    }
    return new QueryResult<T>(ret, getTime());
  }

  /**
   * Check if a record is temporary and old.
   *
   * @param pathRecord Path for the record to check.
   * @return If the record is temporary and old.
   */
  @VisibleForTesting
  public static boolean isOldTempRecord(final String pathRecord) {
    if (!pathRecord.endsWith(TMP_MARK)) {
      return false;
    }
    // Extract temporary record creation time
    Matcher m = OLD_TMP_RECORD_PATTERN.matcher(pathRecord);
    if (m.find()) {
      long time = Long.parseLong(m.group(1));
      return now() - time > OLD_TMP_RECORD_MS;
    }
    return false;
  }

  /**
   * Read a record from a file.
   *
   * @param path Path to the file containing the record.
   * @param clazz Class of the record.
   * @return Record read from the file.
   * @throws IOException If the file cannot be read.
   */
  private <T extends BaseRecord> T getRecord(
      final String path, final Class<T> clazz) throws IOException {
    BufferedReader reader = getReader(path);
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!line.startsWith("#") && line.length() > 0) {
          try {
            T record = newRecord(line, clazz, false);
            return record;
          } catch (Exception ex) {
            LOG.error("Cannot parse line {} in file {}", line, path, ex);
          }
        }
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
    throw new IOException("Cannot read " + path + " for record " +
        clazz.getSimpleName());
  }

  /**
   * Get the path for a record class.
   * @param clazz Class of the record.
   * @return Path for this record class.
   */
  private <T extends BaseRecord> String getPathForClass(final Class<T> clazz) {
    String className = StateStoreUtils.getRecordName(clazz);
    StringBuilder sb = new StringBuilder();
    sb.append(getRootDir());
    if (sb.charAt(sb.length() - 1) != '/') {
      sb.append("/");
    }
    sb.append(className);
    return sb.toString();
  }

  @Override
  public boolean isDriverReady() {
    return this.initialized;
  }

  @Override
  public <T extends BaseRecord> boolean putAll(
      List<T> records, boolean allowUpdate, boolean errorIfExists)
          throws StateStoreUnavailableException {
    verifyDriverReady();
    if (records.isEmpty()) {
      return true;
    }

    long start = monotonicNow();
    StateStoreMetrics metrics = getMetrics();

    // Check if any record exists
    Map<String, T> toWrite = new HashMap<>();
    for (T record : records) {
      Class<? extends BaseRecord> recordClass = record.getClass();
      String path = getPathForClass(recordClass);
      String primaryKey = getPrimaryKey(record);
      String recordPath = path + "/" + primaryKey;

      if (exists(recordPath)) {
        if (allowUpdate) {
          // Update the mod time stamp. Many backends will use their
          // own timestamp for the mod time.
          record.setDateModified(this.getTime());
          toWrite.put(recordPath, record);
        } else if (errorIfExists) {
          LOG.error("Attempt to insert record {} that already exists",
              recordPath);
          if (metrics != null) {
            metrics.addFailure(monotonicNow() - start);
          }
          return false;
        } else  {
          LOG.debug("Not updating {}", record);
        }
      } else {
        toWrite.put(recordPath, record);
      }
    }

    // Write the records
    boolean success = true;
    for (Entry<String, T> entry : toWrite.entrySet()) {
      String recordPath = entry.getKey();
      String recordPathTemp = recordPath + "." + now() + TMP_MARK;
      BufferedWriter writer = getWriter(recordPathTemp);
      try {
        T record = entry.getValue();
        String line = serializeString(record);
        writer.write(line);
      } catch (IOException e) {
        LOG.error("Cannot write {}", recordPathTemp, e);
        success = false;
      } finally {
        if (writer != null) {
          try {
            writer.close();
          } catch (IOException e) {
            LOG.error("Cannot close the writer for {}", recordPathTemp, e);
          }
        }
      }
      // Commit
      if (!rename(recordPathTemp, recordPath)) {
        LOG.error("Failed committing record into {}", recordPath);
        success = false;
      }
    }

    long end = monotonicNow();
    if (metrics != null) {
      if (success) {
        metrics.addWrite(end - start);
      } else {
        metrics.addFailure(end - start);
      }
    }
    return success;
  }

  @Override
  public <T extends BaseRecord> int remove(Class<T> clazz, Query<T> query)
      throws StateStoreUnavailableException {
    verifyDriverReady();

    if (query == null) {
      return 0;
    }

    long start = Time.monotonicNow();
    StateStoreMetrics metrics = getMetrics();
    int removed = 0;
    // Get the current records
    try {
      final QueryResult<T> result = get(clazz);
      final List<T> existingRecords = result.getRecords();
      // Write all of the existing records except those to be removed
      final List<T> recordsToRemove = filterMultiple(query, existingRecords);
      boolean success = true;
      for (T recordToRemove : recordsToRemove) {
        String path = getPathForClass(clazz);
        String primaryKey = getPrimaryKey(recordToRemove);
        String recordToRemovePath = path + "/" + primaryKey;
        if (remove(recordToRemovePath)) {
          removed++;
        } else {
          LOG.error("Cannot remove record {}", recordToRemovePath);
          success = false;
        }
      }
      if (!success) {
        LOG.error("Cannot remove records {} query {}", clazz, query);
        if (metrics != null) {
          metrics.addFailure(monotonicNow() - start);
        }
      }
    } catch (IOException e) {
      LOG.error("Cannot remove records {} query {}", clazz, query, e);
      if (metrics != null) {
        metrics.addFailure(monotonicNow() - start);
      }
    }

    if (removed > 0 && metrics != null) {
      metrics.addRemove(monotonicNow() - start);
    }
    return removed;
  }

  @Override
  public <T extends BaseRecord> boolean removeAll(Class<T> clazz)
      throws StateStoreUnavailableException {
    verifyDriverReady();
    long start = Time.monotonicNow();
    StateStoreMetrics metrics = getMetrics();

    boolean success = true;
    String path = getPathForClass(clazz);
    List<String> children = getChildren(path);
    for (String child : children) {
      String pathRecord = path + "/" + child;
      if (!remove(pathRecord)) {
        success = false;
      }
    }

    if (metrics != null) {
      long time = Time.monotonicNow() - start;
      if (success) {
        metrics.addRemove(time);
      } else {
        metrics.addFailure(time);
      }
    }
    return success;
  }
}
