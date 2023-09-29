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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.hdfs.server.federation.metrics.StateStoreMetrics;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUnavailableException;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreOperationResult;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.Query;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

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

  private ExecutorService concurrentStoreAccessPool;

  /**
   * Get the reader of a record for the file system.
   *
   * @param path Path of the record to read.
   * @param <T> Type of the state store record.
   * @return Reader for the record.
   */
  protected abstract <T extends BaseRecord> BufferedReader getReader(
      String path);

  /**
   * Get the writer of a record for the file system.
   *
   * @param path Path of the record to write.
   * @param <T> Type of the state store record.
   * @return Writer for the record.
   */
  @VisibleForTesting
  public abstract <T extends BaseRecord> BufferedWriter getWriter(
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

  protected abstract int getConcurrentFilesAccessNumThreads();

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
    int threads = getConcurrentFilesAccessNumThreads();
    if (threads > 1) {
      this.concurrentStoreAccessPool =
          new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS,
              new LinkedBlockingQueue<>(),
              new ThreadFactoryBuilder()
                  .setNameFormat("state-store-file-based-concurrent-%d")
                  .setDaemon(true).build());
      LOG.info("File based state store will be accessed concurrently with {} max threads", threads);
    } else {
      LOG.info("File based state store will be accessed serially");
    }
    return true;
  }

  @Override
  public void close() throws Exception {
    if (this.concurrentStoreAccessPool != null) {
      this.concurrentStoreAccessPool.shutdown();
      boolean isTerminated = this.concurrentStoreAccessPool.awaitTermination(5, TimeUnit.SECONDS);
      LOG.info("Concurrent store access pool is terminated: {}", isTerminated);
      this.concurrentStoreAccessPool = null;
    }
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
    List<T> result = Collections.synchronizedList(new ArrayList<>());
    try {
      String path = getPathForClass(clazz);
      List<String> children = getChildren(path);
      List<Callable<Void>> callables = new ArrayList<>();
      children.forEach(child -> callables.add(
          () -> getRecordsFromFileAndRemoveOldTmpRecords(clazz, result, path, child)));
      if (this.concurrentStoreAccessPool != null) {
        // Read records concurrently
        List<Future<Void>> futures = this.concurrentStoreAccessPool.invokeAll(callables);
        for (Future<Void> future : futures) {
          future.get();
        }
      } else {
        // Read records serially
        callables.forEach(e -> {
          try {
            e.call();
          } catch (Exception ex) {
            LOG.error("Failed to retrieve record using file operations.", ex);
            throw new RuntimeException(ex);
          }
        });
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
    return new QueryResult<>(result, getTime());
  }

  /**
   * Get the state store record from the given path (path/child) and add the record to the
   * result list.
   *
   * @param clazz Class of the record.
   * @param result The list of results record. The records would be added to it unless the given
   * path represents old temp file.
   * @param path The parent path.
   * @param child The child path under the parent path. Both path and child completes the file
   * location for the given record.
   * @param <T> Record class of the records.
   * @return Void.
   * @throws IOException If the file read operation fails.
   */
  private <T extends BaseRecord> Void getRecordsFromFileAndRemoveOldTmpRecords(Class<T> clazz,
      List<T> result, String path, String child) throws IOException {
    String pathRecord = path + "/" + child;
    if (child.endsWith(TMP_MARK)) {
      LOG.debug("There is a temporary file {} in {}", child, path);
      if (isOldTempRecord(child)) {
        LOG.warn("Removing {} as it's an old temporary record", child);
        remove(pathRecord);
      }
    } else {
      T record = getRecord(pathRecord, clazz);
      result.add(record);
    }
    return null;
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
    try (BufferedReader reader = getReader(path)) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!line.startsWith("#") && line.length() > 0) {
          try {
            return newRecord(line, clazz, false);
          } catch (Exception ex) {
            LOG.error("Cannot parse line {} in file {}", line, path, ex);
          }
        }
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
  public <T extends BaseRecord> StateStoreOperationResult putAll(
      List<T> records, boolean allowUpdate, boolean errorIfExists)
          throws StateStoreUnavailableException {
    verifyDriverReady();
    if (records.isEmpty()) {
      return StateStoreOperationResult.getDefaultSuccessResult();
    }

    long start = monotonicNow();
    StateStoreMetrics metrics = getMetrics();

    // Check if any record exists
    Map<String, T> toWrite = new HashMap<>();
    final List<String> failedRecordsKeys = Collections.synchronizedList(new ArrayList<>());
    final AtomicBoolean success = new AtomicBoolean(true);

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
          LOG.error("Attempt to insert record {} that already exists", recordPath);
          failedRecordsKeys.add(getOriginalPrimaryKey(primaryKey));
          success.set(false);
        } else {
          LOG.debug("Not updating {}", record);
        }
      } else {
        toWrite.put(recordPath, record);
      }
    }

    // Write the records
    final List<Callable<Void>> callables = new ArrayList<>();
    toWrite.entrySet().forEach(
        entry -> callables.add(() -> writeRecordToFile(success, entry, failedRecordsKeys)));
    if (this.concurrentStoreAccessPool != null) {
      // Write records concurrently
      List<Future<Void>> futures = null;
      try {
        futures = this.concurrentStoreAccessPool.invokeAll(callables);
      } catch (InterruptedException e) {
        success.set(false);
        LOG.error("Failed to put record concurrently.", e);
      }
      if (futures != null) {
        for (Future<Void> future : futures) {
          try {
            future.get();
          } catch (InterruptedException | ExecutionException e) {
            success.set(false);
            LOG.error("Failed to retrieve results from concurrent record put runs.", e);
          }
        }
      }
    } else {
      // Write records serially
      callables.forEach(callable -> {
        try {
          callable.call();
        } catch (Exception e) {
          success.set(false);
          LOG.error("Failed to put record.", e);
        }
      });
    }

    long end = monotonicNow();
    if (metrics != null) {
      if (success.get()) {
        metrics.addWrite(end - start);
      } else {
        metrics.addFailure(end - start);
      }
    }
    return new StateStoreOperationResult(failedRecordsKeys, success.get());
  }

  /**
   * Writes the state store record to the file. At first, the record is written to a temp location
   * and then later renamed to the final location that is passed with the entry key.
   *
   * @param <T> Record class of the records.
   * @param success The atomic boolean that gets updated to false if the file write operation fails.
   * @param entry The entry of the record path and the state store record to be written to the file
   * by first writing to a temp location and then renaming it to the record path.
   * @param failedRecordsList The list of paths of the failed records.
   * @return Void.
   */
  private <T extends BaseRecord> Void writeRecordToFile(AtomicBoolean success,
      Entry<String, T> entry, List<String> failedRecordsList) {
    final String recordPath = entry.getKey();
    final T record = entry.getValue();
    final String primaryKey = getPrimaryKey(record);
    final String recordPathTemp = recordPath + "." + now() + TMP_MARK;
    boolean recordWrittenSuccessfully = true;
    try (BufferedWriter writer = getWriter(recordPathTemp)) {
      String line = serializeString(record);
      writer.write(line);
    } catch (IOException e) {
      LOG.error("Cannot write {}", recordPathTemp, e);
      recordWrittenSuccessfully = false;
      failedRecordsList.add(getOriginalPrimaryKey(primaryKey));
      success.set(false);
    }
    // Commit
    if (recordWrittenSuccessfully && !rename(recordPathTemp, recordPath)) {
      LOG.error("Failed committing record into {}", recordPath);
      failedRecordsList.add(getOriginalPrimaryKey(primaryKey));
      success.set(false);
    }
    return null;
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
