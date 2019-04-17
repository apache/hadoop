/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.utils.db;

import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_CHECKPOINTS_DIR_NAME;

import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.utils.RocksDBStoreMBean;

import com.google.common.base.Preconditions;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocksDB Store that supports creating Tables in DB.
 */
public class RDBStore implements DBStore {
  private static final Logger LOG =
      LoggerFactory.getLogger(RDBStore.class);
  private RocksDB db;
  private File dbLocation;
  private final WriteOptions writeOptions;
  private final DBOptions dbOptions;
  private final CodecRegistry codecRegistry;
  private final Hashtable<String, ColumnFamilyHandle> handleTable;
  private ObjectName statMBeanName;
  private RDBCheckpointManager checkPointManager;
  private String checkpointsParentDir;

  @VisibleForTesting
  public RDBStore(File dbFile, DBOptions options,
                  Set<TableConfig> families) throws IOException {
    this(dbFile, options, families, new CodecRegistry(), false);
  }

  public RDBStore(File dbFile, DBOptions options, Set<TableConfig> families,
                  CodecRegistry registry, boolean readOnly)
      throws IOException {
    Preconditions.checkNotNull(dbFile, "DB file location cannot be null");
    Preconditions.checkNotNull(families);
    Preconditions.checkArgument(families.size() > 0);
    handleTable = new Hashtable<>();
    codecRegistry = registry;
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
        new ArrayList<>();
    final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

    for (TableConfig family : families) {
      columnFamilyDescriptors.add(family.getDescriptor());
    }

    dbOptions = options;
    dbLocation = dbFile;
    // TODO: Read from the next Config.
    writeOptions = new WriteOptions();

    try {
      if (readOnly) {
        db = RocksDB.openReadOnly(dbOptions, dbLocation.getAbsolutePath(),
            columnFamilyDescriptors, columnFamilyHandles);
      } else {
        db = RocksDB.open(dbOptions, dbLocation.getAbsolutePath(),
            columnFamilyDescriptors, columnFamilyHandles);
      }

      for (int x = 0; x < columnFamilyHandles.size(); x++) {
        handleTable.put(
            DFSUtil.bytes2String(columnFamilyHandles.get(x).getName()),
            columnFamilyHandles.get(x));
      }

      if (dbOptions.statistics() != null) {
        Map<String, String> jmxProperties = new HashMap<>();
        jmxProperties.put("dbName", dbFile.getName());
        statMBeanName = HddsUtils.registerWithJmxProperties(
            "Ozone", "RocksDbStore", jmxProperties,
            new RocksDBStoreMBean(dbOptions.statistics()));
        if (statMBeanName == null) {
          LOG.warn("jmx registration failed during RocksDB init, db path :{}",
              dbFile.getAbsolutePath());
        }
      }

      //create checkpoints directory if not exists.
      checkpointsParentDir = Paths.get(dbLocation.getParent(),
          OM_DB_CHECKPOINTS_DIR_NAME).toString();
      File checkpointsDir = new File(checkpointsParentDir);
      if (!checkpointsDir.exists()) {
        boolean success = checkpointsDir.mkdir();
        if (!success) {
          LOG.warn("Unable to create RocksDB checkpoint directory");
        }
      }

      //Initialize checkpoint manager
      checkPointManager = new RDBCheckpointManager(db, "om");

    } catch (RocksDBException e) {
      throw toIOException(
          "Failed init RocksDB, db path : " + dbFile.getAbsolutePath(), e);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("RocksDB successfully opened.");
      LOG.debug("[Option] dbLocation= {}", dbLocation.getAbsolutePath());
      LOG.debug("[Option] createIfMissing = {}", options.createIfMissing());
      LOG.debug("[Option] maxOpenFiles= {}", options.maxOpenFiles());
    }
  }

  public static IOException toIOException(String msg, RocksDBException e) {
    String statusCode = e.getStatus() == null ? "N/A" :
        e.getStatus().getCodeString();
    String errMessage = e.getMessage() == null ? "Unknown error" :
        e.getMessage();
    String output = msg + "; status : " + statusCode
        + "; message : " + errMessage;
    return new IOException(output, e);
  }

  @Override
  public void compactDB() throws IOException {
    if (db != null) {
      try {
        db.compactRange();
      } catch (RocksDBException e) {
        throw toIOException("Failed to compact db", e);
      }
    }
  }

  @Override
  public void close() throws IOException {

    for (final ColumnFamilyHandle handle : handleTable.values()) {
      handle.close();
    }

    if (statMBeanName != null) {
      MBeans.unregister(statMBeanName);
      statMBeanName = null;
    }

    if (db != null) {
      db.close();
    }

    if (dbOptions != null) {
      dbOptions.close();
    }

    if (writeOptions != null) {
      writeOptions.close();
    }
  }

  @Override
  public <KEY, VALUE> void move(KEY key, Table<KEY, VALUE> source,
                                Table<KEY, VALUE> dest) throws IOException {
    try (BatchOperation batchOperation = initBatchOperation()) {

      VALUE value = source.get(key);
      dest.putWithBatch(batchOperation, key, value);
      source.deleteWithBatch(batchOperation, key);
      commitBatchOperation(batchOperation);
    }
  }

  @Override
  public <KEY, VALUE> void move(KEY key, VALUE value, Table<KEY, VALUE> source,
                                Table<KEY, VALUE> dest) throws IOException {
    move(key, key, value, source, dest);
  }

  @Override
  public <KEY, VALUE> void move(KEY sourceKey, KEY destKey, VALUE value,
                                Table<KEY, VALUE> source,
                                Table<KEY, VALUE> dest) throws IOException {
    try (BatchOperation batchOperation = initBatchOperation()) {
      dest.putWithBatch(batchOperation, destKey, value);
      source.deleteWithBatch(batchOperation, sourceKey);
      commitBatchOperation(batchOperation);
    }
  }

  @Override
  public long getEstimatedKeyCount() throws IOException {
    try {
      return db.getLongProperty("rocksdb.estimate-num-keys");
    } catch (RocksDBException e) {
      throw toIOException("Unable to get the estimated count.", e);
    }
  }

  @Override
  public BatchOperation initBatchOperation() {
    return new RDBBatchOperation();
  }

  @Override
  public void commitBatchOperation(BatchOperation operation)
      throws IOException {
    ((RDBBatchOperation) operation).commit(db, writeOptions);
  }


  @VisibleForTesting
  protected ObjectName getStatMBeanName() {
    return statMBeanName;
  }

  @Override
  public Table<byte[], byte[]> getTable(String name) throws IOException {
    ColumnFamilyHandle handle = handleTable.get(name);
    if (handle == null) {
      throw new IOException("No such table in this DB. TableName : " + name);
    }
    return new RDBTable(this.db, handle, this.writeOptions);
  }

  @Override
  public <KEY, VALUE> Table<KEY, VALUE> getTable(String name,
      Class<KEY> keyType, Class<VALUE> valueType) throws IOException {
    return new TypedTable<KEY, VALUE>(getTable(name), codecRegistry, keyType,
        valueType);
  }

  @Override
  public ArrayList<Table> listTables() throws IOException {
    ArrayList<Table> returnList = new ArrayList<>();
    for (ColumnFamilyHandle handle : handleTable.values()) {
      returnList.add(new RDBTable(db, handle, writeOptions));
    }
    return returnList;
  }

  @Override
  public void flush() throws IOException {
    final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true);
    try {
      db.flush(flushOptions);
    } catch (RocksDBException e) {
      LOG.error("Unable to Flush RocksDB data", e);
      throw toIOException("Unable to Flush RocksDB data", e);
    }
  }

  @Override
  public DBCheckpoint getCheckpoint(boolean flush) {
    final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(flush);
    try {
      db.flush(flushOptions);
    } catch (RocksDBException e) {
      LOG.error("Unable to Flush RocksDB data before creating snapshot", e);
    }
    return checkPointManager.createCheckpoint(checkpointsParentDir);
  }

  @Override
  public File getDbLocation() {
    return dbLocation;
  }
}