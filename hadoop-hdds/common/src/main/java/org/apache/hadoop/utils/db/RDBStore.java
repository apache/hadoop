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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.utils.RocksDBStoreMBean;
import org.apache.ratis.shaded.com.google.common.annotations.VisibleForTesting;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;

import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * RocksDB Store that supports creating Tables in DB.
 */
public class RDBStore implements DBStore {
  private static final Logger LOG =
      LoggerFactory.getLogger(RDBStore.class);
  private final RocksDB db;
  private final File dbLocation;
  private final WriteOptions writeOptions;
  private final DBOptions dbOptions;
  private final Hashtable<String, ColumnFamilyHandle> handleTable;
  private ObjectName statMBeanName;

  public RDBStore(File dbFile, DBOptions options, Set<TableConfig> families)
      throws IOException {
    Preconditions.checkNotNull(dbFile, "DB file location cannot be null");
    Preconditions.checkNotNull(families);
    Preconditions.checkArgument(families.size() > 0);
    handleTable = new Hashtable<>();

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
      db = RocksDB.open(dbOptions, dbLocation.getAbsolutePath(),
          columnFamilyDescriptors, columnFamilyHandles);

      for (int x = 0; x < columnFamilyHandles.size(); x++) {
        handleTable.put(
            DFSUtil.bytes2String(columnFamilyHandles.get(x).getName()),
            columnFamilyHandles.get(x));
      }

      if (dbOptions.statistics() != null) {
        Map<String, String> jmxProperties = new HashMap<>();
        jmxProperties.put("dbName", dbFile.getName());
        statMBeanName = MBeans.register("Ozone", "RocksDbStore", jmxProperties,
            new RocksDBStoreMBean(dbOptions.statistics()));
        if (statMBeanName == null) {
          LOG.warn("jmx registration failed during RocksDB init, db path :{}",
              dbFile.getAbsolutePath());
        }
      }

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
  public void move(byte[] key, Table source, Table dest) throws IOException {
    RDBTable sourceTable;
    RDBTable destTable;
    if (source instanceof RDBTable) {
      sourceTable = (RDBTable) source;
    } else {
      LOG.error("Unexpected Table type. Expected RocksTable Store for Source.");
      throw new IOException("Unexpected TableStore Type in source. Expected "
          + "RocksDBTable.");
    }

    if (dest instanceof RDBTable) {
      destTable = (RDBTable) dest;
    } else {
      LOG.error("Unexpected Table type. Expected RocksTable Store for Dest.");
      throw new IOException("Unexpected TableStore Type in dest. Expected "
          + "RocksDBTable.");
    }
    try (WriteBatch batch = new WriteBatch()) {
      byte[] value = sourceTable.get(key);
      batch.put(destTable.getHandle(), key, value);
      batch.delete(sourceTable.getHandle(), key);
      db.write(writeOptions, batch);
    } catch (RocksDBException rockdbException) {
      LOG.error("Move of key failed. Key:{}", DFSUtil.bytes2String(key));
      throw toIOException("Unable to move key: " + DFSUtil.bytes2String(key),
          rockdbException);
    }
  }


  @Override
  public void move(byte[] key, byte[] value, Table source,
      Table dest) throws IOException {
    move(key, key, value, source, dest);
  }

  @Override
  public void move(byte[] sourceKey, byte[] destKey, byte[] value, Table source,
      Table dest) throws IOException {
    RDBTable sourceTable;
    RDBTable destTable;
    if (source instanceof RDBTable) {
      sourceTable = (RDBTable) source;
    } else {
      LOG.error("Unexpected Table type. Expected RocksTable Store for Source.");
      throw new IOException("Unexpected TableStore Type in source. Expected "
          + "RocksDBTable.");
    }

    if (dest instanceof RDBTable) {
      destTable = (RDBTable) dest;
    } else {
      LOG.error("Unexpected Table type. Expected RocksTable Store for Dest.");
      throw new IOException("Unexpected TableStore Type in dest. Expected "
          + "RocksDBTable.");
    }
    try (WriteBatch batch = new WriteBatch()) {
      batch.put(destTable.getHandle(), destKey, value);
      batch.delete(sourceTable.getHandle(), sourceKey);
      db.write(writeOptions, batch);
    } catch (RocksDBException rockdbException) {
      LOG.error("Move of key failed. Key:{}", DFSUtil.bytes2String(sourceKey));
      throw toIOException("Unable to move key: " +
              DFSUtil.bytes2String(sourceKey), rockdbException);
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
  public void write(WriteBatch batch) throws IOException {
    try {
      db.write(writeOptions, batch);
    } catch (RocksDBException e) {
      throw toIOException("Unable to write the batch.", e);
    }
  }

  @VisibleForTesting
  protected ObjectName getStatMBeanName() {
    return statMBeanName;
  }

  @Override
  public Table getTable(String name) throws IOException {
    ColumnFamilyHandle handle = handleTable.get(name);
    if (handle == null) {
      throw new IOException("No such table in this DB. TableName : " + name);
    }
    return new RDBTable(this.db, handle, this.writeOptions);
  }

  @Override
  public ArrayList<Table> listTables() throws IOException {
    ArrayList<Table> returnList = new ArrayList<>();
    for (ColumnFamilyHandle handle : handleTable.values()) {
      returnList.add(new RDBTable(db, handle, writeOptions));
    }
    return returnList;
  }
}