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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_METADATA_STORE_ROCKSDB_STATISTICS;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_METADATA_STORE_ROCKSDB_STATISTICS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_METADATA_STORE_ROCKSDB_STATISTICS_OFF;
import org.iq80.leveldb.Options;
import org.rocksdb.BlockBasedTableConfig;

import java.io.File;
import java.io.IOException;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_LEVELDB;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_ROCKSDB;

import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;

/**
 * Builder for metadata store.
 */
public class MetadataStoreBuilder {

  private File dbFile;
  private long cacheSize;
  private boolean createIfMissing = true;
  private Configuration conf;

  public static MetadataStoreBuilder newBuilder() {
    return new MetadataStoreBuilder();
  }

  public MetadataStoreBuilder setDbFile(File dbPath) {
    this.dbFile = dbPath;
    return this;
  }

  public MetadataStoreBuilder setCacheSize(long cache) {
    this.cacheSize = cache;
    return this;
  }

  public MetadataStoreBuilder setCreateIfMissing(boolean doCreate) {
    this.createIfMissing = doCreate;
    return this;
  }

  public MetadataStoreBuilder setConf(Configuration configuration) {
    this.conf = configuration;
    return this;
  }

  public MetadataStore build() throws IOException {
    if (dbFile == null) {
      throw new IllegalArgumentException("Failed to build metadata store, "
          + "dbFile is required but not found");
    }

    // Build db store based on configuration
    MetadataStore store = null;
    String impl = conf == null ?
        OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_DEFAULT :
        conf.getTrimmed(OzoneConfigKeys.OZONE_METADATA_STORE_IMPL,
            OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_DEFAULT);
    if (OZONE_METADATA_STORE_IMPL_LEVELDB.equals(impl)) {
      Options options = new Options();
      options.createIfMissing(createIfMissing);
      if (cacheSize > 0) {
        options.cacheSize(cacheSize);
      }
      store = new LevelDBStore(dbFile, options);
    } else if (OZONE_METADATA_STORE_IMPL_ROCKSDB.equals(impl)) {
      org.rocksdb.Options opts = new org.rocksdb.Options();
      opts.setCreateIfMissing(createIfMissing);

      if (cacheSize > 0) {
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockCacheSize(cacheSize);
        opts.setTableFormatConfig(tableConfig);
      }

      String rocksDbStat = conf == null ?
          OZONE_METADATA_STORE_ROCKSDB_STATISTICS_DEFAULT :
          conf.getTrimmed(OZONE_METADATA_STORE_ROCKSDB_STATISTICS,
              OZONE_METADATA_STORE_ROCKSDB_STATISTICS_DEFAULT);

      if (!rocksDbStat.equals(OZONE_METADATA_STORE_ROCKSDB_STATISTICS_OFF)) {
        Statistics statistics = new Statistics();
        statistics.setStatsLevel(StatsLevel.valueOf(rocksDbStat));
        opts = opts.setStatistics(statistics);

      }
      store = new RocksDBStore(dbFile, opts);
    } else {
      throw new IllegalArgumentException("Invalid argument for "
          + OzoneConfigKeys.OZONE_METADATA_STORE_IMPL
          + ". Expecting " + OZONE_METADATA_STORE_IMPL_LEVELDB
          + " or " + OZONE_METADATA_STORE_IMPL_ROCKSDB
          + ", but met " + impl);
    }
    return store;
  }
}
