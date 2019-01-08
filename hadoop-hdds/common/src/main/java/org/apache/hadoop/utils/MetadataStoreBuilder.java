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

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;

import com.google.common.annotations.VisibleForTesting;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_LEVELDB;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_ROCKSDB;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS_OFF;
import org.iq80.leveldb.Options;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder for metadata store.
 */
public class MetadataStoreBuilder {

  @VisibleForTesting
  static final Logger LOG =
      LoggerFactory.getLogger(MetadataStoreBuilder.class);
  private File dbFile;
  private long cacheSize;
  private boolean createIfMissing = true;
  private Optional<Configuration> optionalConf = Optional.empty();
  private String dbType;

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
    this.optionalConf = Optional.of(configuration);
    return this;
  }

  /**
   * Set the container DB Type.
   * @param type
   * @return MetadataStoreBuilder
   */
  public MetadataStoreBuilder setDBType(String type) {
    this.dbType = type;
    return this;
  }


  public MetadataStore build() throws IOException {
    if (dbFile == null) {
      throw new IllegalArgumentException("Failed to build metadata store, "
          + "dbFile is required but not found");
    }

    // Build db store based on configuration
    final Configuration conf = optionalConf.orElseGet(
        () -> new OzoneConfiguration());

    if(dbType == null) {
      LOG.debug("dbType is null, using ");
      dbType = conf.getTrimmed(OzoneConfigKeys.OZONE_METADATA_STORE_IMPL,
              OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_DEFAULT);
      LOG.debug("dbType is null, using dbType {} from ozone configuration",
          dbType);
    } else {
      LOG.debug("Using dbType {} for metastore", dbType);
    }
    if (OZONE_METADATA_STORE_IMPL_LEVELDB.equals(dbType)) {
      Options options = new Options();
      options.createIfMissing(createIfMissing);
      if (cacheSize > 0) {
        options.cacheSize(cacheSize);
      }
      return new LevelDBStore(dbFile, options);
    } else if (OZONE_METADATA_STORE_IMPL_ROCKSDB.equals(dbType)) {
      org.rocksdb.Options opts = new org.rocksdb.Options();
      opts.setCreateIfMissing(createIfMissing);

      if (cacheSize > 0) {
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockCacheSize(cacheSize);
        opts.setTableFormatConfig(tableConfig);
      }

      String rocksDbStat = conf.getTrimmed(
          OZONE_METADATA_STORE_ROCKSDB_STATISTICS,
          OZONE_METADATA_STORE_ROCKSDB_STATISTICS_DEFAULT);

      if (!rocksDbStat.equals(OZONE_METADATA_STORE_ROCKSDB_STATISTICS_OFF)) {
        Statistics statistics = new Statistics();
        statistics.setStatsLevel(StatsLevel.valueOf(rocksDbStat));
        opts = opts.setStatistics(statistics);

      }
      return new RocksDBStore(dbFile, opts);
    }
    
    throw new IllegalArgumentException("Invalid argument for "
        + OzoneConfigKeys.OZONE_METADATA_STORE_IMPL
        + ". Expecting " + OZONE_METADATA_STORE_IMPL_LEVELDB
        + " or " + OZONE_METADATA_STORE_IMPL_ROCKSDB
        + ", but met " + dbType);
  }
}
