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

import org.apache.hadoop.conf.StorageUnit;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.DBOptions;

import java.math.BigDecimal;

/**
 * User visible configs based RocksDB tuning page. Documentation for Options.
 * <p>
 * https://github.com/facebook/rocksdb/blob/master/include/rocksdb/options.h
 * <p>
 * Most tuning parameters are based on this URL.
 * <p>
 * https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning
 */
public enum DBProfile {
  //TODO : Add more profiles like TEST etc.
  SSD {
    @Override
    public String toString() {
      return "DBProfile.SSD";
    }

    @Override
    public ColumnFamilyOptions getColumnFamilyOptions() {

      // Set BlockCacheSize to 256 MB. This should not be an issue for HADOOP.
      final long blockCacheSize = toLong(StorageUnit.MB.toBytes(256.00));

      // Set the Default block size to 16KB
      final long blockSize = toLong(StorageUnit.KB.toBytes(16));

      // Write Buffer Size -- set to 128 MB
      final long writeBufferSize = toLong(StorageUnit.MB.toBytes(128));

      return new ColumnFamilyOptions()
          .setLevelCompactionDynamicLevelBytes(true)
          .setWriteBufferSize(writeBufferSize)
          .setTableFormatConfig(
              new BlockBasedTableConfig()
                  .setBlockCacheSize(blockCacheSize)
                  .setBlockSize(blockSize)
                  .setCacheIndexAndFilterBlocks(true)
                  .setPinL0FilterAndIndexBlocksInCache(true)
                  .setFilter(new BloomFilter()));
    }

    @Override
    public DBOptions getDBOptions() {
      final int maxBackgroundCompactions = 4;
      final int maxBackgroundFlushes = 2;
      final long bytesPerSync = toLong(StorageUnit.MB.toBytes(1.00));
      final boolean createIfMissing = true;
      final boolean createMissingColumnFamilies = true;
      return new DBOptions()
          .setIncreaseParallelism(Runtime.getRuntime().availableProcessors())
          .setMaxBackgroundCompactions(maxBackgroundCompactions)
          .setMaxBackgroundFlushes(maxBackgroundFlushes)
          .setBytesPerSync(bytesPerSync)
          .setCreateIfMissing(createIfMissing)
          .setCreateMissingColumnFamilies(createMissingColumnFamilies);
    }


  },
  DISK {
    @Override
    public String toString() {
      return "DBProfile.DISK";
    }

    @Override
    public DBOptions getDBOptions() {
      final long readAheadSize = toLong(StorageUnit.MB.toBytes(4.00));
      return SSD.getDBOptions().setCompactionReadaheadSize(readAheadSize);
    }

    @Override
    public ColumnFamilyOptions getColumnFamilyOptions() {
      ColumnFamilyOptions columnFamilyOptions = SSD.getColumnFamilyOptions();
      columnFamilyOptions.setCompactionStyle(CompactionStyle.LEVEL);
      return columnFamilyOptions;
    }


  };

  private static long toLong(double value) {
    BigDecimal temp = new BigDecimal(value);
    return temp.longValue();
  }

  public abstract DBOptions getDBOptions();

  public abstract ColumnFamilyOptions getColumnFamilyOptions();
}
