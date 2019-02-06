/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package org.apache.hadoop.ozone.genesis;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.RocksDBStore;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;

/**
 * Benchmark rocksdb store.
 */
@State(Scope.Thread)
public class BenchMarkRocksDbStore {
  private static final int DATA_LEN = 1024;
  private static final long MAX_KEYS = 1024 * 10;
  private static final int DB_FILE_LEN = 7;
  private static final String TMP_DIR = "java.io.tmpdir";

  private MetadataStore store;
  private byte[] data;

  @Param(value = {"8"})
  private String blockSize; // 4KB default

  @Param(value = {"64"})
  private String writeBufferSize; //64 MB default

  @Param(value = {"16"})
  private String maxWriteBufferNumber; // 2 default

  @Param(value = {"4"})
  private String maxBackgroundFlushes; // 1 default

  @Param(value = {"512"})
  private String maxBytesForLevelBase;

  @Param(value = {"4"})
  private String backgroundThreads;

  @Param(value = {"5000"})
  private String maxOpenFiles;

  @Setup(Level.Trial)
  public void initialize() throws IOException {
    data = RandomStringUtils.randomAlphanumeric(DATA_LEN)
        .getBytes(Charset.forName("UTF-8"));
    org.rocksdb.Options opts = new org.rocksdb.Options();
    File dbFile = Paths.get(System.getProperty(TMP_DIR))
        .resolve(RandomStringUtils.randomNumeric(DB_FILE_LEN))
        .toFile();
    opts.setCreateIfMissing(true);
    opts.setWriteBufferSize(
        (long) StorageUnit.MB.toBytes(Long.parseLong(writeBufferSize)));
    opts.setMaxWriteBufferNumber(Integer.parseInt(maxWriteBufferNumber));
    opts.setMaxBackgroundFlushes(Integer.parseInt(maxBackgroundFlushes));
    BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
    tableConfig.setBlockSize(
        (long) StorageUnit.KB.toBytes(Long.parseLong(blockSize)));
    opts.setMaxOpenFiles(Integer.parseInt(maxOpenFiles));
    opts.setMaxBytesForLevelBase(
        (long) StorageUnit.MB.toBytes(Long.parseLong(maxBytesForLevelBase)));
    opts.setCompactionStyle(CompactionStyle.UNIVERSAL);
    opts.setLevel0FileNumCompactionTrigger(10);
    opts.setLevel0SlowdownWritesTrigger(20);
    opts.setLevel0StopWritesTrigger(40);
    opts.setTargetFileSizeBase(
        (long) StorageUnit.MB.toBytes(Long.parseLong(maxBytesForLevelBase))
            / 10);
    opts.setMaxBackgroundCompactions(8);
    opts.setUseFsync(false);
    opts.setBytesPerSync(8388608);
    org.rocksdb.Filter bloomFilter = new org.rocksdb.BloomFilter(20);
    tableConfig.setCacheIndexAndFilterBlocks(true);
    tableConfig.setIndexType(IndexType.kHashSearch);
    tableConfig.setFilter(bloomFilter);
    opts.setTableFormatConfig(tableConfig);
    opts.useCappedPrefixExtractor(4);
    store = new RocksDBStore(dbFile, opts);
  }

  @TearDown(Level.Trial)
  public void cleanup() throws IOException {
    store.destroy();
    FileUtils.deleteDirectory(new File(TMP_DIR));
  }

  @Benchmark
  public void test(Blackhole bh) throws IOException {
    long x = org.apache.commons.lang3.RandomUtils.nextLong(0L, MAX_KEYS);
    store.put(Long.toHexString(x).getBytes(Charset.forName("UTF-8")), data);
    bh.consume(
        store.get(Long.toHexString(x).getBytes(Charset.forName("UTF-8"))));
  }
}
