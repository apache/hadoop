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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.util.StringUtils;

import java.lang.reflect.Constructor;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DU_RESERVED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DU_RESERVED_PERCENTAGE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DU_RESERVED_CALCULATOR_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DU_RESERVED_CALCULATOR_KEY;

/**
 * Used for calculating file system space reserved for non-HDFS data.
 */
public abstract class ReservedSpaceCalculator {

  /**
   * Used for creating instances of ReservedSpaceCalculator.
   */
  public static class Builder {

    private final Configuration conf;

    private DF usage;
    private StorageType storageType;

    public Builder(Configuration conf) {
      this.conf = conf;
    }

    public Builder setUsage(DF newUsage) {
      this.usage = newUsage;
      return this;
    }

    public Builder setStorageType(
        StorageType newStorageType) {
      this.storageType = newStorageType;
      return this;
    }

    ReservedSpaceCalculator build() {
      try {
        Class<? extends ReservedSpaceCalculator> clazz = conf.getClass(
            DFS_DATANODE_DU_RESERVED_CALCULATOR_KEY,
            DFS_DATANODE_DU_RESERVED_CALCULATOR_DEFAULT,
            ReservedSpaceCalculator.class);

        Constructor constructor = clazz.getConstructor(
            Configuration.class, DF.class, StorageType.class);

        return (ReservedSpaceCalculator) constructor.newInstance(
            conf, usage, storageType);
      } catch (Exception e) {
        throw new IllegalStateException(
            "Error instantiating ReservedSpaceCalculator", e);
      }
    }
  }

  private final DF usage;
  private final Configuration conf;
  private final StorageType storageType;

  ReservedSpaceCalculator(Configuration conf, DF usage,
      StorageType storageType) {
    this.usage = usage;
    this.conf = conf;
    this.storageType = storageType;
  }

  DF getUsage() {
    return usage;
  }

  long getReservedFromConf(String key, long defaultValue) {
    return conf.getLong(key + "." + StringUtils.toLowerCase(
        storageType.toString()), conf.getLong(key, defaultValue));
  }

  /**
   * Return the capacity of the file system space reserved for non-HDFS.
   *
   * @return the number of bytes reserved for non-HDFS.
   */
  abstract long getReserved();


  /**
   * Based on absolute number of reserved bytes.
   */
  public static class ReservedSpaceCalculatorAbsolute extends
      ReservedSpaceCalculator {

    private final long reservedBytes;

    public ReservedSpaceCalculatorAbsolute(Configuration conf, DF usage,
        StorageType storageType) {
      super(conf, usage, storageType);
      this.reservedBytes = getReservedFromConf(DFS_DATANODE_DU_RESERVED_KEY,
          DFS_DATANODE_DU_RESERVED_DEFAULT);
    }

    @Override
    long getReserved() {
      return reservedBytes;
    }
  }

  /**
   * Based on percentage of total capacity in the storage.
   */
  public static class ReservedSpaceCalculatorPercentage extends
      ReservedSpaceCalculator {

    private final long reservedPct;

    public ReservedSpaceCalculatorPercentage(Configuration conf, DF usage,
        StorageType storageType) {
      super(conf, usage, storageType);
      this.reservedPct = getReservedFromConf(
          DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY,
          DFS_DATANODE_DU_RESERVED_PERCENTAGE_DEFAULT);
    }

    @Override
    long getReserved() {
      return getPercentage(getUsage().getCapacity(), reservedPct);
    }
  }

  /**
   * Calculates absolute and percentage based reserved space and
   * picks the one that will yield more reserved space.
   */
  public static class ReservedSpaceCalculatorConservative extends
      ReservedSpaceCalculator {

    private final long reservedBytes;
    private final long reservedPct;

    public ReservedSpaceCalculatorConservative(Configuration conf, DF usage,
        StorageType storageType) {
      super(conf, usage, storageType);
      this.reservedBytes = getReservedFromConf(DFS_DATANODE_DU_RESERVED_KEY,
          DFS_DATANODE_DU_RESERVED_DEFAULT);
      this.reservedPct = getReservedFromConf(
          DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY,
          DFS_DATANODE_DU_RESERVED_PERCENTAGE_DEFAULT);
    }

    long getReservedBytes() {
      return reservedBytes;
    }

    long getReservedPct() {
      return reservedPct;
    }

    @Override
    long getReserved() {
      return Math.max(getReservedBytes(),
          getPercentage(getUsage().getCapacity(), getReservedPct()));
    }
  }

  /**
   * Calculates absolute and percentage based reserved space and
   * picks the one that will yield less reserved space.
   */
  public static class ReservedSpaceCalculatorAggressive extends
      ReservedSpaceCalculator {

    private final long reservedBytes;
    private final long reservedPct;

    public ReservedSpaceCalculatorAggressive(Configuration conf, DF usage,
        StorageType storageType) {
      super(conf, usage, storageType);
      this.reservedBytes = getReservedFromConf(DFS_DATANODE_DU_RESERVED_KEY,
          DFS_DATANODE_DU_RESERVED_DEFAULT);
      this.reservedPct = getReservedFromConf(
          DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY,
          DFS_DATANODE_DU_RESERVED_PERCENTAGE_DEFAULT);
    }

    long getReservedBytes() {
      return reservedBytes;
    }

    long getReservedPct() {
      return reservedPct;
    }

    @Override
    long getReserved() {
      return Math.min(getReservedBytes(),
          getPercentage(getUsage().getCapacity(), getReservedPct()));
    }
  }

  private static long getPercentage(long total, long percentage) {
    return (total * percentage) / 100;
  }
}