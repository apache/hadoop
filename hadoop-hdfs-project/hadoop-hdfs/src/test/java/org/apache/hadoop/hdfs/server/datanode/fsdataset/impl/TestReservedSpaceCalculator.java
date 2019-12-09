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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DU_RESERVED_CALCULATOR_KEY;
import static org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.ReservedSpaceCalculator.ReservedSpaceCalculatorAbsolute;
import static org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.ReservedSpaceCalculator.ReservedSpaceCalculatorAggressive;
import static org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.ReservedSpaceCalculator.ReservedSpaceCalculatorConservative;
import static org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.ReservedSpaceCalculator.ReservedSpaceCalculatorPercentage;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Unit testing for different types of ReservedSpace calculators.
 */
public class TestReservedSpaceCalculator {

  private Configuration conf;
  private DF usage;
  private ReservedSpaceCalculator reserved;

  @Before
  public void setUp() {
    conf = new Configuration();
    usage = Mockito.mock(DF.class);
  }

  @Test
  public void testReservedSpaceAbsolute() {
    conf.setClass(DFS_DATANODE_DU_RESERVED_CALCULATOR_KEY,
        ReservedSpaceCalculatorAbsolute.class,
        ReservedSpaceCalculator.class);

    // Test both using global configuration
    conf.setLong(DFS_DATANODE_DU_RESERVED_KEY, 900);

    checkReserved(StorageType.DISK, 10000, 900);
    checkReserved(StorageType.SSD, 10000, 900);
    checkReserved(StorageType.ARCHIVE, 10000, 900);
  }

  @Test
  public void testReservedSpaceAbsolutePerStorageType() {
    conf.setClass(DFS_DATANODE_DU_RESERVED_CALCULATOR_KEY,
        ReservedSpaceCalculatorAbsolute.class,
        ReservedSpaceCalculator.class);

    // Test DISK
    conf.setLong(DFS_DATANODE_DU_RESERVED_KEY + ".disk", 500);
    checkReserved(StorageType.DISK, 2300, 500);

    // Test SSD
    conf.setLong(DFS_DATANODE_DU_RESERVED_KEY + ".ssd", 750);
    checkReserved(StorageType.SSD, 1550, 750);
  }

  @Test
  public void testReservedSpacePercentage() {
    conf.setClass(DFS_DATANODE_DU_RESERVED_CALCULATOR_KEY,
        ReservedSpaceCalculatorPercentage.class,
        ReservedSpaceCalculator.class);

    // Test both using global configuration
    conf.setLong(DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY, 10);
    checkReserved(StorageType.DISK, 10000, 1000);
    checkReserved(StorageType.SSD, 10000, 1000);
    checkReserved(StorageType.ARCHIVE, 10000, 1000);

    conf.setLong(DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY, 50);
    checkReserved(StorageType.DISK, 4000, 2000);
    checkReserved(StorageType.SSD, 4000, 2000);
    checkReserved(StorageType.ARCHIVE, 4000, 2000);
  }

  @Test
  public void testReservedSpacePercentagePerStorageType() {
    conf.setClass(DFS_DATANODE_DU_RESERVED_CALCULATOR_KEY,
        ReservedSpaceCalculatorPercentage.class,
        ReservedSpaceCalculator.class);

    // Test DISK
    conf.setLong(DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY + ".disk", 20);
    checkReserved(StorageType.DISK, 1600, 320);

    // Test SSD
    conf.setLong(DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY + ".ssd", 50);
    checkReserved(StorageType.SSD, 8001, 4000);
  }

  @Test
  public void testReservedSpaceConservativePerStorageType() {
    // This policy should take the maximum of the two
    conf.setClass(DFS_DATANODE_DU_RESERVED_CALCULATOR_KEY,
        ReservedSpaceCalculatorConservative.class,
        ReservedSpaceCalculator.class);

    // Test DISK + taking the reserved bytes over percentage,
    // as that gives more reserved space
    conf.setLong(DFS_DATANODE_DU_RESERVED_KEY + ".disk", 800);
    conf.setLong(DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY + ".disk", 20);
    checkReserved(StorageType.DISK, 1600, 800);

    // Test ARCHIVE + taking reserved space based on the percentage,
    // as that gives more reserved space
    conf.setLong(DFS_DATANODE_DU_RESERVED_KEY + ".archive", 1300);
    conf.setLong(DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY + ".archive", 50);
    checkReserved(StorageType.ARCHIVE, 6200, 3100);
  }

  @Test
  public void testReservedSpaceAggresivePerStorageType() {
    // This policy should take the maximum of the two
    conf.setClass(DFS_DATANODE_DU_RESERVED_CALCULATOR_KEY,
        ReservedSpaceCalculatorAggressive.class,
        ReservedSpaceCalculator.class);

    // Test RAM_DISK + taking the reserved bytes over percentage,
    // as that gives less reserved space
    conf.setLong(DFS_DATANODE_DU_RESERVED_KEY + ".ram_disk", 100);
    conf.setLong(DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY + ".ram_disk", 10);
    checkReserved(StorageType.RAM_DISK, 1600, 100);

    // Test ARCHIVE + taking reserved space based on the percentage,
    // as that gives less reserved space
    conf.setLong(DFS_DATANODE_DU_RESERVED_KEY + ".archive", 20000);
    conf.setLong(DFS_DATANODE_DU_RESERVED_PERCENTAGE_KEY + ".archive", 5);
    checkReserved(StorageType.ARCHIVE, 100000, 5000);
  }

  @Test(expected = IllegalStateException.class)
  public void testInvalidCalculator() {
    conf.set(DFS_DATANODE_DU_RESERVED_CALCULATOR_KEY, "INVALIDTYPE");
    reserved = new ReservedSpaceCalculator.Builder(conf)
        .setUsage(usage)
        .setStorageType(StorageType.DISK)
        .build();
  }

  private void checkReserved(StorageType storageType,
      long totalCapacity, long reservedExpected) {
    when(usage.getCapacity()).thenReturn(totalCapacity);

    reserved = new ReservedSpaceCalculator.Builder(conf).setUsage(usage)
        .setStorageType(storageType).build();
    assertEquals(reservedExpected, reserved.getReserved());
  }
}