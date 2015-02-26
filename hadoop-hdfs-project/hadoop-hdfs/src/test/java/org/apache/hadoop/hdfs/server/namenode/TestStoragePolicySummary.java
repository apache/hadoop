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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.StoragePolicySummary.StorageTypeAllocation;
import org.junit.Assert;
import org.junit.Test;

public class TestStoragePolicySummary {
  
  private Map<String, Long> convertToStringMap(StoragePolicySummary sts) {
    LinkedHashMap<String, Long> actualOutput = new LinkedHashMap<>();
    for (Map.Entry<StorageTypeAllocation, Long> entry:
      StoragePolicySummary.sortByComparator(sts.storageComboCounts)) {
      actualOutput.put(entry.getKey().toString(), entry.getValue());
    }
    return actualOutput;
  }

  @Test
  public void testMultipleHots() {
    BlockStoragePolicySuite bsps = BlockStoragePolicySuite.createDefaultSuite();
    StoragePolicySummary sts = new StoragePolicySummary(bsps.getAllPolicies());
    BlockStoragePolicy hot = bsps.getPolicy("HOT");
    sts.add(new StorageType[]{StorageType.DISK},hot);
    sts.add(new StorageType[]{StorageType.DISK,StorageType.DISK},hot);
    sts.add(new StorageType[]{StorageType.DISK,
        StorageType.DISK,StorageType.DISK},hot);
    sts.add(new StorageType[]{StorageType.DISK,
        StorageType.DISK,StorageType.DISK,StorageType.DISK},hot);
    Map<String, Long> actualOutput = convertToStringMap(sts);
    Assert.assertEquals(4,actualOutput.size());
    Map<String, Long>  expectedOutput = new HashMap<>();
    expectedOutput.put("HOT|DISK:1(HOT)", 1l);
    expectedOutput.put("HOT|DISK:2(HOT)", 1l);
    expectedOutput.put("HOT|DISK:3(HOT)", 1l);
    expectedOutput.put("HOT|DISK:4(HOT)", 1l);
    Assert.assertEquals(expectedOutput,actualOutput);
  }
  
  @Test
  public void testMultipleHotsWithDifferentCounts() {
    BlockStoragePolicySuite bsps = BlockStoragePolicySuite.createDefaultSuite();
    StoragePolicySummary sts = new StoragePolicySummary(bsps.getAllPolicies());
    BlockStoragePolicy hot = bsps.getPolicy("HOT");
    sts.add(new StorageType[]{StorageType.DISK},hot);
    sts.add(new StorageType[]{StorageType.DISK,StorageType.DISK},hot);
    sts.add(new StorageType[]{StorageType.DISK,StorageType.DISK},hot);
    sts.add(new StorageType[]{StorageType.DISK,
        StorageType.DISK,StorageType.DISK},hot);
    sts.add(new StorageType[]{StorageType.DISK,
        StorageType.DISK,StorageType.DISK},hot);
    sts.add(new StorageType[]{StorageType.DISK,
        StorageType.DISK,StorageType.DISK,StorageType.DISK},hot);
    Map<String, Long> actualOutput = convertToStringMap(sts);
    Assert.assertEquals(4,actualOutput.size());
    Map<String, Long> expectedOutput = new HashMap<>();
    expectedOutput.put("HOT|DISK:1(HOT)", 1l);
    expectedOutput.put("HOT|DISK:2(HOT)", 2l);
    expectedOutput.put("HOT|DISK:3(HOT)", 2l);
    expectedOutput.put("HOT|DISK:4(HOT)", 1l);
    Assert.assertEquals(expectedOutput,actualOutput);
  }
  
  @Test
  public void testMultipleWarmsInDifferentOrder() {
    BlockStoragePolicySuite bsps = BlockStoragePolicySuite.createDefaultSuite();
    StoragePolicySummary sts = new StoragePolicySummary(bsps.getAllPolicies());
    BlockStoragePolicy warm = bsps.getPolicy("WARM");
    //DISK:1,ARCHIVE:1
    sts.add(new StorageType[]{StorageType.DISK,StorageType.ARCHIVE},warm);
    sts.add(new StorageType[]{StorageType.ARCHIVE,StorageType.DISK},warm);
    //DISK:2,ARCHIVE:1
    sts.add(new StorageType[]{StorageType.ARCHIVE,
        StorageType.DISK,StorageType.DISK},warm);
    sts.add(new StorageType[]{StorageType.DISK,
        StorageType.ARCHIVE,StorageType.DISK},warm);
    sts.add(new StorageType[]{StorageType.DISK,
        StorageType.DISK,StorageType.ARCHIVE},warm);
    //DISK:1,ARCHIVE:2
    sts.add(new StorageType[]{StorageType.DISK,
        StorageType.ARCHIVE,StorageType.ARCHIVE},warm);
    sts.add(new StorageType[]{StorageType.ARCHIVE,
        StorageType.DISK,StorageType.ARCHIVE},warm);
    sts.add(new StorageType[]{StorageType.ARCHIVE,
        StorageType.ARCHIVE,StorageType.DISK},warm);
    //DISK:2,ARCHIVE:2
    sts.add(new StorageType[]{StorageType.ARCHIVE,
        StorageType.ARCHIVE,StorageType.DISK,StorageType.DISK},warm);
    Map<String, Long> actualOutput = convertToStringMap(sts);
    Assert.assertEquals(4,actualOutput.size());
    Map<String, Long>  expectedOutput = new HashMap<>();
    expectedOutput.put("WARM|DISK:1,ARCHIVE:1(WARM)", 2l);
    expectedOutput.put("WARM|DISK:2,ARCHIVE:1", 3l);
    expectedOutput.put("WARM|DISK:1,ARCHIVE:2(WARM)", 3l);
    expectedOutput.put("WARM|DISK:2,ARCHIVE:2", 1l);
    Assert.assertEquals(expectedOutput,actualOutput);
  }
  
  @Test
  public void testDifferentSpecifiedPolicies() {
    BlockStoragePolicySuite bsps = BlockStoragePolicySuite.createDefaultSuite();
    StoragePolicySummary sts = new StoragePolicySummary(bsps.getAllPolicies());
    BlockStoragePolicy hot = bsps.getPolicy("HOT");
    BlockStoragePolicy warm = bsps.getPolicy("WARM");
    BlockStoragePolicy cold = bsps.getPolicy("COLD");
    //DISK:3
    sts.add(new StorageType[]{StorageType.DISK,StorageType.DISK,StorageType.DISK},hot);
    sts.add(new StorageType[]{StorageType.DISK,StorageType.DISK,StorageType.DISK},hot);
    sts.add(new StorageType[]{StorageType.DISK,StorageType.DISK,StorageType.DISK},warm);
    sts.add(new StorageType[]{StorageType.DISK,StorageType.DISK,StorageType.DISK},cold);
    //DISK:1,ARCHIVE:2
    sts.add(new StorageType[]{StorageType.DISK,
        StorageType.ARCHIVE,StorageType.ARCHIVE},hot);
    sts.add(new StorageType[]{StorageType.ARCHIVE,
        StorageType.DISK,StorageType.ARCHIVE},warm);
    sts.add(new StorageType[]{StorageType.ARCHIVE,
        StorageType.ARCHIVE,StorageType.DISK},cold);
    sts.add(new StorageType[]{StorageType.ARCHIVE,
        StorageType.ARCHIVE,StorageType.DISK},cold);
    //ARCHIVE:3
    sts.add(new StorageType[]{StorageType.ARCHIVE,
        StorageType.ARCHIVE,StorageType.ARCHIVE},hot);
    sts.add(new StorageType[]{StorageType.ARCHIVE,
        StorageType.ARCHIVE,StorageType.ARCHIVE},hot);
    sts.add(new StorageType[]{StorageType.ARCHIVE,
        StorageType.ARCHIVE,StorageType.ARCHIVE},warm);
    sts.add(new StorageType[]{StorageType.ARCHIVE,
        StorageType.ARCHIVE,StorageType.ARCHIVE},cold);
    Map<String, Long> actualOutput = convertToStringMap(sts);
    Assert.assertEquals(9,actualOutput.size());
    Map<String, Long>  expectedOutput = new HashMap<>();
    expectedOutput.put("HOT|DISK:3(HOT)", 2l);
    expectedOutput.put("COLD|DISK:1,ARCHIVE:2(WARM)", 2l);
    expectedOutput.put("HOT|ARCHIVE:3(COLD)", 2l);
    expectedOutput.put("WARM|DISK:3(HOT)", 1l);
    expectedOutput.put("COLD|DISK:3(HOT)", 1l);
    expectedOutput.put("WARM|ARCHIVE:3(COLD)", 1l);
    expectedOutput.put("WARM|DISK:1,ARCHIVE:2(WARM)", 1l);
    expectedOutput.put("COLD|ARCHIVE:3(COLD)", 1l);
    expectedOutput.put("HOT|DISK:1,ARCHIVE:2(WARM)", 1l);
    Assert.assertEquals(expectedOutput,actualOutput);
  }
  
  @Test
  public void testSortInDescendingOrder() {
    BlockStoragePolicySuite bsps = BlockStoragePolicySuite.createDefaultSuite();
    StoragePolicySummary sts = new StoragePolicySummary(bsps.getAllPolicies());
    BlockStoragePolicy hot = bsps.getPolicy("HOT");
    BlockStoragePolicy warm = bsps.getPolicy("WARM");
    BlockStoragePolicy cold = bsps.getPolicy("COLD");
    //DISK:3
    sts.add(new StorageType[]{StorageType.DISK,StorageType.DISK,StorageType.DISK},hot);
    sts.add(new StorageType[]{StorageType.DISK,StorageType.DISK,StorageType.DISK},hot);
    //DISK:1,ARCHIVE:2
    sts.add(new StorageType[]{StorageType.DISK,
        StorageType.ARCHIVE,StorageType.ARCHIVE},warm);
    sts.add(new StorageType[]{StorageType.ARCHIVE,
        StorageType.DISK,StorageType.ARCHIVE},warm);
    sts.add(new StorageType[]{StorageType.ARCHIVE,
        StorageType.ARCHIVE,StorageType.DISK},warm);
    //ARCHIVE:3
    sts.add(new StorageType[]{StorageType.ARCHIVE,
        StorageType.ARCHIVE,StorageType.ARCHIVE},cold);
    sts.add(new StorageType[]{StorageType.ARCHIVE,
        StorageType.ARCHIVE,StorageType.ARCHIVE},cold);
    sts.add(new StorageType[]{StorageType.ARCHIVE,
        StorageType.ARCHIVE,StorageType.ARCHIVE},cold);
    sts.add(new StorageType[]{StorageType.ARCHIVE,
        StorageType.ARCHIVE,StorageType.ARCHIVE},cold);
    Map<String, Long> actualOutput = convertToStringMap(sts);
    Assert.assertEquals(3,actualOutput.size());
    Map<String, Long>  expectedOutput = new LinkedHashMap<>();
    expectedOutput.put("COLD|ARCHIVE:3(COLD)", 4l);
    expectedOutput.put("WARM|DISK:1,ARCHIVE:2(WARM)", 3l);
    expectedOutput.put("HOT|DISK:3(HOT)", 2l);
    Assert.assertEquals(expectedOutput.toString(),actualOutput.toString());
  }
}
