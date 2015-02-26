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

import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;

/**
 * Aggregate the storage type information for a set of blocks
 *
 */
public class StoragePolicySummary {

  Map<StorageTypeAllocation, Long> storageComboCounts = new HashMap<>();
  final BlockStoragePolicy[] storagePolicies;
  int totalBlocks;

  StoragePolicySummary(BlockStoragePolicy[] storagePolicies) {
    this.storagePolicies = storagePolicies;
  }

  // Add a storage type combination
  void add(StorageType[] storageTypes, BlockStoragePolicy policy) {
    StorageTypeAllocation storageCombo = 
        new StorageTypeAllocation(storageTypes, policy);
    Long count = storageComboCounts.get(storageCombo);
    if (count == null) {
      storageComboCounts.put(storageCombo, 1l);
      storageCombo.setActualStoragePolicy(
          getStoragePolicy(storageCombo.getStorageTypes()));
    } else {
      storageComboCounts.put(storageCombo, count.longValue()+1);
    }
    totalBlocks++;
  }

  // sort the storageType combinations based on the total blocks counts
  // in descending order
  static List<Entry<StorageTypeAllocation, Long>> sortByComparator(
      Map<StorageTypeAllocation, Long> unsortMap) {
    List<Entry<StorageTypeAllocation, Long>> storageAllocations = 
        new LinkedList<>(unsortMap.entrySet());
    // Sorting the list based on values
    Collections.sort(storageAllocations, 
      new Comparator<Entry<StorageTypeAllocation, Long>>() {
          public int compare(Entry<StorageTypeAllocation, Long> o1,
              Entry<StorageTypeAllocation, Long> o2)
          {
            return o2.getValue().compareTo(o1.getValue());
          }
    });
    return storageAllocations;
  }

  public String toString() {
    StringBuilder compliantBlocksSB = new StringBuilder();
    compliantBlocksSB.append("\nBlocks satisfying the specified storage policy:");
    compliantBlocksSB.append("\nStorage Policy                  # of blocks       % of blocks\n");
    StringBuilder nonCompliantBlocksSB = new StringBuilder();
    Formatter compliantFormatter = new Formatter(compliantBlocksSB);
    Formatter nonCompliantFormatter = new Formatter(nonCompliantBlocksSB);
    NumberFormat percentFormat = NumberFormat.getPercentInstance();
    percentFormat.setMinimumFractionDigits(4);
    percentFormat.setMaximumFractionDigits(4);
    for (Map.Entry<StorageTypeAllocation, Long> storageComboCount:
      sortByComparator(storageComboCounts)) {
      double percent = (double) storageComboCount.getValue() / 
          (double) totalBlocks;
      StorageTypeAllocation sta = storageComboCount.getKey();
      if (sta.policyMatches()) {
        compliantFormatter.format("%-25s %10d  %20s%n",
            sta.getStoragePolicyDescriptor(),
            storageComboCount.getValue(),
            percentFormat.format(percent));
      } else {
        if (nonCompliantBlocksSB.length() == 0) {
          nonCompliantBlocksSB.append("\nBlocks NOT satisfying the specified storage policy:");
          nonCompliantBlocksSB.append("\nStorage Policy                  ");
          nonCompliantBlocksSB.append(
              "Specified Storage Policy      # of blocks       % of blocks\n");
        }
        nonCompliantFormatter.format("%-35s %-20s %10d  %20s%n",
            sta.getStoragePolicyDescriptor(),
            sta.getSpecifiedStoragePolicy().getName(),
            storageComboCount.getValue(),
            percentFormat.format(percent));
      }
    }
    if (nonCompliantBlocksSB.length() == 0) {
      nonCompliantBlocksSB.append("\nAll blocks satisfy specified storage policy.\n");
    }
    compliantFormatter.close();
    nonCompliantFormatter.close();
    return compliantBlocksSB.toString() + nonCompliantBlocksSB;
  }

  /**
   * 
   * @param storageTypes - sorted array of storageTypes
   * @return Storage Policy which matches the specific storage Combination
   */
  private BlockStoragePolicy getStoragePolicy(StorageType[] storageTypes) {
    for (BlockStoragePolicy storagePolicy:storagePolicies) {
      StorageType[] policyStorageTypes = storagePolicy.getStorageTypes();
      policyStorageTypes = Arrays.copyOf(policyStorageTypes, policyStorageTypes.length);
      Arrays.sort(policyStorageTypes);
      if (policyStorageTypes.length <= storageTypes.length) {
        int i = 0; 
        for (; i < policyStorageTypes.length; i++) {
          if (policyStorageTypes[i] != storageTypes[i]) {
            break;
          }
        }
        if (i < policyStorageTypes.length) {
          continue;
        }
        int j=policyStorageTypes.length;
        for (; j < storageTypes.length; j++) {
          if (policyStorageTypes[i-1] != storageTypes[j]) {
            break;
          }
        }

        if (j==storageTypes.length) {
          return storagePolicy;
        }
      }
    }
    return null;
  }

  /**
   * Internal class which represents a unique Storage type combination
   *
   */
  static class StorageTypeAllocation {
    private final BlockStoragePolicy specifiedStoragePolicy;
    private final StorageType[] storageTypes;
    private BlockStoragePolicy actualStoragePolicy;

    StorageTypeAllocation(StorageType[] storageTypes, 
        BlockStoragePolicy specifiedStoragePolicy) {
      Arrays.sort(storageTypes);
      this.storageTypes = storageTypes;
      this.specifiedStoragePolicy = specifiedStoragePolicy;
    }
    
    StorageType[] getStorageTypes() {
      return storageTypes;
    }

    BlockStoragePolicy getSpecifiedStoragePolicy() {
      return specifiedStoragePolicy;
    }
    
    void setActualStoragePolicy(BlockStoragePolicy actualStoragePolicy) {
      this.actualStoragePolicy = actualStoragePolicy;
    }
    
    BlockStoragePolicy getActualStoragePolicy() {
      return actualStoragePolicy;
    }

    private static String getStorageAllocationAsString
      (Map<StorageType, Integer> storageType_countmap) {
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<StorageType, Integer> 
      storageTypeCountEntry:storageType_countmap.entrySet()) {
        sb.append(storageTypeCountEntry.getKey().name()+ ":"
            + storageTypeCountEntry.getValue() + ",");
      }
      if (sb.length() > 1) {
        sb.deleteCharAt(sb.length()-1);
      }
      return sb.toString();
    }

    private String getStorageAllocationAsString() {
      Map<StorageType, Integer> storageType_countmap = 
          new EnumMap<>(StorageType.class);
      for (StorageType storageType: storageTypes) {
        Integer count = storageType_countmap.get(storageType);
        if (count == null) {
          storageType_countmap.put(storageType, 1);
        } else {
          storageType_countmap.put(storageType, count.intValue()+1);
        }
      }
      return (getStorageAllocationAsString(storageType_countmap));
    }
    
    String getStoragePolicyDescriptor() {
      StringBuilder storagePolicyDescriptorSB = new StringBuilder();
      if (actualStoragePolicy!=null) {
        storagePolicyDescriptorSB.append(getStorageAllocationAsString())
        .append("(")
        .append(actualStoragePolicy.getName())
        .append(")");
      } else {
        storagePolicyDescriptorSB.append(getStorageAllocationAsString());
      }
      return storagePolicyDescriptorSB.toString();
    }
    
    boolean policyMatches() {
      return specifiedStoragePolicy.equals(actualStoragePolicy);
    }
    
    @Override
    public String toString() {
      return specifiedStoragePolicy.getName() + "|" + getStoragePolicyDescriptor();
    }

    @Override
    public int hashCode() {
      return Objects.hash(specifiedStoragePolicy,Arrays.hashCode(storageTypes));
    }

    @Override
    public boolean equals(Object another) {
      return (another instanceof StorageTypeAllocation && 
          Objects.equals(specifiedStoragePolicy,
              ((StorageTypeAllocation)another).specifiedStoragePolicy) &&
              Arrays.equals(storageTypes,
                  ((StorageTypeAllocation)another).storageTypes));
    }
  }
}
