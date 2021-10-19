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

import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.hdfs.util.ReferenceCountMap.ReferenceCounter;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;

/**
 * Feature that represents the ACLs of the inode.
 */
@InterfaceAudience.Private
public class AclFeature implements INode.Feature, ReferenceCounter {
  public static final ImmutableList<AclEntry> EMPTY_ENTRY_LIST =
    ImmutableList.of();
  private int refCount = 0;

  private final int [] entries;

  public AclFeature(int[] entries) {
    this.entries = entries;
  }

  /**
   * Get the number of entries present
   */
  int getEntriesSize() {
    return entries.length;
  }

  /**
   * Get the entry at the specified position
   * @param pos Position of the entry to be obtained
   * @return integer representation of AclEntry
   * @throws IndexOutOfBoundsException if pos out of bound
   */
  int getEntryAt(int pos) {
    Preconditions.checkPositionIndex(pos, entries.length,
        "Invalid position for AclEntry");
    return entries[pos];
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    return Arrays.equals(entries, ((AclFeature) o).entries);
  }

  @Override
  public String toString() {
    return "AclFeature : " + Integer.toHexString(hashCode()) + " Size of entries : " + entries.length;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(entries);
  }

  @Override
  public synchronized int getRefCount() {
    return refCount;
  }

  @Override
  public synchronized int incrementAndGetRefCount() {
    return ++refCount;
  }

  @Override
  public synchronized int decrementAndGetRefCount() {
    return (refCount > 0) ? --refCount : 0;
  }
}
