/**
 * Copyright 2008 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;

class UnmodifyableHRegionInfo extends HRegionInfo {
  /* Default constructor - creates empty object */
  UnmodifyableHRegionInfo() {
    super(new UnmodifyableHTableDescriptor(), null, null);
  }
  
  /*
   * Construct HRegionInfo with explicit parameters
   * 
   * @param tableDesc the table descriptor
   * @param startKey first key in region
   * @param endKey end of key range
   * @throws IllegalArgumentException
   */
  UnmodifyableHRegionInfo(final HTableDescriptor tableDesc,
      final byte [] startKey, final byte [] endKey)
  throws IllegalArgumentException {
    super(new UnmodifyableHTableDescriptor(tableDesc), startKey, endKey, false);
  }

  /*
   * Construct HRegionInfo with explicit parameters
   * 
   * @param tableDesc the table descriptor
   * @param startKey first key in region
   * @param endKey end of key range
   * @param split true if this region has split and we have daughter regions
   * regions that may or may not hold references to this region.
   * @throws IllegalArgumentException
   */
  UnmodifyableHRegionInfo(HTableDescriptor tableDesc,
      final byte [] startKey, final byte [] endKey, final boolean split)
  throws IllegalArgumentException {
    super(new UnmodifyableHTableDescriptor(tableDesc), startKey, endKey, split);
  }
  
  /*
   * Creates an unmodifyable copy of an HRegionInfo
   * 
   * @param info
   */
  UnmodifyableHRegionInfo(HRegionInfo info) {
    super(new UnmodifyableHTableDescriptor(info.getTableDesc()),
        info.getStartKey(), info.getEndKey(), info.isSplit());
  }
  
  /**
   * @param split set split status
   */
  @Override
  public void setSplit(boolean split) {
    throw new UnsupportedOperationException("HRegionInfo is read-only");
  }

  /**
   * @param offLine set online - offline status
   */
  @Override
  public void setOffline(boolean offLine) {
    throw new UnsupportedOperationException("HRegionInfo is read-only");
  }
}
