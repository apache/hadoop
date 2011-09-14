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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HConstants;

/**
 * A {@link RegionSplitPolicy} implementation which splits a region
 * as soon as any of its store files exceeds a maximum configurable
 * size.
 */
class ConstantSizeRegionSplitPolicy extends RegionSplitPolicy {
  private long desiredMaxFileSize;

  @Override
  void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    long maxFileSize = region.getTableDesc().getMaxFileSize();

    // By default we split region if a file > HConstants.DEFAULT_MAX_FILE_SIZE.
    if (maxFileSize == HConstants.DEFAULT_MAX_FILE_SIZE) {
      maxFileSize = getConf().getLong("hbase.hregion.max.filesize",
        HConstants.DEFAULT_MAX_FILE_SIZE);
    }
    this.desiredMaxFileSize = maxFileSize;
  }
  
  @Override
  boolean shouldSplit() {
    boolean force = region.shouldForceSplit();
    boolean foundABigStore = false;
    
    for (Store store : region.getStores().values()) {
      // If any of the stores are unable to split (eg they contain reference files)
      // then don't split
      if ((!store.canSplit())) {
        return false;
      }

      // Mark if any store is big enough
      if (store.getSize() > desiredMaxFileSize) {
        foundABigStore = true;
      }
    }

    return foundABigStore || force;
  }

  long getDesiredMaxFileSize() {
    return desiredMaxFileSize;
  }
}
