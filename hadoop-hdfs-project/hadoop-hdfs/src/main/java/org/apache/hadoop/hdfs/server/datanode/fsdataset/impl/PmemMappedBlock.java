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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Represents an HDFS block that is mapped to persistent memory by DataNode
 * with mapped byte buffer. PMDK is NOT involved in this implementation.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PmemMappedBlock implements MappableBlock {
  private static final Logger LOG =
      LoggerFactory.getLogger(PmemMappedBlock.class);
  private final PmemVolumeManager pmemVolumeManager;
  private long length;
  private Byte volumeIndex = null;
  private ExtendedBlockId key;

  PmemMappedBlock(long length, Byte volumeIndex, ExtendedBlockId key,
                  PmemVolumeManager pmemVolumeManager) {
    assert length > 0;
    this.length = length;
    this.volumeIndex = volumeIndex;
    this.key = key;
    this.pmemVolumeManager = pmemVolumeManager;
  }

  @Override
  public long getLength() {
    return length;
  }

  @Override
  public void close() {
    String cacheFilePath =
        pmemVolumeManager.inferCacheFilePath(volumeIndex, key);
    try {
      FsDatasetUtil.deleteMappedFile(cacheFilePath);
      pmemVolumeManager.afterUncache(key);
      LOG.info("Successfully uncached one replica:{} from persistent memory"
          + ", [cached path={}, length={}]", key, cacheFilePath, length);
    } catch (IOException e) {
      LOG.warn("Failed to delete the mapped File: {}!", cacheFilePath, e);
    }
  }
}