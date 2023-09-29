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
import org.apache.hadoop.io.nativeio.NativeIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Represents an HDFS block that is mapped to persistent memory by the DataNode.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NativePmemMappedBlock implements MappableBlock {
  private static final Logger LOG =
      LoggerFactory.getLogger(NativePmemMappedBlock.class);

  private long pmemMappedAddress = -1L;
  private long length;
  private ExtendedBlockId key;

  NativePmemMappedBlock(long pmemMappedAddress, long length,
      ExtendedBlockId key) {
    assert length > 0;
    this.pmemMappedAddress = pmemMappedAddress;
    this.length = length;
    this.key = key;
  }

  @Override
  public long getLength() {
    return length;
  }

  @Override
  public long getAddress() {
    return pmemMappedAddress;
  }

  @Override
  public ExtendedBlockId getKey() {
    return key;
  }

  @Override
  public void close() {
    if (pmemMappedAddress != -1L) {
      try {
        String cacheFilePath =
            PmemVolumeManager.getInstance().getCachePath(key);
        // Current libpmem will report error when pmem_unmap is called with
        // length not aligned with page size, although the length is returned
        // by pmem_map_file.
        boolean success =
            NativeIO.POSIX.Pmem.unmapBlock(pmemMappedAddress, length);
        if (!success) {
          throw new IOException("Failed to unmap the mapped file from " +
              "pmem address: " + pmemMappedAddress);
        }
        pmemMappedAddress = -1L;
        FsDatasetUtil.deleteMappedFile(cacheFilePath);
        LOG.info("Successfully uncached one replica:{} from persistent memory"
            + ", [cached path={}, length={}]", key, cacheFilePath, length);
      } catch (IOException e) {
        LOG.warn("IOException occurred for block {}!", key, e);
      }
    }
  }
}
