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
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.io.nativeio.NativeIO;

/**
 * Creates MappableBlockLoader.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class MappableBlockLoaderFactory {

  private MappableBlockLoaderFactory() {
    // Prevent instantiation
  }

  /**
   * Create a specific cache loader according to the configuration.
   * If persistent memory volume is not configured, return a cache loader
   * for DRAM cache. Otherwise, return a cache loader for pmem cache.
   */
  public static MappableBlockLoader createCacheLoader(DNConf conf) {
    if (conf.getPmemVolumes() == null || conf.getPmemVolumes().length == 0) {
      return new MemoryMappableBlockLoader();
    }
    if (NativeIO.isAvailable() && NativeIO.POSIX.isPmdkAvailable()) {
      return new NativePmemMappableBlockLoader();
    }
    return new PmemMappableBlockLoader();
  }
}
