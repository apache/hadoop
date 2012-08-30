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
package org.apache.hadoop.fs;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Wrapper for {@link BlockLocation} that also adds {@link VolumeId} volume
 * location information for each replica.
 */
@InterfaceStability.Unstable
@InterfaceAudience.Public
public class BlockStorageLocation extends BlockLocation {

  private final VolumeId[] volumeIds;

  public BlockStorageLocation(BlockLocation loc, VolumeId[] volumeIds)
      throws IOException {
    // Initialize with data from passed in BlockLocation
    super(loc.getNames(), loc.getHosts(), loc.getTopologyPaths(), loc
        .getOffset(), loc.getLength(), loc.isCorrupt());
    this.volumeIds = volumeIds;
  }

  /**
   * Gets the list of {@link VolumeId} corresponding to the block's replicas.
   * 
   * @return volumeIds list of VolumeId for the block's replicas
   */
  public VolumeId[] getVolumeIds() {
    return volumeIds;
  }
}
