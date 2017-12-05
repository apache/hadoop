/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.aliasmap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.server.common.FileRegion;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Protocol used by clients to read/write data about aliases of
 * provided blocks for an in-memory implementation of the
 * {@link org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface InMemoryAliasMapProtocol {

  /**
   * The result of a read from the in-memory aliasmap. It contains the
   * a list of FileRegions that are returned, along with the next block
   * from which the read operation must continue.
   */
  class IterationResult {

    private final List<FileRegion> batch;
    private final Optional<Block> nextMarker;

    public IterationResult(List<FileRegion> batch, Optional<Block> nextMarker) {
      this.batch = batch;
      this.nextMarker = nextMarker;
    }

    public List<FileRegion> getFileRegions() {
      return batch;
    }

    public Optional<Block> getNextBlock() {
      return nextMarker;
    }
  }

  /**
   * List the next batch of {@link FileRegion}s in the alias map starting from
   * the given {@code marker}. To retrieve all {@link FileRegion}s stored in the
   * alias map, multiple calls to this function might be required.
   * @param marker the next block to get fileregions from.
   * @return the {@link IterationResult} with a set of
   * FileRegions and the next marker.
   * @throws IOException
   */
  InMemoryAliasMap.IterationResult list(Optional<Block> marker)
      throws IOException;

  /**
   * Gets the {@link ProvidedStorageLocation} associated with the
   * specified block.
   * @param block the block to lookup
   * @return the associated {@link ProvidedStorageLocation}.
   * @throws IOException
   */
  @Nonnull
  Optional<ProvidedStorageLocation> read(@Nonnull Block block)
      throws IOException;

  /**
   * Stores the block and it's associated {@link ProvidedStorageLocation}
   * in the alias map.
   * @param block
   * @param providedStorageLocation
   * @throws IOException
   */
  void write(@Nonnull Block block,
      @Nonnull ProvidedStorageLocation providedStorageLocation)
      throws IOException;

  /**
   * Get the associated block pool id.
   * @return the block pool id associated with the Namenode running
   * the in-memory alias map.
   */
  String getBlockPoolId() throws IOException;
}
