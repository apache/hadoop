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
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

/**
 * This is an interface for the underlying volume.
 */
public interface FsVolumeSpi {
  /**
   * Obtain a reference object that had increased 1 reference count of the
   * volume.
   *
   * It is caller's responsibility to close {@link FsVolumeReference} to decrease
   * the reference count on the volume.
   */
  FsVolumeReference obtainReference() throws ClosedChannelException;

  /** @return the StorageUuid of the volume */
  String getStorageID();

  /** @return a list of block pools. */
  String[] getBlockPoolList();

  /** @return the available storage space in bytes. */
  long getAvailable() throws IOException;

  /** @return the base path to the volume */
  String getBasePath();

  /** @return the path to the volume */
  String getPath(String bpid) throws IOException;

  /** @return the directory for the finalized blocks in the block pool. */
  File getFinalizedDir(String bpid) throws IOException;
  
  StorageType getStorageType();

  /** Returns true if the volume is NOT backed by persistent storage. */
  boolean isTransientStorage();

  /**
   * Reserve disk space for a block (RBW or Re-replicating)
   * so a writer does not run out of space before the block is full.
   */
  void reserveSpaceForReplica(long bytesToReserve);

  /**
   * Release disk space previously reserved for block opened for write.
   */
  void releaseReservedSpace(long bytesToRelease);

  /**
   * Release reserved memory for an RBW block written to transient storage
   * i.e. RAM.
   * bytesToRelease will be rounded down to the OS page size since locked
   * memory reservation must always be a multiple of the page size.
   */
  void releaseLockedMemory(long bytesToRelease);

  /**
   * BlockIterator will return ExtendedBlock entries from a block pool in
   * this volume.  The entries will be returned in sorted order.<p/>
   *
   * BlockIterator objects themselves do not always have internal
   * synchronization, so they can only safely be used by a single thread at a
   * time.<p/>
   *
   * Closing the iterator does not save it.  You must call save to save it.
   */
  interface BlockIterator extends Closeable {
    /**
     * Get the next block.<p/>
     *
     * Note that this block may be removed in between the time we list it,
     * and the time the caller tries to use it, or it may represent a stale
     * entry.  Callers should handle the case where the returned block no
     * longer exists.
     *
     * @return               The next block, or null if there are no
     *                         more blocks.  Null if there was an error
     *                         determining the next block.
     *
     * @throws IOException   If there was an error getting the next block in
     *                         this volume.  In this case, EOF will be set on
     *                         the iterator.
     */
    ExtendedBlock nextBlock() throws IOException;

    /**
     * Returns true if we got to the end of the block pool.
     */
    boolean atEnd();

    /**
     * Repositions the iterator at the beginning of the block pool.
     */
    void rewind();

    /**
     * Save this block iterator to the underlying volume.
     * Any existing saved block iterator with this name will be overwritten.
     * maxStalenessMs will not be saved.
     *
     * @throws IOException   If there was an error when saving the block
     *                         iterator.
     */
    void save() throws IOException;

    /**
     * Set the maximum staleness of entries that we will return.<p/>
     *
     * A maximum staleness of 0 means we will never return stale entries; a
     * larger value will allow us to reduce resource consumption in exchange
     * for returning more potentially stale entries.  Even with staleness set
     * to 0, consumers of this API must handle race conditions where block
     * disappear before they can be processed.
     */
    void setMaxStalenessMs(long maxStalenessMs);

    /**
     * Get the wall-clock time, measured in milliseconds since the Epoch,
     * when this iterator was created.
     */
    long getIterStartMs();

    /**
     * Get the wall-clock time, measured in milliseconds since the Epoch,
     * when this iterator was last saved.  Returns iterStartMs if the
     * iterator was never saved.
     */
    long getLastSavedMs();

    /**
     * Get the id of the block pool which this iterator traverses.
     */
    String getBlockPoolId();
  }

  /**
   * Create a new block iterator.  It will start at the beginning of the
   * block set.
   *
   * @param bpid             The block pool id to iterate over.
   * @param name             The name of the block iterator to create.
   *
   * @return                 The new block iterator.
   */
  BlockIterator newBlockIterator(String bpid, String name);

  /**
   * Load a saved block iterator.
   *
   * @param bpid             The block pool id to iterate over.
   * @param name             The name of the block iterator to load.
   *
   * @return                 The saved block iterator.
   * @throws IOException     If there was an IO error loading the saved
   *                           block iterator.
   */
  BlockIterator loadBlockIterator(String bpid, String name) throws IOException;

  /**
   * Get the FSDatasetSpi which this volume is a part of.
   */
  FsDatasetSpi getDataset();
}
