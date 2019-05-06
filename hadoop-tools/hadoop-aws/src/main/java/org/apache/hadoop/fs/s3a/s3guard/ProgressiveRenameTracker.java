/*
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

package org.apache.hadoop.fs.s3a.s3guard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.util.DurationInfo;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.s3guard.S3Guard.addMoveAncestors;
import static org.apache.hadoop.fs.s3a.s3guard.S3Guard.addMoveDir;

/**
 * This rename tracker progressively updates the metadata store
 * as it proceeds, during the parallelized copy operation.
 *
 * Algorithm
 * <ol>
 *   <li>
 *     As {@link #fileCopied(Path, FileStatus, Path, long, boolean)} callbacks
 *     are raised, the metastore is updated.
 *   </li>
 *   <li>
 *     Including parent entries, as appropriate.
 *   </li>
 *   <li>
 *     All directories which have been created are tracked locally,
 *     to avoid needing to read the store; this is a thread-safe structure.
 *   </li>
 *   <li>
 *    The actual update is performed out of any synchronized block.
 *   </li>
 *   <li>
 *     The delete list is also (currently) built up.
 *   </li>
 * </ol>
 * <pre>
 *
 * </pre>
 */
public class ProgressiveRenameTracker extends RenameTracker {

  private final Object lock = new Object();

  private final StoreContext storeContext;

  private final MetadataStore metadataStore;

  private final Collection<Path> sourcePaths = new HashSet<>();

  private final List<PathMetadata> destMetas = new ArrayList<>();

  public ProgressiveRenameTracker(
      final StoreContext storeContext,
      final MetadataStore metadataStore,
      final Path sourceRoot,
      final Path dest) {
    super(storeContext, sourceRoot, dest);
    this.storeContext = storeContext;
    this.metadataStore = metadataStore;
  }

  public StoreContext getStoreContext() {
    return storeContext;
  }

  @Override
  public void fileCopied(
      final Path sourcePath,
      final FileStatus sourceStatus,
      final Path destPath,
      final long blockSize,
      final boolean addAncestors) throws IOException {

    // build the list of entries to add in a synchronized block.
    List<PathMetadata> entriesToAdd;

    synchronized (lock) {
      checkArgument(!sourcePaths.contains(sourcePath),
          "File being renamed is already processed %s", destPath);
      // create the file metadata and update the local structures.
      PathMetadata newEntry = checkNotNull(
          S3Guard.addMoveFile(metadataStore,
              sourcePaths,
              destMetas,
              sourcePath,
              destPath,
              sourceStatus.getLen(),
              blockSize,
              getOwner()));
      if (addAncestors) {
        // add all new ancestors. The null check is to keep code checks
        // happy.
        entriesToAdd = checkNotNull(
            addMoveAncestors(
                metadataStore,
                sourcePaths,
                destMetas,
                getSourceRoot(),
                sourcePath,
                destPath,
                getOwner()));
      } else {
        // no ancestors, so create an empty list instead.
        entriesToAdd = new ArrayList<>(1);
      }
      // add the final entry
      entriesToAdd.add(newEntry);
    }

    // outside the lock, the entriesToAdd list has all new files to create.
    // ...so update the store.
    metadataStore.put(entriesToAdd);
  }

  @Override
  public void directoryMarkerCopied(final FileStatus sourceStatus,
      final Path destPath,
      final boolean addAncestors) throws IOException {
    List<PathMetadata> entriesToAdd;
    synchronized (lock) {
      PathMetadata newEntry = checkNotNull(
          addMoveDir(metadataStore, sourcePaths, destMetas,
              sourceStatus.getPath(),
              destPath, getOwner()));
      // Ancestor directories may not be listed, so we explicitly add them
      if (addAncestors) {
        entriesToAdd = checkNotNull(
            addMoveAncestors(metadataStore,
                sourcePaths,
                destMetas,
                getSourceRoot(),
                sourceStatus.getPath(),
                destPath,
                getOwner()));
      } else {
        // no ancestors, so create an empty list instead.
        entriesToAdd = new ArrayList<>(1);
      }
      // add the final entry
      entriesToAdd.add(newEntry);
    }
    // outside the lock, the entriesToAdd list has all new files to create.
    // ...so update the store.
    try (DurationInfo ignored = new DurationInfo(LOG, false,
        "adding %s metastore entries", entriesToAdd.size())) {
      metadataStore.move(null, entriesToAdd);
    }
  }

  @Override
  public synchronized void noteSourceDirectoryMoved() throws IOException {
    if (!sourcePaths.contains(getSourceRoot())) {
      addMoveDir(metadataStore, sourcePaths, destMetas,
          getSourceRoot(),
          getDest(), getOwner());
    }
  }

  /**
   * As source objects are deleted, so is the list of entries.
   * @param paths keys of objects deleted.
   * @throws IOException failure.
   */
  @Override
  public void sourceObjectsDeleted(
      final List<Path> paths) throws IOException {

    // delete the paths from the metastore
    try (DurationInfo ignored = new DurationInfo(LOG, false,
        "Deleting %s metastore entries", paths.size())) {
      metadataStore.move(paths, null);
    }
  }

  @Override
  public void completeRename() throws IOException {
    // this should all have happened.
    metadataStore.move(sourcePaths, destMetas);
    super.completeRename();
  }

  @Override
  public IOException renameFailed(final Exception ex) {
    LOG.debug("Rename has failed", ex);
    return super.renameFailed(ex);
  }
}
