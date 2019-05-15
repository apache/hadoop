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
 *     are raised, the metastore is updated with the new file entry.
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
 *     When deletes are executed, the store is also updated.
 *   </li>
 *   <li>
 *     And at the completion of a successful rename, the source directory is also removed.
 *   </li>
 * </ol>
 * <pre>
 *
 * </pre>
 */
public class ProgressiveRenameTracker extends RenameTracker {

  /**
   * The collection of paths to delete; this is added as individual files
   * are renamed.
   * The metastore is only updated with these entries after the DELETE
   * call succeeds.
   */
  private final Collection<Path> pathsToDelete = new HashSet<>();

  private final List<PathMetadata> destMetas = new ArrayList<>();

  public ProgressiveRenameTracker(
      final StoreContext storeContext,
      final MetadataStore metadataStore,
      final Path sourceRoot,
      final Path dest,
      final BulkOperationState operationState) {
    super("ProgressiveRenameTracker",
        storeContext, metadataStore, sourceRoot, dest, operationState);
  }

  /**
   * When a file is copied, any ancestors
   * are calculated and then the store is updated with
   * the destination entries.
   *
   * The source entries are added to the {@link #pathsToDelete} list.
   * @param sourcePath path of source
   * @param sourceStatus status of source.
   * @param destPath destination path.
   * @param blockSize block size.
   * @param addAncestors should ancestors be added?
   * @throws IOException failure
   */
  @Override
  public void fileCopied(
      final Path sourcePath,
      final FileStatus sourceStatus,
      final Path destPath,
      final long blockSize,
      final boolean addAncestors) throws IOException {

    // build the list of entries to add in a synchronized block.
    final List<PathMetadata> entriesToAdd = new ArrayList<>(1);
    LOG.debug("Updating store with copied file {}", sourcePath);
    MetadataStore store = getMetadataStore();
    synchronized (this) {
      checkArgument(!pathsToDelete.contains(sourcePath),
          "File being renamed is already processed %s", destPath);
      // create the file metadata and update the local structures.
      S3Guard.addMoveFile(
          store,
          pathsToDelete,
          entriesToAdd,
          sourcePath,
          destPath,
          sourceStatus.getLen(),
          blockSize,
          getOwner());
      LOG.debug("New metastore entry : {}", entriesToAdd.get(0));
      if (addAncestors) {
        // add all new ancestors.
        addMoveAncestors(
            store,
            pathsToDelete,
            entriesToAdd,
            getSourceRoot(),
            sourcePath,
            destPath,
            getOwner());
      }
    }

    // outside the lock, the entriesToAdd list has all new files to create.
    // ...so update the store.
    store.move(null, entriesToAdd, getOperationState());
  }

  /**
   * A directory marker has been added.
   * Add the new entry and record the source path as another entry to delete.
   * @param sourceStatus status of source.
   * @param destPath destination path.
   * @param addAncestors should ancestors be added?
   * @throws IOException failure.
   */
  @Override
  public void directoryMarkerCopied(final FileStatus sourceStatus,
      final Path destPath,
      final boolean addAncestors) throws IOException {
    // this list is created on demand.
    final List<PathMetadata> entriesToAdd = new ArrayList<>(1);
    MetadataStore store = getMetadataStore();
    synchronized (this) {
      addMoveDir(store,
          pathsToDelete,
          entriesToAdd,
          sourceStatus.getPath(),
          destPath,
          getOwner());
      // Ancestor directories may not be listed, so we explicitly add them
      if (addAncestors) {
        addMoveAncestors(store,
            pathsToDelete,
            entriesToAdd,
            getSourceRoot(),
            sourceStatus.getPath(),
            destPath,
            getOwner());
      }
    }
    // outside the lock, the entriesToAdd list has all new files to create.
    // ...so update the store.
    try (DurationInfo ignored = new DurationInfo(LOG, false,
        "adding %s metastore entries", entriesToAdd.size())) {
      store.move(null, entriesToAdd, getOperationState());
    }
  }

  @Override
  public synchronized void moveSourceDirectory() throws IOException {
    if (!pathsToDelete.contains(getSourceRoot())) {
      addMoveDir(getMetadataStore(), pathsToDelete, destMetas,
          getSourceRoot(),
          getDest(),
          getOwner());
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
        "delete %s metastore entries", paths.size())) {
      getMetadataStore().move(paths, null, getOperationState());
    }
  }

  @Override
  public void completeRename() throws IOException {
    // this should all have happened.
    LOG.debug("Rename completed for {}", this);
    getMetadataStore().move(pathsToDelete, destMetas, getOperationState());
    super.completeRename();
  }

}
