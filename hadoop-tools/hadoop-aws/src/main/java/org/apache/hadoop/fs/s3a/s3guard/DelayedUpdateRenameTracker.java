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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import com.amazonaws.SdkBaseException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.fs.s3a.impl.StoreContext;

/**
 * This is the rename updating strategy originally used:
 * a collection of source paths and a list of destinations are created,
 * then updated at the end (possibly slow).
 */
public class DelayedUpdateRenameTracker extends RenameTracker {

  private final StoreContext storeContext;

  private final MetadataStore metadataStore;
  private final Collection<Path> srcPaths = new HashSet<>();

  private final List<PathMetadata> destMetas = new ArrayList<>();

  private final List<Path> deletedPaths = new ArrayList<>();

  public DelayedUpdateRenameTracker(
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
  public synchronized void fileCopied(
      final Path sourcePath,
      final FileStatus sourceStatus,
      final Path destPath,
      final long blockSize,
      final boolean addAncestors) throws IOException {
    S3Guard.addMoveFile(metadataStore,
        srcPaths,
        destMetas,
        sourcePath,
        destPath,
        sourceStatus.getLen(),
        blockSize,
        getOwner());
    // Ancestor directories may not be listed, so we explicitly add them
    if (addAncestors) {
      S3Guard.addMoveAncestors(metadataStore,
          srcPaths,
          destMetas,
          getSourceRoot(),
          sourceStatus.getPath(),
          destPath,
          getOwner());
    }
  }

  @Override
  public synchronized void directoryMarkerCopied(final FileStatus sourceStatus,
      final Path destPath,
      final boolean addAncestors) throws IOException {
    S3Guard.addMoveDir(metadataStore, srcPaths, destMetas,
        sourceStatus.getPath(),
        destPath, getOwner());
    // Ancestor directories may not be listed, so we explicitly add them
    if (addAncestors) {
      S3Guard.addMoveAncestors(metadataStore,
          srcPaths,
          destMetas,
          getSourceRoot(),
          sourceStatus.getPath(),
          destPath,
          getOwner());
    }
  }

  @Override
  public synchronized void noteSourceDirectoryMoved() throws IOException {
    if (!srcPaths.contains(getSourceRoot())) {
      S3Guard.addMoveDir(metadataStore, srcPaths, destMetas,
          getSourceRoot(),
          getDest(), getOwner());
    }
  }

  @Override
  public synchronized void sourceObjectsDeleted(
      final List<Path> paths) throws IOException {
    // convert to paths.
    deletedPaths.addAll(paths);
  }

  @Override
  public void completeRename() throws IOException {
    metadataStore.move(srcPaths, destMetas);
    super.completeRename();
  }

  @Override
  public IOException renameFailed(final Exception ex) {
    LOG.warn("Rename has failed; updating s3guard with destination state");
    try {
      // the destination paths are updated; the source is left alone.
      // either the delete operation didn't begin, or the
      metadataStore.move(new ArrayList<>(0), destMetas);
      Function<String, Path> qualifier
          = getStoreContext().getKeyToPathQualifier();
      for (Path deletedPath : deletedPaths) {
        // this is not ideal in that it may leave parent stuff around.
        metadataStore.delete(deletedPath);
      }
      deleteParentPaths();
    } catch (IOException | SdkBaseException e) {
      LOG.warn("Ignoring error raised in AWS SDK ", e);
    }

    return super.renameFailed(ex);
  }

  /**
   * Delete all the parent paths we know to be empty (by walking up the tree
   * deleting as appropriate).
   * @throws IOException failure
   */
  private void deleteParentPaths() throws IOException {
    Set<Path> parentPaths = new HashSet<>();
    for (Path deletedPath : deletedPaths) {
      Path parent = deletedPath.getParent();
      if (!parent.equals(getSourceRoot())) {
        parentPaths.add(parent);
      }
    }
    // now there's a set of parent paths. We now want to
    // get them ordered by depth, so that deeper entries come first
    // that way: when we check for a parent path existing we can
    // see if it really is empty.
    List<Path> parents = new ArrayList<>(parentPaths);
    parents.sort(
        Comparator.comparing(
            (Path p) -> p.depth())
            .thenComparing((Path p) -> p.toUri().getPath()));
    for (Path parent : parents) {
      PathMetadata md = metadataStore.get(parent, true);
      if (md != null && md.isEmptyDirectory().equals(Tristate.TRUE)) {
        // if were confident that this is empty: delete it.
        metadataStore.delete(parent);
      }
    }
  }
}
