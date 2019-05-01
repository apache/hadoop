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

import com.amazonaws.services.s3.model.DeleteObjectsRequest;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.impl.StoreContext;

/**
 * This is the rename updating strategy originally used:
 * a collection of source paths and a list of destinations are created,
 * then updated at the end (possibly slow)
 */
public class DelayedUpdateRenameOperation extends RenameOperation {

  private final StoreContext storeContext;

  private final MetadataStore metadataStore;
  private final Collection<Path> srcPaths = new HashSet<>();

  private final List<PathMetadata> dstMetas = new ArrayList<>();

  private final List<DeleteObjectsRequest.KeyVersion> deletedKeys = new ArrayList<>();

  public DelayedUpdateRenameOperation(
      final StoreContext storeContext,
      final MetadataStore metadataStore,
      final Path sourceRoot,
      final Path dest) {
    super(sourceRoot, dest, storeContext.getUsername());
    this.storeContext = storeContext;
    this.metadataStore = metadataStore;
  }

  @Override
  public void fileCopied(
      final Path childSource,
      final FileStatus sourceStatus,
      final Path destPath,
      final long blockSize,
      final boolean addAncestors
  ) throws IOException {
    S3Guard.addMoveFile(metadataStore,
        srcPaths,
        dstMetas,
        childSource,
        destPath,
        sourceStatus.getLen(),
        blockSize,
        getOwner());
    // Ancestor directories may not be listed, so we explicitly add them
    if (addAncestors) {
      S3Guard.addMoveAncestors(metadataStore,
          srcPaths,
          dstMetas,
          getSourceRoot(),
          sourceStatus.getPath(),
          destPath,
          getOwner());
    }
  }

  @Override
  public void directoryMarkerCopied(final FileStatus sourceStatus,
      final Path destPath,
      final boolean addAncestors) throws IOException {
    S3Guard.addMoveDir(metadataStore, srcPaths, dstMetas,
        sourceStatus.getPath(),
        destPath, getOwner());
    // Ancestor directories may not be listed, so we explicitly add them
    if (addAncestors) {
      S3Guard.addMoveAncestors(metadataStore,
          srcPaths,
          dstMetas,
          getSourceRoot(),
          sourceStatus.getPath(),
          destPath,
          getOwner());
    }
  }

  @Override
  public void noteSourceDirectoryMoved() throws IOException {
    if (!srcPaths.contains(getSourceRoot())) {
      S3Guard.addMoveDir(metadataStore, srcPaths, dstMetas,
          getSourceRoot(),
          getDest(), getOwner());
    }
  }

  @Override
  public void sourceObjectsDeleted(
      final List<DeleteObjectsRequest.KeyVersion> keys) throws IOException {
    // convert to paths.
    deletedKeys.addAll(keys);
  }

  @Override
  public void complete() throws IOException {
    metadataStore.move(srcPaths, dstMetas);
  }

  @Override
  public IOException renameFailed(final Exception ex) throws IOException {
    super.renameFailed(ex);
    return null;
  }
}
