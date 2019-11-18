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

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;
import org.apache.hadoop.fs.s3a.impl.ExecutingStoreOperation;
import org.apache.hadoop.util.DurationInfo;

/**
 * Import a directory tree into the metastore.
 * This code was moved from S3GuardTool and enhanced to mark
 * the destination tree as authoritative.
 */
class Importer extends ExecutingStoreOperation<Long> {

  private static final Logger LOG = LoggerFactory.getLogger(
      Importer.class);

  private final S3AFileSystem filesystem;

  private final MetadataStore store;

  private final S3AFileStatus status;

  private final boolean authoritative;

  /**
   * For DDB the BulkOperation tracking eliminates the need for this cache,
   * but it is retained here for (a) the local store and (b) to allow for
   * ease of moving to operations which may update the store in parallel with
   * writing.
   */
  private final Set<Path> dirCache = new HashSet<>();

  /**
   * Import.
   * @param filesystem Unguarded FS to scan.
   * @param store store to update
   * @param status source status
   * @param authoritative should the imported tree be marked as authoritative
   */
  Importer(final S3AFileSystem filesystem,
      final MetadataStore store,
      final S3AFileStatus status,
      final boolean authoritative) {
    super(filesystem.createStoreContext());
    Preconditions.checkState(!filesystem.hasMetadataStore(),
        "Source filesystem for import has a metadata store");
    this.filesystem = filesystem;
    this.store = store;
    this.status = status;
    this.authoritative = authoritative;
  }

  private S3AFileSystem getFilesystem() {
    return filesystem;
  }

  private MetadataStore getStore() {
    return store;
  }

  private FileStatus getStatus() {
    return status;
  }

  @Override
  public Long execute() throws IOException {
    final long items;
    if (status.isFile()) {
      PathMetadata meta = new PathMetadata(status);
      getStore().put(meta, null);
      items = 1;
    } else {
      try (DurationInfo ignored =
               new DurationInfo(LOG, "audit %s", getStatus().getPath())) {
        items = importDir(getStatus());
      }
    }
    return items;
  }

  /**
   * Recursively import every path under path.
   * @return number of items inserted into MetadataStore
   * @throws IOException on I/O errors.
   */
  private long importDir(FileStatus status) throws IOException {
    Preconditions.checkArgument(status.isDirectory());
    long items;
    final Path basePath = status.getPath();
    try (BulkOperationState operationState = getStore()
        .initiateBulkWrite(
            BulkOperationState.OperationType.Import,
            basePath)) {
      RemoteIterator<S3ALocatedFileStatus> it = getFilesystem()
          .listFilesAndEmptyDirectories(basePath, true);
      items = 0;

      while (it.hasNext()) {
        S3ALocatedFileStatus located = it.next();
        S3AFileStatus child;
        if (located.isDirectory()) {
          child = DynamoDBMetadataStore.makeDirStatus(located.getPath(),
              located.getOwner());
          dirCache.add(child.getPath());
        } else {
          child = located.toS3AFileStatus();
        }
        putParentsIfNotPresent(child, operationState);
        S3Guard.putWithTtl(getStore(),
            new PathMetadata(child),
            getFilesystem().getTtlTimeProvider(),
            operationState);
        items++;
      }
      // here all entries are imported.
      // tell the store that everything should be marked as auth
      if (authoritative){
        LOG.info("Marking imported directory as authoritative");
        getStore().markAsAuthoritative(basePath, operationState);
      }
    }
    return items;
  }

  /**
   * Put parents into MS and cache if the parents are not presented.
   *
   * @param f the file or an empty directory.
   * @param operationState store's bulk update state.
   * @throws IOException on I/O errors.
   */
  private void putParentsIfNotPresent(FileStatus f,
      @Nullable BulkOperationState operationState) throws IOException {
    Preconditions.checkNotNull(f);
    Path parent = f.getPath().getParent();
    while (parent != null) {
      if (dirCache.contains(parent)) {
        return;
      }
      S3AFileStatus dir = DynamoDBMetadataStore.makeDirStatus(parent,
          f.getOwner());
      S3Guard.putWithTtl(getStore(), new PathMetadata(dir),
          getFilesystem().getTtlTimeProvider(),
          operationState);
      dirCache.add(parent);
      parent = parent.getParent();
    }
  }
}
