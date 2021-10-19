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

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
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
class ImportOperation extends ExecutingStoreOperation<Long> {

  private static final Logger LOG = LoggerFactory.getLogger(
      ImportOperation.class);

  /**
   * Source file system: must not be guarded.
   */
  private final S3AFileSystem filesystem;

  /**
   * Destination metadata store.
   */
  private final MetadataStore store;

  /**
   * Source entry: File or directory.
   */
  private final S3AFileStatus status;

  /**
   * If importing the directory tree -should it be marked
   * authoritative afterwards?
   */
  private final boolean authoritative;

  private final boolean verbose;

  /**
   * For DDB the BulkOperation tracking eliminates the need for this cache,
   * but it is retained here for the local store and to allow for
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
   * @param verbose Verbose output
   */
  ImportOperation(final S3AFileSystem filesystem,
      final MetadataStore store,
      final S3AFileStatus status,
      final boolean authoritative,
      final boolean verbose) {
    super(filesystem.createStoreContext());
    this.verbose = verbose;
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
               new DurationInfo(LOG, "Importing %s", getStatus().getPath())) {
        items = importDir();
      }
    }
    return items;
  }

  /**
   * Recursively import every path under path.
   * @return number of items inserted into MetadataStore
   * @throws IOException on I/O errors.
   */
  private long importDir() throws IOException {
    Preconditions.checkArgument(status.isDirectory());
    long totalCountOfEntriesWritten = 0;
    final Path basePath = status.getPath();
    final MetadataStore ms = getStore();
    LOG.info("Importing directory {}", basePath);
    try (BulkOperationState operationState = ms
        .initiateBulkWrite(
            BulkOperationState.OperationType.Import,
            basePath)) {
      long countOfFilesWritten = 0;
      long countOfDirsWritten = 0;
      RemoteIterator<S3ALocatedFileStatus> it = getFilesystem()
          .listFilesAndEmptyDirectoriesForceNonAuth(basePath, true);
      while (it.hasNext()) {
        S3ALocatedFileStatus located = it.next();
        S3AFileStatus child;
        final Path path = located.getPath();
        final boolean isDirectory = located.isDirectory();
        if (isDirectory) {
          child = DynamoDBMetadataStore.makeDirStatus(path,
              located.getOwner());
          dirCache.add(path);
          // and update the dir count
          countOfDirsWritten++;
        } else {
          child = located.toS3AFileStatus();
        }

        int parentsWritten = putParentsIfNotPresent(child, operationState);
        LOG.debug("Wrote {} parent entries", parentsWritten);

        // We don't blindly overwrite any existing file entry in S3Guard with a
        // new one, Because that may lose the version information.
        // instead we merge them
        if (!isDirectory) {
          final PathMetadata existingEntry = S3Guard.getWithTtl(ms, path, null,
              false, true);
          if (existingEntry != null) {
            final S3AFileStatus existingStatus = existingEntry.getFileStatus();
            if (existingStatus.isFile()) {
              // source is also a file.
              // we only worry about an update if the timestamp is different,
              final String existingEtag = existingStatus.getETag();
              final String childEtag = child.getETag();
              if (child.getModificationTime()
                  != existingStatus.getModificationTime()
                  || existingStatus.getLen() != child.getLen()
                  || existingEtag == null
                  || !existingEtag.equals(childEtag)) {
                // files are potentially different, though a modtime change
                // can just be a clock skew problem
                // so if the etag is unchanged, we propagate any versionID
                if (childEtag.equals(existingEtag)) {
                  // copy over any version ID.
                  child.setVersionId(existingStatus.getVersionId());
                }
              } else {
                // the entry modtimes match
                child = null;
              }
            }
          }
          if (child != null) {
            countOfFilesWritten++;
          }
        }
        if (child != null) {
          // there's an entry to add.

          // log entry spaced to same width
          String t = isDirectory ? "Dir " : "File";
          if (verbose) {
            LOG.info("{} {}", t, path);
          } else {
            LOG.debug("{} {}", t, path);
          }
          S3Guard.putWithTtl(
              ms,
              new PathMetadata(child),
              getFilesystem().getTtlTimeProvider(),
              operationState);
          totalCountOfEntriesWritten++;
        }
      }
      LOG.info("Updated S3Guard with {} files and {} directory entries",
          countOfFilesWritten, countOfDirsWritten);

      // here all entries are imported.
      // tell the store that everything should be marked as auth
      if (authoritative) {
        LOG.info("Marking directory tree {} as authoritative",
            basePath);
        ms.markAsAuthoritative(basePath, operationState);
      }
    }
    return totalCountOfEntriesWritten;
  }

  /**
   * Put parents into metastore and cache if the parents are not present.
   *
   * There's duplication here with S3Guard DDB ancestor state, but this
   * is designed to work across implementations.
   * @param fileStatus the file or an empty directory.
   * @param operationState store's bulk update state.
   * @return number of entries written.
   * @throws IOException on I/O errors.
   */
  private int putParentsIfNotPresent(FileStatus fileStatus,
      @Nullable BulkOperationState operationState) throws IOException {
    Preconditions.checkNotNull(fileStatus);
    Path parent = fileStatus.getPath().getParent();
    int count = 0;
    while (parent != null) {
      if (dirCache.contains(parent)) {
        return count;
      }
      final ITtlTimeProvider timeProvider
          = getFilesystem().getTtlTimeProvider();
      final PathMetadata pmd = S3Guard.getWithTtl(getStore(), parent,
          timeProvider, false, true);
      if (pmd == null || pmd.isDeleted()) {
        S3AFileStatus dir = DynamoDBMetadataStore.makeDirStatus(parent,
            fileStatus.getOwner());
        S3Guard.putWithTtl(getStore(), new PathMetadata(dir),
            timeProvider,
            operationState);
        count++;
      }
      dirCache.add(parent);
      parent = parent.getParent();
    }
    return count;
  }

}
