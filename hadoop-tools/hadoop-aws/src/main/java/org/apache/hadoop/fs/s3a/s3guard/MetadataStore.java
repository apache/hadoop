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

package org.apache.hadoop.fs.s3a.s3guard;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.Retries.RetryTranslated;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.impl.StoreContext;

/**
 * {@code MetadataStore} defines the set of operations that any metadata store
 * implementation must provide.  Note that all {@link Path} objects provided
 * to methods must be absolute, not relative paths.
 * Implementations must implement any retries needed internally, such that
 * transient errors are generally recovered from without throwing exceptions
 * from this API.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface MetadataStore extends Closeable {

  /**
   * Performs one-time initialization of the metadata store.
   *
   * @param fs {@code FileSystem} associated with the MetadataStore
   * @param ttlTimeProvider the time provider to use for metadata expiry
   * @throws IOException if there is an error
   */
  void initialize(FileSystem fs, ITtlTimeProvider ttlTimeProvider)
      throws IOException;

  /**
   * Performs one-time initialization of the metadata store via configuration.
   * @see #initialize(FileSystem, ITtlTimeProvider)
   * @param conf Configuration.
   * @param ttlTimeProvider the time provider to use for metadata expiry
   * @throws IOException if there is an error
   */
  void initialize(Configuration conf,
      ITtlTimeProvider ttlTimeProvider) throws IOException;

  /**
   * Deletes exactly one path, leaving a tombstone to prevent lingering,
   * inconsistent copies of it from being listed.
   *
   * Deleting an entry with a tombstone needs a
   * {@link org.apache.hadoop.fs.s3a.s3guard.S3Guard.TtlTimeProvider} because
   * the lastUpdated field of the record has to be updated to <pre>now</pre>.
   *
   * @param path the path to delete
   * @param operationState (nullable) operational state for a bulk update
   * @throws IOException if there is an error
   */
  void delete(Path path,
      @Nullable BulkOperationState operationState)
      throws IOException;

  /**
   * Removes the record of exactly one path.  Does not leave a tombstone (see
   * {@link MetadataStore#delete(Path, BulkOperationState)}. It is currently
   * intended for testing only, and a need to use it as part of normal
   * FileSystem usage is not anticipated.
   *
   * @param path the path to delete
   * @throws IOException if there is an error
   */
  @VisibleForTesting
  void forgetMetadata(Path path) throws IOException;

  /**
   * Deletes the entire sub-tree rooted at the given path, leaving tombstones
   * to prevent lingering, inconsistent copies of it from being listed.
   *
   * In addition to affecting future calls to {@link #get(Path)},
   * implementations must also update any stored {@code DirListingMetadata}
   * objects which track the parent of this file.
   *
   * Deleting a subtree with a tombstone needs a
   * {@link org.apache.hadoop.fs.s3a.s3guard.S3Guard.TtlTimeProvider} because
   * the lastUpdated field of all records have to be updated to <pre>now</pre>.
   *
   * @param path the root of the sub-tree to delete
   * @param operationState (nullable) operational state for a bulk update
   * @throws IOException if there is an error
   */
  @Retries.RetryTranslated
  void deleteSubtree(Path path,
      @Nullable BulkOperationState operationState)
      throws IOException;

  /**
   * Delete the paths.
   * There's no attempt to order the paths: they are
   * deleted in the order passed in.
   * @param paths paths to delete.
   * @param operationState Nullable operation state
   * @throws IOException failure
   */

  @RetryTranslated
  void deletePaths(Collection<Path> paths,
      @Nullable BulkOperationState operationState)
      throws IOException;

  /**
   * Gets metadata for a path.
   *
   * @param path the path to get
   * @return metadata for {@code path}, {@code null} if not found
   * @throws IOException if there is an error
   */
  PathMetadata get(Path path) throws IOException;

  /**
   * Gets metadata for a path.  Alternate method that includes a hint
   * whether or not the MetadataStore should do work to compute the value for
   * {@link PathMetadata#isEmptyDirectory()}.  Since determining emptiness
   * may be an expensive operation, this can save wasted work.
   *
   * @param path the path to get
   * @param wantEmptyDirectoryFlag Set to true to give a hint to the
   *   MetadataStore that it should try to compute the empty directory flag.
   * @return metadata for {@code path}, {@code null} if not found
   * @throws IOException if there is an error
   */
  PathMetadata get(Path path, boolean wantEmptyDirectoryFlag)
      throws IOException;

  /**
   * Lists metadata for all direct children of a path.
   *
   * @param path the path to list
   * @return metadata for all direct children of {@code path} which are being
   *     tracked by the MetadataStore, or {@code null} if the path was not found
   *     in the MetadataStore.
   * @throws IOException if there is an error
   */
  @Retries.RetryTranslated
  DirListingMetadata listChildren(Path path) throws IOException;

  /**
   * This adds all new ancestors of a path as directories.
   * <p>
   * Important: to propagate TTL information, any new ancestors added
   * must have their last updated timestamps set through
   * {@link S3Guard#patchLastUpdated(Collection, ITtlTimeProvider)}.
   * @param qualifiedPath path to update
   * @param operationState (nullable) operational state for a bulk update
   * @throws IOException failure
   */
  @RetryTranslated
  void addAncestors(Path qualifiedPath,
      @Nullable BulkOperationState operationState) throws IOException;

  /**
   * Record the effects of a {@link FileSystem#rename(Path, Path)} in the
   * MetadataStore.  Clients provide explicit enumeration of the affected
   * paths (recursively), before and after the rename.
   *
   * This operation is not atomic, unless specific implementations claim
   * otherwise.
   *
   * On the need to provide an enumeration of directory trees instead of just
   * source and destination paths:
   * Since a MetadataStore does not have to track all metadata for the
   * underlying storage system, and a new MetadataStore may be created on an
   * existing underlying filesystem, this move() may be the first time the
   * MetadataStore sees the affected paths.  Therefore, simply providing src
   * and destination paths may not be enough to record the deletions (under
   * src path) and creations (at destination) that are happening during the
   * rename().
   *
   * @param pathsToDelete Collection of all paths that were removed from the
   *                      source directory tree of the move.
   * @param pathsToCreate Collection of all PathMetadata for the new paths
   *                      that were created at the destination of the rename().
   * @param operationState     Any ongoing state supplied to the rename tracker
   *                      which is to be passed in with each move operation.
   * @throws IOException if there is an error
   */
  void move(@Nullable Collection<Path> pathsToDelete,
      @Nullable Collection<PathMetadata> pathsToCreate,
      @Nullable BulkOperationState operationState) throws IOException;

  /**
   * Saves metadata for exactly one path.
   *
   * Implementations may pre-create all the path's ancestors automatically.
   * Implementations must update any {@code DirListingMetadata} objects which
   * track the immediate parent of this file.
   *
   * @param meta the metadata to save
   * @throws IOException if there is an error
   */
  @RetryTranslated
  void put(PathMetadata meta) throws IOException;

  /**
   * Saves metadata for exactly one path, potentially
   * using any bulk operation state to eliminate duplicate work.
   *
   * Implementations may pre-create all the path's ancestors automatically.
   * Implementations must update any {@code DirListingMetadata} objects which
   * track the immediate parent of this file.
   *
   * @param meta the metadata to save
   * @param operationState operational state for a bulk update
   * @throws IOException if there is an error
   */
  @RetryTranslated
  void put(PathMetadata meta,
      @Nullable BulkOperationState operationState) throws IOException;

  /**
   * Saves metadata for any number of paths.
   *
   * Semantics are otherwise the same as single-path puts.
   *
   * @param metas the metadata to save
   * @param operationState (nullable) operational state for a bulk update
   * @throws IOException if there is an error
   */
  void put(Collection<? extends PathMetadata> metas,
      @Nullable BulkOperationState operationState) throws IOException;

  /**
   * Save directory listing metadata. Callers may save a partial directory
   * listing for a given path, or may store a complete and authoritative copy
   * of the directory listing.  {@code MetadataStore} implementations may
   * subsequently keep track of all modifications to the directory contents at
   * this path, and return authoritative results from subsequent calls to
   * {@link #listChildren(Path)}. See {@link DirListingMetadata}.
   *
   * Any authoritative results returned are only authoritative for the scope
   * of the {@code MetadataStore}:  A per-process {@code MetadataStore}, for
   * example, would only show results visible to that process, potentially
   * missing metadata updates (create, delete) made to the same path by
   * another process.
   *
   * To optimize updates and avoid overwriting existing entries which
   * may contain extra data, entries in the list of unchangedEntries may
   * be excluded. That is: the listing metadata has the full list of
   * what it believes are children, but implementations can opt to ignore
   * some.
   * @param meta Directory listing metadata.
   * @param unchangedEntries list of entries in the dir listing which have
   * not changed since the directory was list scanned on s3guard.
   * @param operationState operational state for a bulk update
   * @throws IOException if there is an error
   */
  void put(DirListingMetadata meta,
      final List<Path> unchangedEntries,
      @Nullable BulkOperationState operationState) throws IOException;

  /**
   * Destroy all resources associated with the metadata store.
   *
   * The destroyed resources can be DynamoDB tables, MySQL databases/tables, or
   * HDFS directories. Any operations after calling this method may possibly
   * fail.
   *
   * This operation is idempotent.
   *
   * @throws IOException if there is an error
   */
  void destroy() throws IOException;

  /**
   * Prune method with two modes of operation:
   * <ul>
   *   <li>
   *    {@link PruneMode#ALL_BY_MODTIME}
   *    Clear any metadata older than a specified mod_time from the store.
   *    Note that this modification time is the S3 modification time from the
   *    object's metadata - from the object store.
   *    Implementations MUST clear file metadata, and MAY clear directory
   *    metadata (s3a itself does not track modification time for directories).
   *    Implementations may also choose to throw UnsupportedOperationException
   *    instead. Note that modification times must be in UTC, as returned by
   *    System.currentTimeMillis at the time of modification.
   *   </li>
   * </ul>
   *
   * <ul>
   *   <li>
   *    {@link PruneMode#TOMBSTONES_BY_LASTUPDATED}
   *    Clear any tombstone updated earlier than a specified time from the
   *    store. Note that this last_updated is the time when the metadata
   *    entry was last updated and maintained by the metadata store.
   *    Implementations MUST clear file metadata, and MAY clear directory
   *    metadata (s3a itself does not track modification time for directories).
   *    Implementations may also choose to throw UnsupportedOperationException
   *    instead. Note that last_updated must be in UTC, as returned by
   *    System.currentTimeMillis at the time of modification.
   *   </li>
   * </ul>
   *
   * @param pruneMode Prune Mode
   * @param cutoff Oldest time to allow (UTC)
   * @throws IOException if there is an error
   * @throws UnsupportedOperationException if not implemented
   */
  void prune(PruneMode pruneMode, long cutoff) throws IOException,
      UnsupportedOperationException;

  /**
   * Same as {@link MetadataStore#prune(PruneMode, long)}, but with an
   * additional keyPrefix parameter to filter the pruned keys with a prefix.
   *
   * @param pruneMode Prune Mode
   * @param cutoff Oldest time in milliseconds to allow (UTC)
   * @param keyPrefix The prefix for the keys that should be removed
   * @throws IOException if there is an error
   * @throws UnsupportedOperationException if not implemented
   * @return the number of pruned entries
   */
  long prune(PruneMode pruneMode, long cutoff, String keyPrefix)
      throws IOException, UnsupportedOperationException;

  /**
   * Get any diagnostics information from a store, as a list of (key, value)
   * tuples for display. Arbitrary values; no guarantee of stability.
   * These are for debugging and testing only.
   * @return a map of strings.
   * @throws IOException if there is an error
   */
  Map<String, String> getDiagnostics() throws IOException;

  /**
   * Tune/update parameters for an existing table.
   * @param parameters map of params to change.
   * @throws IOException if there is an error
   */
  void updateParameters(Map<String, String> parameters) throws IOException;

  /**
   * Mark all directories created/touched in an operation as authoritative.
   * The metastore can now update that path with any authoritative
   * flags it chooses.
   * The store may assume that therefore the operation state is complete.
   * This holds for rename and needs to be documented for import.
   * @param dest destination path.
   * @param operationState active state.
   * @throws IOException failure.
   * @return the number of directories marked.
   */
  default int markAsAuthoritative(Path dest,
      BulkOperationState operationState)
      throws IOException {
    return 0;
  }

  /**
   * Modes of operation for prune.
   * For details see {@link MetadataStore#prune(PruneMode, long)}
   */
  enum PruneMode {
    ALL_BY_MODTIME,
    TOMBSTONES_BY_LASTUPDATED
  }

  /**
   * Start a rename operation.
   *
   * @param storeContext store context.
   * @param source source path
   * @param sourceStatus status of the source file/dir
   * @param dest destination path.
   * @return the rename tracker
   * @throws IOException Failure.
   */
  RenameTracker initiateRenameOperation(
      StoreContext storeContext,
      Path source,
      S3AFileStatus sourceStatus,
      Path dest)
      throws IOException;

  /**
   * Initiate a bulk update and create an operation state for it.
   * This may then be passed into put operations.
   * @param operation the type of the operation.
   * @param dest path under which updates will be explicitly put.
   * @return null or a store-specific state to pass into the put operations.
   * @throws IOException failure
   */
  default BulkOperationState initiateBulkWrite(
      BulkOperationState.OperationType operation,
      Path dest) throws IOException {
    return new BulkOperationState(operation);
  }

  /**
   * The TtlTimeProvider has to be set during the initialization for the
   * metadatastore, but this method can be used for testing, and change the
   * instance during runtime.
   *
   * @param ttlTimeProvider
   */
  void setTtlTimeProvider(ITtlTimeProvider ttlTimeProvider);

  /**
   * Get any S3GuardInstrumentation for this store...must not be null.
   * @return any store instrumentation.
   */
  default MetastoreInstrumentation getInstrumentation() {
    return new MetastoreInstrumentationImpl();
  }
}
