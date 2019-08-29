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

package org.apache.hadoop.fs.s3a.impl;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.fs.s3a.s3guard.BulkOperationState;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.S3Guard;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DurationInfo;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.submit;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.waitForCompletion;

/**
 * Implementation of the delete operation.
 * <p>
 * How S3Guard/Store inconsistency is handled:
 * <ol>
 *   <li>
 *     The list operation does not ask for tombstone markers; objects
 *     under tombstones will be found and deleted.
 *     The {@code extraFilesDeleted} counter will be incremented here.
 *   </li>
 *   <li>
 *     That may result in recently deleted files being found and
 *     duplicate delete requests issued. This is mostly harmless.
 *   </li>
 *   <li>
 *     If a path is considered authoritative on the client, so only S3Guard
 *     is used for listings, we wrap up the delete with a scan of raw S3.
 *     This will find and eliminate OOB additions.
 *   </li>
 *   <li>
 *     Exception 1: simple directory markers of the form PATH + "/".
 *     These are treated as a signal that there are no children; no
 *     listing is made.
 *   </li>
 *   <li>
 *     Exception 2: delete(path, true) where path has a tombstone in S3Guard.
 *     Here the delete is downgraded to a no-op even before this operation
 *     is created. Thus: no listings of S3.
 *   </li>
 * </ol>
 * If this class is logged at debug, requests will be audited:
 * the response to a bulk delete call will be reviewed to see if there
 * were fewer files deleted than requested; that will be printed
 * at WARN level. This is independent of handling rejected delete
 * requests which raise exceptions -those are processed lower down.
 * <p>
 * Performance tuning:
 * <p>
 * The operation to POST a delete request (or issue many individual
 * DELETE calls) then update the S3Guard table is done in an async
 * operation so that it can overlap with the LIST calls for data.
 * However, only one single operation is queued at a time.
 * Executing more than one batch delete is possible, it just
 * adds complexity in terms of error handling as well as in
 * the datastructures used to track outstanding operations.
 * If this is done, then it may be good to experiment with different
 * page sizes. The default value is
 * {@link InternalConstants#MAX_ENTRIES_TO_DELETE}, the maximum a single
 * POST permits.
 * <p>
 * 1. Smaller pages executed in parallel may have different
 * performance characteristics when deleting very large directories,
 * because it will be the DynamoDB calls which will come to dominate.
 * Any exploration of options here MUST be done with performance
 * measurements taken from test runs in EC2 against local DDB and S3 stores,
 * so as to ensure network latencies do not skew the results.
 * 2. Note that as the DDB thread/connection pools will be shared across
 * all active delete operations, speedups will be minimal unless
 * those pools are large enough to cope the extra load.
 * There are also some opportunities to explore in
 * {@code DynamoDBMetadataStore} with batching delete requests
 * in the DDB APIS.
 */
public class DeleteOperation extends AbstractStoreOperation {

  private static final Logger LOG = LoggerFactory.getLogger(
      DeleteOperation.class);

  /**
   * Used to stop any re-entrancy of the rename.
   * This is an execute-once operation.
   */
  private final AtomicBoolean executed = new AtomicBoolean(false);

  /**
   * pre-fetched source status.
   */
  private final S3AFileStatus status;

  /**
   * recursive delete?
   */
  private final boolean recursive;

  /**
   * callback provider.
   */
  private final OperationCallbacks callbacks;

  /**
   * number of entries in a page.
   */
  private final int pageSize;

  /**
   * metastore -never null but may be the NullMetadataStore.
   */
  private final MetadataStore metadataStore;

  /**
   * Executor for async operations.
   */
  private final ListeningExecutorService executor;

  /**
   * List of keys built up for the next delete batch.
   */
  private List<DeleteObjectsRequest.KeyVersion> keys;

  /**
   * List of paths built up for deletion.
   */
  private List<Path> paths;

  /**
   * The single async delete operation, or null.
   */
  private CompletableFuture<Void> deleteFuture;

  /**
   * Bulk Operation state if this is a bulk operation.
   */
  private BulkOperationState operationState;

  /**
   * Counter of deleted files.
   */
  private long filesDeleted;

  /**
   * Counter of files found in the S3 Store during a raw scan of the store
   * after the previous listing was in auth-mode.
   */
  private long extraFilesDeleted;

  /**
   * Constructor.
   * @param context store context
   * @param status  pre-fetched source status
   * @param recursive recursive delete?
   * @param callbacks callback provider
   * @param pageSize number of entries in a page
   */
  public DeleteOperation(final StoreContext context,
      final S3AFileStatus status,
      final boolean recursive,
      final OperationCallbacks callbacks,
      final int pageSize) {

    super(context);
    this.status = status;
    this.recursive = recursive;
    this.callbacks = callbacks;
    checkArgument(pageSize > 0
            && pageSize <= InternalConstants.MAX_ENTRIES_TO_DELETE,
        "page size out of range: %d", pageSize);
    this.pageSize = pageSize;
    metadataStore = context.getMetadataStore();
    executor = context.createThrottledExecutor(2);
  }

  public long getFilesDeleted() {
    return filesDeleted;
  }

  public long getExtraFilesDeleted() {
    return extraFilesDeleted;
  }

  /**
   * Delete a file or directory tree.
   * <p>
   * This call does not create any fake parent directory; that is
   * left to the caller.
   * The actual delete call is done in a separate thread.
   * Only one delete at a time is submitted, however, to reduce the
   * complexity of recovering from failures.
   * <p>
   * The DynamoDB store deletes paths in parallel itself, so that
   * potentially slow part of the process is somewhat speeded up.
   * The extra parallelization here is to list files from the store/DDB while
   * that delete operation is in progress.
   *
   * @return true, except in the corner cases of root directory deletion
   * @throws PathIsNotEmptyDirectoryException if the path is a dir and this
   * is not a recursive delete.
   * @throws IOException list failures or an inability to delete a file.
   */
  @Retries.RetryTranslated
  public boolean execute() throws IOException {
    Preconditions.checkState(
        !executed.getAndSet(true),
        "delete attempted twice");
    StoreContext context = getStoreContext();
    Path path = status.getPath();
    LOG.debug("Delete path {} - recursive {}", path, recursive);
    LOG.debug("Type = {}",
        status.isFile() ? "File"
            : (status.isEmptyDirectory() == Tristate.TRUE
                ? "Empty Directory"
                : "Directory"));

    String key = context.pathToKey(path);
    if (status.isDirectory()) {
      LOG.debug("delete: Path is a directory: {}", path);
      checkArgument(
          status.isEmptyDirectory() != Tristate.UNKNOWN,
          "File status must have directory emptiness computed");

      if (!key.endsWith("/")) {
        key = key + "/";
      }

      if ("/".equals(key)) {
        LOG.error("S3A: Cannot delete the root directory."
                + " Path: {}. Recursive: {}",
            status.getPath(), recursive);
        return false;
      }

      if (!recursive && status.isEmptyDirectory() == Tristate.FALSE) {
        throw new PathIsNotEmptyDirectoryException(path.toString());
      }
      if (status.isEmptyDirectory() == Tristate.TRUE) {
        LOG.debug("deleting empty directory {}", path);
        deleteObjectAtPath(path, key, false);
      } else {
        deleteDirectory(path, key);
      }

    } else {
      // simple file.
      LOG.debug("deleting simple file {}", path);
      deleteObjectAtPath(path, key, true);
    }
    LOG.debug("Deleted {} files", filesDeleted);
    return true;
  }

  /**
   * Directory delete: combine paginated list of files with single or
   * multiple object delete calls.
   *
   * @param path directory path
   * @param dirKey directory key
   * @throws IOException failure
   */
  protected void deleteDirectory(final Path path,
      final String dirKey) throws IOException {
    // create an operation state so that the store can manage the bulk
    // operation if it needs to
    operationState = S3Guard.initiateBulkWrite(
        metadataStore,
        BulkOperationState.OperationType.Delete,
        path);
    try (DurationInfo ignored =
             new DurationInfo(LOG, false, "deleting %s", dirKey)) {

      // init the lists of keys and paths to delete
      resetDeleteList();
      deleteFuture = null;

      // list files including any under tombstones through S3Guard
      LOG.debug("Getting objects for directory prefix {} to delete", dirKey);
      final RemoteIterator<S3ALocatedFileStatus> locatedFiles =
          callbacks.listFilesAndEmptyDirectories(path, status, false, true);

      // iterate through and delete. The next() call will block when a new S3
      // page is required; this any active delete submitted to the executor
      // will run in parallel with this.
      while (locatedFiles.hasNext()) {
        // get the next entry in the listing.
        S3AFileStatus child = locatedFiles.next().toS3AFileStatus();
        queueForDeletion(child);
      }
      LOG.debug("Deleting final batch of listed files");
      deleteNextBatch();
      maybeAwaitCompletion(deleteFuture);

      // if s3guard is authoritative we follow up with a bulk list and
      // delete process on S3 this helps recover from any situation where S3
      // and S3Guard have become inconsistent.
      // This is only needed for auth paths; by performing the previous listing
      // without tombstone filtering, any files returned by the non-auth
      // S3 list which were hidden under tombstones will have been found
      // and deleted.

      if (callbacks.allowAuthoritative(path)) {
        LOG.debug("Path is authoritatively guarded;"
            + " listing files on S3 for completeness");
        // let the ongoing delete finish to avoid duplicates
        final RemoteIterator<S3AFileStatus> objects =
            callbacks.listObjects(path, dirKey);

        // iterate through and delete. The next() call will block when a new S3
        // page is required; this any active delete submitted to the executor
        // will run in parallel with this.
        while (objects.hasNext()) {
          // get the next entry in the listing.
          extraFilesDeleted++;
          queueForDeletion(deletionKey(objects.next()), null);
        }
        if (extraFilesDeleted > 0) {
          LOG.debug("Raw S3 Scan found {} file(s) to delete",
              extraFilesDeleted);
          // there is no more data:
          // await any ongoing operation
          deleteNextBatch();
          maybeAwaitCompletion(deleteFuture);
        }
      }

      // final cleanup of the directory tree in the metastore, including the
      // directory entry itself.

      try (DurationInfo ignored2 =
               new DurationInfo(LOG, false, "Delete metastore")) {
        metadataStore.deleteSubtree(path, operationState);
      }
    } finally {
      IOUtils.cleanupWithLogger(LOG, operationState);
    }
    LOG.debug("Delete \"{}\" completed; deleted {} objects", path,
        filesDeleted);
  }

  /**
   * Build an S3 key for a delete request.
   * possibly adding a "/" if it represents directory and it does
   * not have a trailing slash already.
   * @param stat status to build the key from
   * @return a key for a delete request
   */
  private String deletionKey(final S3AFileStatus stat) {
    return getStoreContext().fullKey(stat);
  }

  /**
   * Queue for deletion.
   * @param stat status to queue
   * @throws IOException failure of the previous batch of deletions.
   */
  private void queueForDeletion(
      final S3AFileStatus stat) throws IOException {
    queueForDeletion(deletionKey(stat), stat.getPath());
  }

  /**
   * Queue keys for deletion; once a page of keys are ready to delete this
   * call is submitted to the executor, after waiting for the previous run to
   * complete.
   *
   * @param key key to delete
   * @param deletePath nullable path of the key
   * @throws IOException failure of the previous batch of deletions.
   */
  protected void queueForDeletion(final String key,
      @Nullable final Path deletePath) throws IOException {
    LOG.debug("Got object to delete: \"{}\"", key);
    keys.add(new DeleteObjectsRequest.KeyVersion(key));
    if (deletePath != null) {
      paths.add(deletePath);
    }

    if (keys.size() == pageSize) {
      deleteNextBatch();
    }
  }

  /**
   * Wait for the previous batch to finish then submit this page.
   * The lists of keys and pages are reset here.
   * complete.
   *
   * @throws IOException failure of the previous batch of deletions.
   */
  protected void deleteNextBatch()
      throws IOException {
    // delete a single page of keys and the metadata.
    // block for any previous batch.
    maybeAwaitCompletion(deleteFuture);

    // delete the current page of keys and paths
    deleteFuture = submitDelete(keys, paths);
    // reset the references so a new list can be built up.
    resetDeleteList();
  }

  protected void resetDeleteList() {
    keys = new ArrayList<>(pageSize);
    paths = new ArrayList<>(pageSize);
  }

  /**
   * Delete file or dir marker.
   * @param path path
   * @param key key
   * @param isFile is this a file?
   * @throws IOException failure
   */
  @Retries.RetryTranslated
  protected void deleteObjectAtPath(final Path path,
      final String key,
      final boolean isFile)
      throws IOException {
    LOG.debug("delete: {} {}", isFile ? "file" : "dir marker", key);
    filesDeleted++;
    callbacks.deleteObjectAtPath(path, key, isFile, operationState);
  }

  /**
   * Delete a single page of keys and optionally the metadata.
   * For a large page, it is the metadata size which dominates.
   * Its possible to invoke this with empty lists of keys or paths.
   * If both lists are empty no work is submitted.
   *
   * @param keyList keys to delete.
   * @param pathList paths to update the metastore with.
   * @return the submitted future or null
   */
  protected CompletableFuture<Void> submitDelete(
      final List<DeleteObjectsRequest.KeyVersion> keyList,
      final List<Path> pathList) {

    if (keyList.isEmpty() && pathList.isEmpty()) {
      return null;
    }
    filesDeleted += keyList.size();
    return submit(executor, () -> {
      asyncDeleteAction(operationState,
          keyList,
          pathList,
          LOG.isDebugEnabled());
      return null;
    });
  }

  /**
   * The action called in the asynchronous thread to delete
   * the keys from S3 and paths from S3Guard.
   *
   * @param state ongoing operation state
   * @param keyList keys to delete.
   * @param pathList paths to update the metastore with.
   * @param auditDeletedKeys should the results be audited and undeleted
   * entries logged?
   * @throws IOException failure
   */
  private void asyncDeleteAction(
      final BulkOperationState state,
      final List<DeleteObjectsRequest.KeyVersion> keyList,
      final List<Path> pathList,
      final boolean auditDeletedKeys)
      throws IOException {
    try (DurationInfo ignored =
             new DurationInfo(LOG, false, "Delete page of keys")) {
      DeleteObjectsResult result = null;
      List<Path> undeletedObjects = new ArrayList<>();
      if (!keyList.isEmpty()) {
        result = callbacks.removeKeys(keyList, false, undeletedObjects,
            state, !auditDeletedKeys);
      }
      if (!pathList.isEmpty()) {
        metadataStore.deletePaths(pathList, state);
      }
      if (auditDeletedKeys && result != null) {
        // audit the deleted keys
        List<DeleteObjectsResult.DeletedObject> deletedObjects =
            result.getDeletedObjects();
        if (deletedObjects.size() != keyList.size()) {
          // size mismatch
          LOG.warn("Size mismatch in deletion operation. "
                  + "Expected count of deleted files: {}; "
                  + "actual: {}",
              keyList.size(), deletedObjects.size());
          // strip out the deleted keys
          for (DeleteObjectsResult.DeletedObject del : deletedObjects) {
            keyList.removeIf(kv -> kv.getKey().equals(del.getKey()));
          }
          for (DeleteObjectsRequest.KeyVersion kv : keyList) {
            LOG.debug("{}", kv.getKey());
          }
        }
      }
    }
  }

  /**
   * Block awaiting completion for any non-null future passed in;
   * No-op if a null arg was supplied.
   * @param future future
   * @throws IOException any exception raised in the callable
   */
  protected void maybeAwaitCompletion(
      @Nullable final CompletableFuture<Void> future)
      throws IOException {
    if (future != null) {
      try (DurationInfo ignored =
               new DurationInfo(LOG, false, "delete completion")) {
        waitForCompletion(future);
      }
    }
  }

}
