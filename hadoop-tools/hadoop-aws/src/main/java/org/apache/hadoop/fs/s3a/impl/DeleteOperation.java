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
import java.util.Optional;
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
import org.apache.hadoop.util.DurationInfo;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.submit;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.waitForCompletion;

/**
 * Implementation of the delete operation.
 * For an authoritative S3Guarded store, after the list and delete of the
 * combined store, we repeat against raw S3.
 * This will correct for any situation where the authoritative listing is
 * incomplete.
 */
public class DeleteOperation extends AbstractStoreOperation {

  private static final Logger LOG = LoggerFactory.getLogger(
      DeleteOperation.class);

  /**
   * This is a switch to turn on when trying to debug
   * deletion problems; it requests the results of
   * the delete call from AWS then audits them.
   */
  private static final boolean AUDIT_DELETED_KEYS = true;

  /**
   * Used to stop any re-entrancy of the rename.
   * This is an execute-once operation.
   */
  private final AtomicBoolean executed = new AtomicBoolean(false);

  private final S3AFileStatus status;

  private final boolean recursive;

  private final OperationCallbacks callbacks;

  private final int pageSize;

  private final MetadataStore metadataStore;

  private final ListeningExecutorService executor;

  private List<DeleteObjectsRequest.KeyVersion> keys;

  private List<Path> paths;

  private CompletableFuture<Void> deleteFuture;

  private long filesDeleted;
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
      final OperationCallbacks callbacks, int pageSize) {

    super(context);
    this.status = status;
    this.recursive = recursive;
    this.callbacks = callbacks;
    checkArgument(pageSize > 0
        && pageSize <=InternalConstants.MAX_ENTRIES_TO_DELETE,
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
   * <p>
   * Note that DDB is not used for listing objects here, even if the
   * store is marked as auth: that actually means that newly created files
   * may not get found for the delete.
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
   *  Directory delete: combine paginated list of files with single or
   *  multiple object delete calls.
   *
   * @param path directory path
   * @param dirKey directory key
   * @throws IOException failure
   */
  protected void deleteDirectory(final Path path,
      final String dirKey) throws IOException {
    // create an operation state so that the store can manage the bulk
    // operation if it needs to
    try (BulkOperationState operationState =
             S3Guard.initiateBulkWrite(
                 metadataStore,
                 BulkOperationState.OperationType.Delete,
                 path);
         DurationInfo ignored =
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
        queueForDeletion(operationState, child);
      }
      LOG.debug("Deleting final batch of listed files");
      deleteNextBatch(operationState);
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
          queueForDeletion(operationState,
              deletionKey(objects.next()),
              null);
        }
        if (extraFilesDeleted > 0) {
          LOG.debug("Raw S3 Scan found {} file(s) to delete",
              extraFilesDeleted);
          // there is no more data:
          // await any ongoing operation
          deleteNextBatch(operationState);
          maybeAwaitCompletion(deleteFuture);
        }
      }

      // final cleanup of the directory tree in the metastore, including the
      // directory entry itself.

      try (DurationInfo ignored2 =
               new DurationInfo(LOG, false, "Delete metastore")) {
        metadataStore.deleteSubtree(path, operationState);
      }
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
   * @param operationState operation
   * @param stat status to queue
   * @throws IOException failure of the previous batch of deletions.
   */
  private void queueForDeletion(
      final BulkOperationState operationState,
      final S3AFileStatus stat) throws IOException {
    queueForDeletion(operationState, deletionKey(stat), stat.getPath());
  }

  /**
   * Queue keys for deletion; once a page of keys are ready to delete this
   * call is submitted to the executor, after waiting for the previous run to
   * complete.
   *
   * @param operationState operation
   * @param key key to delete
   * @param deletePath nullable path of the key
   * @throws IOException failure of the previous batch of deletions.
   */
  protected void queueForDeletion(final BulkOperationState operationState,
      final String key,
      @Nullable final Path deletePath) throws IOException {
    LOG.debug("Got object to delete: \"{}\"", key);
    keys.add(new DeleteObjectsRequest.KeyVersion(key));
    if (deletePath != null) {
      paths.add(deletePath);
    }

    if (keys.size() == pageSize) {
      deleteNextBatch(operationState);
    }
  }

  /**
   * Wait for the previous batch to finish then submit this page.
   * The lists of keys and pages are reset here.
   * complete.
   *
   * @param operationState operation
   * @throws IOException failure of the previous batch of deletions.
   */
  protected void deleteNextBatch(final BulkOperationState operationState)
      throws IOException {
    // delete a single page of keys and the metadata.
    // block for any previous batch.
    maybeAwaitCompletion(deleteFuture);

    // delete the current page of keys and paths
    deleteFuture = submitDelete(keys, paths, operationState);
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
      final String key, boolean isFile)
      throws IOException {
    LOG.debug("delete: {} {}", isFile? "file": "dir marker", key);
    filesDeleted++;
    callbacks.deleteObjectAtPath(path, key, isFile);
  }

  /**
   * Delete a single page of keys and optionally the metadata.
   * For a large page, it is the metadata size which dominates.
   * Its possible to invoke this with empty lists of keys or paths.
   * If both lists are empty no work is submitted.
   *
   * @param keyList keys to delete.
   * @param pathList paths to update the metastore with.
   * @param operationState ongoing operation state
   * @return the submitted future or null
   */
  protected CompletableFuture<Void> submitDelete(
      final List<DeleteObjectsRequest.KeyVersion> keyList,
      final List<Path> pathList,
      final BulkOperationState operationState) {

    if (keyList.isEmpty() && pathList.isEmpty())  {
      return null;
    }
    filesDeleted += keyList.size();
    return submit(executor, () -> {
      try (DurationInfo ignored =
               new DurationInfo(LOG, false, "Delete page of keys")) {
        Optional<DeleteObjectsResult> result = Optional.empty();
        List<Path> undeletedObjects = new ArrayList<>();
        if (!keyList.isEmpty()) {
          result = callbacks.removeKeys(keyList, false, undeletedObjects,
          operationState,
          !AUDIT_DELETED_KEYS);
        }
        if (!pathList.isEmpty()) {
          metadataStore.deletePaths(pathList, operationState);
        }
        if (AUDIT_DELETED_KEYS && result.isPresent()) {
          // audit the deleted keys
          List<DeleteObjectsResult.DeletedObject> deletedObjects = result.get()
              .getDeletedObjects();
          if (deletedObjects.size() != keyList.size()) {
            // size mismatch
            LOG.warn("Size mismatch in deletion operation. "
                + "Expected count of deleted files: {}; "
                + "actual: {}",
                keyList.size(), deletedObjects.size());
            // strip out the deleted keys
            for (DeleteObjectsResult.DeletedObject del : deletedObjects) {
              keyList.removeIf(kv ->kv.getKey() .equals(del.getKey()));
            }
            for (DeleteObjectsRequest.KeyVersion kv : keyList) {
              LOG.info("{}", kv.getKey());
            }
          }
        }
      }
      return null;
    });
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
               new DurationInfo(LOG, false, "delete completion")){
        waitForCompletion(future);
      }
    }
  }

}
