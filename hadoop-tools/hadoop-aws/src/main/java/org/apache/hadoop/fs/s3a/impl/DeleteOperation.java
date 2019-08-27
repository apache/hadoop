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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
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
import org.apache.hadoop.fs.s3a.S3ListRequest;
import org.apache.hadoop.fs.s3a.S3ListResult;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.fs.s3a.s3guard.BulkOperationState;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.S3Guard;
import org.apache.hadoop.util.DurationInfo;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.submit;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.waitForCompletion;
import static org.apache.hadoop.fs.s3a.s3guard.S3Guard.isNullMetadataStore;

/**
 * Implementation of the delete operation.
 * For a S3Guarded store, after the list and delete of the combined store,
 * we repeat against raw S3.
 * This will correct for any situation where there are tombstones against
 * files which exist or the authoritative listing is incomplete.
 */
public class DeleteOperation extends AbstractStoreOperation {

  private static final Logger LOG = LoggerFactory.getLogger(
      DeleteOperation.class);

  /**
   * Used to stop any re-entrancy of the rename.
   * This is an execute-once operation.
   */
  private final AtomicBoolean executed = new AtomicBoolean(false);

  private final S3AFileStatus status;

  private final boolean recursive;

  private final RenameOperation.RenameOperationCallbacks callbacks;

  private final int pageSize;

  private final MetadataStore metadataStore;

  private final ListeningExecutorService executor;

  private List<DeleteObjectsRequest.KeyVersion> keys;

  private List<Path> paths;

  private CompletableFuture<Void> deleteFuture;

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
      final RenameOperation.RenameOperationCallbacks callbacks, int pageSize) {

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

  /**
   * Delete an object.
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
   * @throws IOException due to inability to delete a directory or file.
   * @throws AmazonClientException on failures inside the AWS SDK
   */
  @Retries.RetryMixed
  public boolean execute() throws IOException, AmazonClientException {
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
    StoreContext storeContext = getStoreContext();
    // create an operation state so that the store can manage the bulk
    //   *      operation if it needs to
    try (BulkOperationState operationState =
             S3Guard.initiateBulkWrite(
                 metadataStore,
                 BulkOperationState.OperationType.Delete,
                 path);
         DurationInfo ignored =
             new DurationInfo(LOG, false, "Delete Tree")) {
      LOG.debug("Getting objects for directory prefix {} to delete", dirKey);


      resetDeleteList();
      deleteFuture = null;
      final RemoteIterator<S3ALocatedFileStatus> iterator =
          callbacks.listFilesAndEmptyDirectories(path);
      while (iterator.hasNext()) {
        // get the next entry in the listing.
        S3ALocatedFileStatus child = iterator.next();
        // convert it to an S3 key.
        String k = storeContext.pathToKey(child.getPath());
        // possibly adding a "/" if it represents directory and it does
        // not have a trailing slash already.
        String key = (child.isDirectory() && !k.endsWith("/"))
            ? k + "/"
            : k;
        // the source object to copy as a path.
        queueForDeletion(operationState, key, child.getPath());
      }
      // if s3guard is on we follow up with a bulk list and delete process
      // on S3 this helps recover from any situation where S3 and S3Guard have become inconsistent.

      S3ListResult objects = null;
      if (!isNullMetadataStore(getStoreContext().getMetadataStore())) {
        LOG.debug("Path is guarded; listing files on S3 for completeness");
        int extraFilesDeleted = 0;
        S3ListRequest request = callbacks.createListObjectsRequest(dirKey,
            null);
        objects = callbacks.listObjects(request);
        while (objects != null) {
          for (S3ObjectSummary summary : objects.getObjectSummaries()) {
            // we only care about S3 here
            extraFilesDeleted++;
            queueForDeletion(operationState, summary.getKey(), null);
          }

          if (objects.isTruncated()) {
            // continue the listing.
            // This will probe S3 and may be slow; any ongoing delete will overlap
            objects = callbacks.continueListObjects(request, objects);
          } else {
            objects = null;
          }
        }
        if (extraFilesDeleted > 0) {
          LOG.info("Raw S3 Scan found {} extra file(s) to delete",
              extraFilesDeleted);
        }
      }
      // there is no more data:
      // await any ongoing operation
      deleteNextBatch(operationState);
      maybeAwaitCompletion(deleteFuture);
      try (DurationInfo ignored2 =
               new DurationInfo(LOG, false, "Delete metastore")) {
        metadataStore.deleteSubtree(path, operationState);
      }
    }
  }

  /**
   * Queue keys for deletion; once a page of keys are ready to delete this
   * call is submitted to the executor, after waiting for the previous run to
   * complete.
   *
   * @param operationState operation
   * @param key key to delete
   * @param deletePath nullable path of the key
   * @throws IOException failure.
   */
  protected void queueForDeletion(final BulkOperationState operationState,
      final String key,
      @Nullable final Path deletePath) throws IOException {
    LOG.debug("Got object to delete {}", key);
    keys.add(new DeleteObjectsRequest.KeyVersion(key));
    if (deletePath!= null) {
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
   * @throws IOException failure.
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
  @Retries.RetryMixed
  protected void deleteObjectAtPath(final Path path,
      final String key, boolean isFile)
      throws IOException {
    LOG.debug("delete: {} {}", isFile? "file": "dir marker", key);
    callbacks.deleteObjectAtPath(path, key, isFile);
  }

  /**
   * Delete a single page of keys and optionally the metadata.
   * For a large page, it is the metadata size which dominates.
   * Its possible to invoke this with empty lists of keys or paths.
   * If both lists are empty no work is submitted.
   *
   * @param keys keys to delete.
   * @param paths paths to update the metastore with.
   * @param operationState ongoing operation state
   * @return the submitted future or null
   */
  protected CompletableFuture<Void> submitDelete(
      final List<DeleteObjectsRequest.KeyVersion> keys,
      final List<Path> paths, final BulkOperationState operationState) {

    if (keys.isEmpty() && paths.isEmpty())  {
      return null;
    }
    return submit(executor, () -> {
      try (DurationInfo ignored =
               new DurationInfo(LOG, false, "Delete page of keys")) {
        List<Path> undeletedObjects = new ArrayList<>();
        if (!keys.isEmpty()) {
          callbacks.removeKeys(keys, false, undeletedObjects, operationState);
        }
        if (!paths.isEmpty()) {
          metadataStore.deletePaths(paths, operationState);
        }
      }
      return null;
    });
  }

  /**
   * Block awaiting completion for any non-null future passed in;
   * No-op if a null arg was supplied.
   * @param future future
   * @return null, always
   * @throws IOException any exception raised in the callable
   */
  protected CompletableFuture<Void> maybeAwaitCompletion(
      @Nullable final CompletableFuture<Void> future)
      throws IOException {
    if (future != null) {
      try (DurationInfo ignored =
               new DurationInfo(LOG, false, "delete completion")){
        waitForCompletion(future);
      }
    }
    return null;
  }

}
