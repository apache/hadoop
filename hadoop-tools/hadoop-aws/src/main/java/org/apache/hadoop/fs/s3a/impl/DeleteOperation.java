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
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
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

/**
 * Implementation of the delete operation.
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

  private final DeleteOperationCallbacks callbacks;

  private final int pageSize;

  private final MetadataStore metadataStore;

  private final ListeningExecutorService executor;

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
      final DeleteOperationCallbacks callbacks, int pageSize) {

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
        deleteObjectAtPath(path, key, false);
      } else {
        // Directory delete: combine paginated list of files with single or
        // multiple object delete calls.
        // create an operation state so that the store can manage the bulk
        // operation if it needs to.
        try (BulkOperationState operationState =
                 S3Guard.initiateBulkWrite(
                     metadataStore,
                     BulkOperationState.OperationType.Delete,
                     path);
             DurationInfo ignored =
                 new DurationInfo(LOG, false, "Delete Tree")) {
          LOG.debug("Getting objects for directory prefix {} to delete", key);

          S3ListRequest request = callbacks.createListObjectsRequest(key, null);

          S3ListResult objects = callbacks.listObjects(request);
          List<S3ObjectSummary> summaries = objects.getObjectSummaries();
          List<DeleteObjectsRequest.KeyVersion> keys =
              new ArrayList<>(summaries.size());
          List<Path> paths = new ArrayList<>(summaries.size());
          CompletableFuture<Void> deleteFuture = null;
          while (true) {
            for (S3ObjectSummary summary : objects.getObjectSummaries()) {
              String k = summary.getKey();
              keys.add(new DeleteObjectsRequest.KeyVersion(k));
              paths.add(context.keyToPath(k));
              LOG.debug("Got object to delete {}", k);

              if (keys.size() == pageSize) {
                // delete a single page of keys and the metadata.
                // block for any previous batch.
                maybeAwaitCompletion(deleteFuture);

                // delete the current page of keys and paths
                deleteFuture = submitDelete(keys, paths, operationState);
                // reset the references so a new list can be built up.
                keys = new ArrayList<>(summaries.size());
                paths = new ArrayList<>(summaries.size());
              }
            }

            if (objects.isTruncated()) {
              // continue the listing.
              // This will probe S3 and may be slow; any ongoing delete will overlap
              objects = callbacks.continueListObjects(request, objects);
            } else {
              // there is no more data:
              // await any ongoing operation
              maybeAwaitCompletion(deleteFuture);

              // delete the final set of entries.
              maybeAwaitCompletion(submitDelete(keys, paths, operationState));


              // Do: break out of the while() loop
              break;
            }
          }
          // TODO: now enum and delete all remaining files in the store, as these
          // represent files the listing missed due to inconsistency issues.
          try (DurationInfo ignored2 =
                   new DurationInfo(LOG, false, "Delete metastore")) {
            metadataStore.deleteSubtree(path, operationState);
          }
        }
      }

    } else {
      deleteObjectAtPath(path, key, true);
    }
    return true;
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
   * Delete a single page of keys and the metadata.
   * For a large page, it is the metadata size which dominates.
   * @param keys keys to delete.
   * @param paths paths to update the metastore with
   * @param operationState ongoing operation state
   * @return the submitted future
   */
  protected CompletableFuture<Void> submitDelete(
      final List<DeleteObjectsRequest.KeyVersion> keys,
      final List<Path> paths, final BulkOperationState operationState) {

    if (keys.isEmpty()) {
      return null;
    }
    return submit(executor, () -> {
      try (DurationInfo ignored =
               new DurationInfo(LOG, false, "Delete page of keys")) {
        callbacks.removeKeys(keys, false, operationState);
        metadataStore.deletePaths(paths, operationState);
      }
      return null;
    });
  }

  /**
   * Block awaiting completion for any non-null future passed in;
   * No-op if a null arg was supplied.
   * @param deleteFuture future
   * @return null, always
   * @throws IOException any exception raised in the callable
   */
  protected CompletableFuture<Void> maybeAwaitCompletion(
      @Nullable final CompletableFuture<Void> deleteFuture)
      throws IOException {
    if (deleteFuture != null) {
      try (DurationInfo ignored =
               new DurationInfo(LOG, false, "delete completion")){
        waitForCompletion(deleteFuture);
      }
    }
    return null;
  }

  /**
   * These are all the callbacks which the delete operation needs,
   * derived from the appropriate S3AFileSystem methods.
   */
  public interface DeleteOperationCallbacks {

    /**
     * Create a {@code ListObjectsRequest} request against this bucket.
     * @param key key for request
     * @param delimiter any delimiter
     * @return the request
     */
    S3ListRequest createListObjectsRequest(String key,
        String delimiter);

    /**
     * Delete an object, also updating the metastore.
     * This call does <i>not</i> create any mock parent entries.
     * @param path path path to delete
     * @param key key of entry
     * @param isFile is the path a file (used for instrumentation only)
     * @throws AmazonClientException problems working with S3
     * @throws IOException IO failure in the metastore
     */
    @Retries.RetryMixed
    void deleteObjectAtPath(Path path, String key, boolean isFile)
        throws AmazonClientException, IOException;

    /**
     * Initiate a {@code listObjects} operation, incrementing metrics
     * in the process.
     *
     * @param request request to initiate
     * @return the results
     * @throws IOException if the retry invocation raises one (it shouldn't).
     */
    @Retries.RetryRaw
    S3ListResult listObjects(S3ListRequest request)
        throws IOException;

    /**
     * List the next set of objects.
     * @param request last list objects request to continue
     * @param prevResult last paged result to continue from
     * @return the next result object
     * @throws IOException none, just there for retryUntranslated.
     */
    @Retries.RetryRaw
    S3ListResult continueListObjects(S3ListRequest request,
        S3ListResult prevResult) throws IOException;

    /**
     * RemoveKeys from S3 with handling of {@code MultiObjectDeleteException}.
     *
     * @param keysToDelete collection of keys to delete on the s3-backend.
     *        if empty, no request is made of the object store.
     * @param deleteFakeDir indicates whether this is for deleting fake dirs
     * @param operationState (nullable) operational state for a bulk update
     * @throws InvalidRequestException if the request was rejected due to
     * a mistaken attempt to delete the root directory.
     * @throws MultiObjectDeleteException one or more of the keys could not
     * be deleted in a multiple object delete operation.
     * @throws AmazonClientException amazon-layer failure.
     * @throws IOException other IO Exception.
     */
    @Retries.RetryMixed
    void removeKeys(
        final List<DeleteObjectsRequest.KeyVersion> keysToDelete,
        final boolean deleteFakeDir,
        final BulkOperationState operationState)
        throws MultiObjectDeleteException, AmazonClientException,
        IOException;
  }

}
