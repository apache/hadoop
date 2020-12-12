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
import java.util.stream.Collectors;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.fs.s3a.s3guard.BulkOperationState;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.S3Guard;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.maybeAwaitCompletion;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.submit;

/**
 * Implementation of the delete() operation.
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
 * <p>
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
 * <p>
 * 2. Note that as the DDB thread/connection pools will be shared across
 * all active delete operations, speedups will be minimal unless
 * those pools are large enough to cope the extra load.
 * <p>
 * There are also some opportunities to explore in
 * {@code DynamoDBMetadataStore} with batching delete requests
 * in the DDB APIs.
 */
public class DeleteOperation extends ExecutingStoreOperation<Boolean> {

  private static final Logger LOG = LoggerFactory.getLogger(
      DeleteOperation.class);

  /**
   * Pre-fetched source status.
   */
  private final S3AFileStatus status;

  /**
   * Recursive delete?
   */
  private final boolean recursive;

  /**
   * Callback provider.
   */
  private final OperationCallbacks callbacks;

  /**
   * Number of entries in a page.
   */
  private final int pageSize;

  /**
   * Metastore -never null but may be the NullMetadataStore.
   */
  private final MetadataStore metadataStore;

  /**
   * Executor for async operations.
   */
  private final ListeningExecutorService executor;

  /**
   * List of keys built up for the next delete batch.
   */
  private List<DeleteEntry> keys;

  /**
   * List of paths built up for incremental deletion on tree delete.
   * At the end of the entire delete the full tree is scanned in S3Guard
   * and tombstones added. For this reason this list of paths <i>must not</i>
   * include directory markers, as that will break the scan.
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
   * @param pageSize size of delete pages
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
        "page size out of range: %s", pageSize);
    this.pageSize = pageSize;
    metadataStore = context.getMetadataStore();
    executor = MoreExecutors.listeningDecorator(
        context.createThrottledExecutor(1));
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
  public Boolean execute() throws IOException {
    executeOnlyOnce();

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
        deleteDirectoryTree(path, key);
      }

    } else {
      // simple file.
      LOG.debug("deleting simple file {}", path);
      deleteObjectAtPath(path, key, true);
    }
    LOG.debug("Deleted {} objects", filesDeleted);
    return true;
  }

  /**
   * Delete a directory tree.
   * <p>
   * This is done by asking the filesystem for a list of all objects under
   * the directory path, without using any S3Guard tombstone markers to hide
   * objects which may be returned in S3 listings but which are considered
   * deleted.
   * <p>
   * Once the first {@link #pageSize} worth of objects has been listed, a batch
   * delete is queued for execution in a separate thread; subsequent batches
   * block waiting for the first call to complete or fail before again,
   * being deleted in the separate thread.
   * <p>
   * After all listed objects are queued for deletion,
   * if the path is considered authoritative in the client, a final scan
   * of S3 <i>without S3Guard</i> is executed, so as to find and delete
   * any out-of-band objects in the tree.
   * @param path directory path
   * @param dirKey directory key
   * @throws IOException failure
   */
  @Retries.RetryTranslated
  protected void deleteDirectoryTree(final Path path,
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
          callbacks.listFilesAndDirectoryMarkers(path, status,
              false, true);

      // iterate through and delete. The next() call will block when a new S3
      // page is required; this any active delete submitted to the executor
      // will run in parallel with this.
      while (locatedFiles.hasNext()) {
        // get the next entry in the listing.
        S3AFileStatus child = locatedFiles.next().toS3AFileStatus();
        queueForDeletion(child);
      }
      LOG.debug("Deleting final batch of listed files");
      submitNextBatch();
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
          S3AFileStatus next = objects.next();
          LOG.debug("Found Unlisted entry {}", next);
          queueForDeletion(deletionKey(next), null,
              next.isDirectory());
        }
        if (extraFilesDeleted > 0) {
          LOG.debug("Raw S3 Scan found {} extra file(s) to delete",
              extraFilesDeleted);
          // there is no more data:
          // await any ongoing operation
          submitNextBatch();
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
   * Build an S3 key for a delete request,
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
    queueForDeletion(deletionKey(stat), stat.getPath(), stat.isDirectory());
  }

  /**
   * Queue keys for deletion.
   * Once a page of keys are ready to delete this
   * call is submitted to the executor, after waiting for the previous run to
   * complete.
   *
   * @param key key to delete
   * @param deletePath nullable path of the key
   * @param isDirMarker is the entry a directory?
   * @throws IOException failure of the previous batch of deletions.
   */
  private void queueForDeletion(final String key,
      @Nullable final Path deletePath,
      boolean isDirMarker) throws IOException {
    LOG.debug("Adding object to delete: \"{}\"", key);
    keys.add(new DeleteEntry(key, isDirMarker));
    if (deletePath != null) {
      if (!isDirMarker) {
        paths.add(deletePath);
      }
    }

    if (keys.size() == pageSize) {
      submitNextBatch();
    }
  }

  /**
   * Wait for the previous batch to finish then submit this page.
   * The lists of keys and pages are reset here.
   *
   * @throws IOException failure of the previous batch of deletions.
   */
  private void submitNextBatch()
      throws IOException {
    // delete a single page of keys and the metadata.
    // block for any previous batch.
    maybeAwaitCompletion(deleteFuture);

    // delete the current page of keys and paths
    deleteFuture = submitDelete(keys, paths);
    // reset the references so a new list can be built up.
    resetDeleteList();
  }

  /**
   * Reset the lists of keys and paths so that a new batch of
   * entries can built up.
   */
  private void resetDeleteList() {
    keys = new ArrayList<>(pageSize);
    paths = new ArrayList<>(pageSize);
  }

  /**
   * Delete a file or directory marker.
   * @param path path
   * @param key key
   * @param isFile is this a file?
   * @throws IOException failure
   */
  @Retries.RetryTranslated
  private void deleteObjectAtPath(
      final Path path,
      final String key,
      final boolean isFile)
      throws IOException {
    LOG.debug("delete: {} {}", (isFile ? "file" : "dir marker"), key);
    filesDeleted++;
    callbacks.deleteObjectAtPath(path, key, isFile, operationState);
  }

  /**
   * Delete a single page of keys and optionally the metadata.
   * For a large page, it is the metadata size which dominates.
   * Its possible to invoke this with empty lists of keys or paths.
   * If both lists are empty no work is submitted and null is returned.
   *
   * @param keyList keys to delete.
   * @param pathList paths to update the metastore with.
   * @return the submitted future or null
   */
  private CompletableFuture<Void> submitDelete(
      final List<DeleteEntry> keyList,
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
  @Retries.RetryTranslated
  private void asyncDeleteAction(
      final BulkOperationState state,
      final List<DeleteEntry> keyList,
      final List<Path> pathList,
      final boolean auditDeletedKeys)
      throws IOException {
    List<DeleteObjectsResult.DeletedObject> deletedObjects = new ArrayList<>();
    try (DurationInfo ignored =
             new DurationInfo(LOG, false,
                 "Delete page of %d keys", keyList.size())) {
      DeleteObjectsResult result = null;
      List<Path> undeletedObjects = new ArrayList<>();
      if (!keyList.isEmpty()) {
        // first delete the files.
        List<DeleteObjectsRequest.KeyVersion> files = keyList.stream()
            .filter(e -> !e.isDirMarker)
            .map(e -> e.keyVersion)
            .collect(Collectors.toList());
        LOG.debug("Deleting of {} file objects", files.size());
        result = Invoker.once("Remove S3 Files",
            status.getPath().toString(),
            () -> callbacks.removeKeys(
                files,
                false,
                undeletedObjects,
                state,
                !auditDeletedKeys));
        if (result != null) {
          deletedObjects.addAll(result.getDeletedObjects());
        }
        // now the dirs
        List<DeleteObjectsRequest.KeyVersion> dirs = keyList.stream()
            .filter(e -> e.isDirMarker)
            .map(e -> e.keyVersion)
            .collect(Collectors.toList());
        LOG.debug("Deleting of {} directory markers", dirs.size());
        // This is invoked with deleteFakeDir = true, so
        // S3Guard is not updated.
        result = Invoker.once("Remove S3 Dir Markers",
            status.getPath().toString(),
            () -> callbacks.removeKeys(
                dirs,
                true,
                undeletedObjects,
                state,
                !auditDeletedKeys));
        if (result != null) {
          deletedObjects.addAll(result.getDeletedObjects());
        }
      }
      if (!pathList.isEmpty()) {
        // delete file paths only. This stops tombstones
        // being added until the final directory cleanup
        // (HADOOP-17244)
        metadataStore.deletePaths(pathList, state);
      }
      if (auditDeletedKeys) {
        // audit the deleted keys
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
          for (DeleteEntry kv : keyList) {
            LOG.debug("{}", kv.getKey());
          }
        }
      }
    }
  }

  /**
   * Deletion entry; dir marker state is tracked to control S3Guard
   * update policy.
   */
  private static final class DeleteEntry {
    private final DeleteObjectsRequest.KeyVersion keyVersion;

    private final boolean isDirMarker;

    private DeleteEntry(final String key, final boolean isDirMarker) {
      this.keyVersion = new DeleteObjectsRequest.KeyVersion(key);
      this.isDirMarker = isDirMarker;
    }

    public String getKey() {
      return keyVersion.getKey();
    }

    @Override
    public String toString() {
      return "DeleteEntry{" +
          "key='" + getKey() + '\'' +
          ", isDirMarker=" + isDirMarker +
          '}';
    }
  }

}
