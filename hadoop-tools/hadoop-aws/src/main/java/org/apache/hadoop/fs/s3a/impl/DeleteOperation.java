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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
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
import org.apache.hadoop.util.DurationInfo;


import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.waitForCompletionIgnoringExceptions;
import static org.apache.hadoop.fs.store.audit.AuditingFunctions.callableWithinAuditSpan;
import static org.apache.hadoop.util.Preconditions.checkArgument;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.maybeAwaitCompletion;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.submit;

/**
 * Implementation of the delete() operation.
 * This issues only one bulk delete at a time,
 * intending to update S3Guard after every request succeeded.
 * Now that S3Guard has been removed, it
 * would be possible to issue multiple delete calls
 * in parallel.
 * If this is done, then it may be good to experiment with different
 * page sizes. The default value is
 * {@link InternalConstants#MAX_ENTRIES_TO_DELETE}, the maximum a single
 * POST permits.
 * <p>
 * Smaller pages executed in parallel may have different
 * performance characteristics when deleting very large directories.
 * Any exploration of options here MUST be done with performance
 * measurements taken from test runs in EC2 against local S3 stores,
 * so as to ensure network latencies do not skew the results.

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
   * Executor for async operations.
   */
  private final ListeningExecutorService executor;

  /**
   * List of keys built up for the next delete batch.
   */
  private List<DeleteEntry> keys;

  /**
   * The single async delete operation, or null.
   */
  private CompletableFuture<Void> deleteFuture;

  /**
   * Counter of deleted files.
   */
  private long filesDeleted;

  /**
   * Do directory operations purge pending uploads?
   */
  private final boolean dirOperationsPurgeUploads;

  /**
   * Count of uploads aborted.
   */
  private Optional<Long> uploadsAborted = Optional.empty();

  /**
   * Constructor.
   * @param context store context
   * @param status  pre-fetched source status
   * @param recursive recursive delete?
   * @param callbacks callback provider
   * @param pageSize size of delete pages
   * @param dirOperationsPurgeUploads Do directory operations purge pending uploads?
   */
  public DeleteOperation(final StoreContext context,
      final S3AFileStatus status,
      final boolean recursive,
      final OperationCallbacks callbacks,
      final int pageSize,
      final boolean dirOperationsPurgeUploads) {

    super(context);
    this.status = status;
    this.recursive = recursive;
    this.callbacks = callbacks;
    checkArgument(pageSize > 0
            && pageSize <= InternalConstants.MAX_ENTRIES_TO_DELETE,
        "page size out of range: %s", pageSize);
    this.pageSize = pageSize;
    executor = MoreExecutors.listeningDecorator(
        context.createThrottledExecutor(1));
    this.dirOperationsPurgeUploads = dirOperationsPurgeUploads;
  }

  public long getFilesDeleted() {
    return filesDeleted;
  }

  /**
   * Get the count of uploads aborted.
   * Non-empty iff enabled, and the operations completed without errors.
   * @return count of aborted uploads.
   */
  public Optional<Long> getUploadsAborted() {
    return uploadsAborted;
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
   * With S3Guard removed, the problem of updating any
   * DynamoDB store has gone away -delete calls could now be issued
   * in parallel.
   * However, rate limiting may be required to keep write load
   * below the throttling point. Every entry in a single
   * bulk delete call counts as a single write request -overloading
   * an S3 partition with delete calls has been a problem in
   * the past.
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
   * the directory path.
   * <p>
   * Once the first {@link #pageSize} worth of objects has been listed, a batch
   * delete is queued for execution in a separate thread; subsequent batches
   * block waiting for the first call to complete or fail before again,
   * being deleted in the separate thread.
   * <p>
   * After all listed objects are queued for deletion,
   * @param path directory path
   * @param dirKey directory key
   * @throws IOException failure
   */
  @Retries.RetryTranslated
  protected void deleteDirectoryTree(final Path path,
      final String dirKey) throws IOException {

    try (DurationInfo ignored =
             new DurationInfo(LOG, false, "deleting %s", dirKey)) {

      final CompletableFuture<Long> abortUploads;
      if (dirOperationsPurgeUploads) {
        final StoreContext sc = getStoreContext();
        final String key = dirKey;
        LOG.debug("All uploads under {} will be deleted", key);
        abortUploads = submit(sc.getExecutor(), sc.getActiveAuditSpan(), () ->
            callbacks.abortMultipartUploadsUnderPrefix(key));
      } else {
        abortUploads = null;
      }

      // init the lists of keys and paths to delete
      resetDeleteList();
      deleteFuture = null;

      // list files
      LOG.debug("Getting objects for directory prefix {} to delete", dirKey);
      final RemoteIterator<S3ALocatedFileStatus> locatedFiles =
          callbacks.listFilesAndDirectoryMarkers(path, status,
              true);

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
      uploadsAborted = waitForCompletionIgnoringExceptions(abortUploads);
    }
    LOG.debug("Delete \"{}\" completed; deleted {} objects and aborted {} uploads", path,
        filesDeleted, uploadsAborted.orElse(0L));
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
    queueForDeletion(deletionKey(stat), stat.isDirectory());
  }

  /**
   * Queue keys for deletion.
   * Once a page of keys are ready to delete this
   * call is submitted to the executor, after waiting for the previous run to
   * complete.
   *
   * @param key key to delete
   * @param isDirMarker is the entry a directory?
   * @throws IOException failure of the previous batch of deletions.
   */
  private void queueForDeletion(final String key,
      boolean isDirMarker) throws IOException {
    LOG.debug("Adding object to delete: \"{}\"", key);
    keys.add(new DeleteEntry(key, isDirMarker));
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
    maybeAwaitCompletion(deleteFuture).ifPresent(count ->
        LOG.debug("Deleted {} uploads", count));

    // delete the current page of keys and paths
    deleteFuture = submitDelete(keys);
    // reset the references so a new list can be built up.
    resetDeleteList();
  }

  /**
   * Reset the lists of keys and paths so that a new batch of
   * entries can built up.
   */
  private void resetDeleteList() {
    keys = new ArrayList<>(pageSize);
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
    callbacks.deleteObjectAtPath(path, key, isFile);
  }

  /**
   * Delete a single page of keys.
   * If the list is empty no work is submitted and null is returned.
   *
   * @param keyList keys to delete.
   * @return the submitted future or null
   */
  private CompletableFuture<Void> submitDelete(
      final List<DeleteEntry> keyList) {

    if (keyList.isEmpty()) {
      return null;
    }
    filesDeleted += keyList.size();
    return submit(executor,
        callableWithinAuditSpan(
            getAuditSpan(), () -> {
              asyncDeleteAction(
                  keyList);
              return null;
            }));
  }

  /**
   * The action called in the asynchronous thread to delete
   * the keys from S3 and paths from S3Guard.
   *
   * @param keyList keys to delete.
   * entries logged?
   * @throws IOException failure
   */
  @Retries.RetryTranslated
  private void asyncDeleteAction(
      final List<DeleteEntry> keyList)
      throws IOException {
    try (DurationInfo ignored =
             new DurationInfo(LOG, false,
                 "Delete page of %d keys", keyList.size())) {
      if (!keyList.isEmpty()) {
        // first delete the files.
        List<ObjectIdentifier> files = keyList.stream()
            .filter(e -> !e.isDirMarker)
            .map(e -> e.objectIdentifier)
            .collect(Collectors.toList());
        LOG.debug("Deleting of {} file objects", files.size());
        Invoker.once("Remove S3 Files",
            status.getPath().toString(),
            () -> callbacks.removeKeys(
                files,
                false
            ));
        // now the dirs
        List<ObjectIdentifier> dirs = keyList.stream()
            .filter(e -> e.isDirMarker)
            .map(e -> e.objectIdentifier)
            .collect(Collectors.toList());
        if (!dirs.isEmpty()) {
          LOG.debug("Deleting {} directory markers", dirs.size());
          // This is invoked with deleteFakeDir.
          Invoker.once("Remove S3 Dir Markers",
              status.getPath().toString(),
              () -> callbacks.removeKeys(
                  dirs,
                  true
              ));
        }
      }
    }
  }

  /**
   * Deletion entry; dir marker state is tracked to allow
   * delete requests to be split into file
   * and marker delete phases.
   * Without S3Guard, the split is only used
   * to choose which statistics to update.
   */
  private static final class DeleteEntry {
    private final ObjectIdentifier objectIdentifier;

    private final boolean isDirMarker;

    private DeleteEntry(final String key, final boolean isDirMarker) {
      this.objectIdentifier = ObjectIdentifier.builder().key(key).build();
      this.isDirMarker = isDirMarker;
    }

    public String getKey() {
      return objectIdentifier.key();
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
