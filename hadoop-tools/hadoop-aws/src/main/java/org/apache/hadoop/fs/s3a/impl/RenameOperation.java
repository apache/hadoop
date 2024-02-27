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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.RenameFailedException;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.OperationDuration;

import static org.apache.hadoop.fs.s3a.S3AUtils.translateException;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.waitForCompletionIgnoringExceptions;
import static org.apache.hadoop.fs.store.audit.AuditingFunctions.callableWithinAuditSpan;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.submit;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.waitForCompletion;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.RENAME_PARALLEL_LIMIT;
import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * A parallelized rename operation.
 * <p>
 * The parallel execution is in groups of size
 * {@link InternalConstants#RENAME_PARALLEL_LIMIT}; it is only
 * after one group completes that the next group is initiated.
 * <p>
 * Once enough files have been copied that they meet the
 * {@link InternalConstants#MAX_ENTRIES_TO_DELETE} threshold, a delete
 * is initiated.
 * If it succeeds, the rename continues with the next group of files.
 * <p>
 * Directory Markers which have child entries are never copied; only those
 * which represent empty directories are copied in the rename.
 * The {@link DirMarkerTracker} tracks which markers must be copied, and
 * which can simply be deleted from the source.
 * As a result: rename always purges all non-leaf directory markers from
 * the copied tree. This is to ensure that even if a directory tree
 * is copied from an authoritative path to a non-authoritative one
 * there is never any contamination of the non-auth path with markers.
 * <p>
 * The rename operation implements the classic HDFS rename policy of
 * rename(file, dir) renames the file under the directory.
 * <p>
 *
 * There is <i>no</i> validation of input and output paths.
 * Callers are required to themselves verify that destination is not under
 * the source, above the source, the source itself, etc, etc.
 */
public class RenameOperation extends ExecutingStoreOperation<Long> {

  private static final Logger LOG = LoggerFactory.getLogger(
      RenameOperation.class);

  private final Path sourcePath;

  private final String sourceKey;

  private final S3AFileStatus sourceStatus;

  private final Path destPath;

  private final String destKey;

  private final S3AFileStatus destStatus;

  /**
   * Callbacks into the filesystem.
   */
  private final OperationCallbacks callbacks;

  /**
   * Counter of bytes copied.
   */

  private final AtomicLong bytesCopied = new AtomicLong();

  /**
   * Page size for bulk deletes.
   */
  private final int pageSize;

  /**
   * List of active copies.
   */
  private final List<CompletableFuture<Path>> activeCopies =
      new ArrayList<>(RENAME_PARALLEL_LIMIT);

  /**
   * list of keys to delete on the next (bulk) delete call.
   */
  private final List<ObjectIdentifier> keysToDelete =
      new ArrayList<>();

  /**
   * Do directory operations purge pending uploads?
   */
  private final boolean dirOperationsPurgeUploads;

  /**
   * Count of uploads aborted.
   */
  private Optional<Long> uploadsAborted = Optional.empty();

  /**
   * Initiate the rename.
   * @param storeContext store context
   * @param sourcePath source path
   * @param sourceKey key of source
   * @param sourceStatus pre-fetched source status
   * @param destPath destination path.
   * @param destKey destination key
   * @param destStatus destination status.
   * @param callbacks callback provider
   * @param pageSize size of delete requests
   * @param dirOperationsPurgeUploads Do directory operations purge pending uploads?
   */
  public RenameOperation(
      final StoreContext storeContext,
      final Path sourcePath,
      final String sourceKey,
      final S3AFileStatus sourceStatus,
      final Path destPath,
      final String destKey,
      final S3AFileStatus destStatus,
      final OperationCallbacks callbacks,
      final int pageSize,
      final boolean dirOperationsPurgeUploads) {
    super(storeContext);
    this.sourcePath = sourcePath;
    this.sourceKey = sourceKey;
    this.sourceStatus = sourceStatus;
    this.destPath = destPath;
    this.destKey = destKey;
    this.destStatus = destStatus;
    this.callbacks = callbacks;
    checkArgument(pageSize > 0
                    && pageSize <= InternalConstants.MAX_ENTRIES_TO_DELETE,
            "page size out of range: %s", pageSize);
    this.pageSize = pageSize;
    this.dirOperationsPurgeUploads = dirOperationsPurgeUploads;
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
   * Wait for the active copies to complete then reset the list.
   * @param reason for messages
   * @throws IOException if one of the called futures raised an IOE.
   * @throws RuntimeException if one of the futures raised one.
   */
  @Retries.OnceTranslated
  private void completeActiveCopies(String reason) throws IOException {
    LOG.debug("Waiting for {} active copies to complete: {}",
        activeCopies.size(), reason);
    waitForCompletion(activeCopies);
    activeCopies.clear();
  }

  /**
   * Queue an object for deletion.
   * <p>
   * This object will be deleted when the next page of objects to delete
   * is posted to S3. Therefore, the COPY must have finished
   * before that deletion operation takes place.
   * This is managed by:
   * <ol>
   *   <li>
   *     The delete operation only being executed once all active
   *     copies have completed.
   *   </li>
   *   <li>
   *     Only queuing objects here whose copy operation has
   *     been submitted and so is in that thread pool.
   *   </li>
   * </ol>
   * This method must only be called from the primary thread.
   * @param path path to the object.
   * @param key key of the object.
   */
  private void queueToDelete(Path path, String key) {
    LOG.debug("Queueing to delete {}", path);
    keysToDelete.add(ObjectIdentifier.builder().key(key).build());
  }

  /**
   * Queue a list of markers for deletion.
   * <p>
   * no-op if the list is empty.
   * <p>
   * See {@link #queueToDelete(Path, String)} for
   * details on safe use of this method.
   *
   * @param markersToDelete markers
   */
  private void queueToDelete(
      List<DirMarkerTracker.Marker> markersToDelete) {
    markersToDelete.forEach(m -> queueToDelete(
        null,
        m.getKey()));
  }

  /**
   * Queue a single marker for deletion.
   * <p>
   * See {@link #queueToDelete(Path, String)} for
   * details on safe use of this method.
   *
   * @param marker markers
   */
  private void queueToDelete(final DirMarkerTracker.Marker marker) {
    queueToDelete(marker.getPath(), marker.getKey());
  }

  /**
   * Block waiting for ay active copies to finish
   * then delete all queued keys + paths to delete.
   * @param reason reason for logs
   * @throws IOException failure.
   */
  @Retries.RetryTranslated
  private void completeActiveCopiesAndDeleteSources(String reason)
      throws IOException {
    completeActiveCopies(reason);
    removeSourceObjects(
        keysToDelete
    );
    // now reset the lists.
    keysToDelete.clear();
  }

  @Retries.RetryMixed
  public Long execute() throws IOException {
    executeOnlyOnce();

    // The path to whichever file or directory is created by the
    // rename. When deleting markers all parents of
    // this path will need their markers pruned.
    Path destCreated = destPath;

    // Ok! Time to start
    try {
      if (sourceStatus.isFile()) {
        // rename the file. The destination path will be different
        // from that passed in if the destination is a directory;
        // the final value is needed to completely delete parent markers
        // when they are not being retained.
        destCreated = renameFileToDest();
      } else {
        recursiveDirectoryRename();
      }
    } catch (SdkException | IOException ex) {
      // rename failed.
      // block for all ongoing copies to complete, successfully or not
      try {
        completeActiveCopies("failure handling");
      } catch (IOException e) {
        // Downgrading to warn because an exception is already
        // about to be thrown.
        LOG.warn("While completing all active copies", e);
      }
      throw convertToIOException(ex);
    }
    callbacks.finishRename(sourcePath, destCreated);
    return bytesCopied.get();
  }

  /**
   * The source is a file: rename it to the destination, which
   * will be under the current destination path if that is a directory.
   * @return the path of the object created.
   * @throws IOException failure
   */
  protected Path renameFileToDest() throws IOException {
    final StoreContext storeContext = getStoreContext();
    // the source is a file.
    Path copyDestinationPath = destPath;
    String copyDestinationKey = destKey;
    S3ObjectAttributes sourceAttributes =
        callbacks.createObjectAttributes(sourceStatus);
    S3AReadOpContext readContext = callbacks.createReadContext(sourceStatus);
    if (destStatus != null && destStatus.isDirectory()) {
      // destination is a directory: build the final destination underneath
      String newDestKey = maybeAddTrailingSlash(destKey);
      String filename = sourceKey.substring(
          storeContext.pathToKey(sourcePath.getParent()).length() + 1);
      newDestKey = newDestKey + filename;
      copyDestinationKey = newDestKey;
      copyDestinationPath = storeContext.keyToPath(newDestKey);
    }
    // destination either does not exist or is a file to overwrite.
    LOG.debug("rename: renaming file {} to {}", sourcePath,
        copyDestinationPath);
    copySource(
        sourceKey,
        sourceAttributes,
        readContext,
        copyDestinationPath,
        copyDestinationKey);
    bytesCopied.addAndGet(sourceStatus.getLen());
    // delete the source
    callbacks.deleteObjectAtPath(sourcePath, sourceKey, true);
    return copyDestinationPath;
  }

  /**
   * Execute a full recursive rename.
   * There is a special handling of directly markers here -only leaf markers
   * are copied. This reduces incompatibility "regions" across versions.
   * @throws IOException failure
   */
  protected void recursiveDirectoryRename() throws IOException {
    final StoreContext storeContext = getStoreContext();

    LOG.debug("rename: renaming directory {} to {}", sourcePath, destPath);

    // This is a directory-to-directory copy
    String dstKey = maybeAddTrailingSlash(destKey);
    String srcKey = maybeAddTrailingSlash(sourceKey);

    // Verify dest is not a child of the source directory
    if (dstKey.startsWith(srcKey)) {
      throw new RenameFailedException(srcKey, dstKey,
          "cannot rename a directory to a subdirectory of itself ");
    }
    // start the async dir cleanup
    final CompletableFuture<Long> abortUploads;
    if (dirOperationsPurgeUploads) {
      final String key = srcKey;
      LOG.debug("All uploads under {} will be deleted", key);
      abortUploads = submit(getStoreContext().getExecutor(), () ->
          callbacks.abortMultipartUploadsUnderPrefix(key));
    } else {
      abortUploads = null;
    }

    if (destStatus != null
        && destStatus.isEmptyDirectory() == Tristate.TRUE) {
      // delete unnecessary fake directory at the destination.

      LOG.debug("Deleting fake directory marker at destination {}",
          destStatus.getPath());
      // Although the dir marker policy doesn't always need to do this,
      // it's simplest just to be consistent here.
      callbacks.deleteObjectAtPath(destStatus.getPath(), dstKey, false);
    }

    Path parentPath = storeContext.keyToPath(srcKey);

    // Track directory markers so that we know which leaf directories need to be
    // recreated
    DirMarkerTracker dirMarkerTracker = new DirMarkerTracker(parentPath,
        false);

    final RemoteIterator<S3ALocatedFileStatus> iterator =
        callbacks.listFilesAndDirectoryMarkers(parentPath,
            sourceStatus,
            true);
    while (iterator.hasNext()) {
      // get the next entry in the listing.
      S3ALocatedFileStatus child = iterator.next();
      LOG.debug("To rename {}", child);
      // convert it to an S3 key.
      String k = storeContext.pathToKey(child.getPath());
      // possibly adding a "/" if it represents directory and it does
      // not have a trailing slash already.
      String key = (child.isDirectory() && !k.endsWith("/"))
          ? k + "/"
          : k;
      // the source object to copy as a path.
      Path childSourcePath = storeContext.keyToPath(key);

      List<DirMarkerTracker.Marker> markersToDelete;

      boolean isMarker = key.endsWith("/");
      if (isMarker) {
        // add the marker to the tracker.
        // it will not be deleted _yet_ but it may find a list of parent
        // markers which may now be deleted.
        markersToDelete = dirMarkerTracker.markerFound(
            childSourcePath, key, child);
      } else {
        // it is a file.
        // note that it has been found -this may find a list of parent
        // markers which may now be deleted.
        markersToDelete = dirMarkerTracker.fileFound(
            childSourcePath, key, child);
        // the destination key is that of the key under the source tree,
        // remapped under the new destination path.
        String newDestKey =
            dstKey + key.substring(srcKey.length());
        Path childDestPath = storeContext.keyToPath(newDestKey);

        // mark the source file for deletion on a successful copy.
        queueToDelete(childSourcePath, key);
          // now begin the single copy
        CompletableFuture<Path> copy = initiateCopy(child, key,
            newDestKey, childDestPath);
        activeCopies.add(copy);
        bytesCopied.addAndGet(sourceStatus.getLen());
      }
      // add any markers to delete to the operation so they get cleaned
      // incrementally
      queueToDelete(markersToDelete);
      // and trigger any end of loop operations
      endOfLoopActions();
    } // end of iteration through the list

    // finally process remaining directory markers
    copyEmptyDirectoryMarkers(srcKey, dstKey, dirMarkerTracker);

    // await the final set of copies and their deletion
    // This will notify the renameTracker that these objects
    // have been deleted.
    completeActiveCopiesAndDeleteSources("final copy and delete");

    // and if uploads were being aborted, wait for that to finish
    uploadsAborted = waitForCompletionIgnoringExceptions(abortUploads);
  }

  /**
   * Operations to perform at the end of every loop iteration.
   * <p>
   * This may block the thread waiting for copies to complete
   * and/or delete a page of data.
   */
  private void endOfLoopActions() throws IOException {
    if (keysToDelete.size() == pageSize) {
      // finish ongoing copies then delete all queued keys.
      completeActiveCopiesAndDeleteSources("paged delete");
    } else {
      if (activeCopies.size() == RENAME_PARALLEL_LIMIT) {
        // the limit of active copies has been reached;
        // wait for completion or errors to surface.
        LOG.debug("Waiting for active copies to complete");
        completeActiveCopies("batch threshold reached");
      }
    }
  }

  /**
   * Process all directory markers at the end of the rename.
   * All leaf markers are queued to be copied in the store;
   * <p>
   * Why not simply create new markers? All the metadata
   * gets copied too, so if there was anything relevant then
   * it would be preserved.
   * <p>
   * At the same time: markers aren't valued much and may
   * be deleted without any safety checks -so if there was relevant
   * data it is at risk of destruction at any point.
   * If there are lots of empty directory rename operations taking place,
   * the decision to copy the source may need revisiting.
   * Be advised though: the costs of the copy not withstanding,
   * it is a lot easier to have one single type of scheduled copy operation
   * than have copy and touch calls being scheduled.
   * <p>
   * The duration returned is the time to initiate all copy/delete operations,
   * including any blocking waits for active copies and paged deletes
   * to execute. There may still be outstanding operations
   * queued by this method -the duration may be an underestimate
   * of the time this operation actually takes.
   *
   * @param srcKey source key with trailing /
   * @param dstKey dest key with trailing /
   * @param dirMarkerTracker tracker of markers
   * @return how long it took.
   */
  private OperationDuration copyEmptyDirectoryMarkers(
      final String srcKey,
      final String dstKey,
      final DirMarkerTracker dirMarkerTracker) throws IOException {
    // directory marker work.
    LOG.debug("Copying markers from {}", dirMarkerTracker);
    final StoreContext storeContext = getStoreContext();
    Map<Path, DirMarkerTracker.Marker> leafMarkers =
        dirMarkerTracker.getLeafMarkers();
    Map<Path, DirMarkerTracker.Marker> surplus =
        dirMarkerTracker.getSurplusMarkers();
    // for all leaf markers: copy the original
    DurationInfo duration = new DurationInfo(LOG, false,
        "copying %d leaf markers with %d surplus not copied",
        leafMarkers.size(), surplus.size());
    for (DirMarkerTracker.Marker entry: leafMarkers.values()) {
      String key = entry.getKey();
      String newDestKey =
          dstKey + key.substring(srcKey.length());
      Path childDestPath = storeContext.keyToPath(newDestKey);
      LOG.debug("copying dir marker from {} to {}", key, newDestKey);

      activeCopies.add(
          initiateCopy(
              entry.getStatus(),
              key,
              newDestKey,
              childDestPath));
      queueToDelete(entry);
      // end of loop
      endOfLoopActions();
    }
    duration.close();
    return duration;
  }

  /**
   * Initiate a copy operation in the executor.
   * @param source status of the source object.
   * @param key source key
   * @param newDestKey destination key
   * @param childDestPath destination path.
   * @return the future.
   */
  protected CompletableFuture<Path> initiateCopy(
      final S3ALocatedFileStatus source,
      final String key,
      final String newDestKey,
      final Path childDestPath) {
    S3ObjectAttributes sourceAttributes =
        callbacks.createObjectAttributes(
            source.getPath(),
            source.getEtag(),
            source.getVersionId(),
            source.getLen());
    // queue the copy operation for execution in the thread pool
    return submit(getStoreContext().getExecutor(),
        callableWithinAuditSpan(getAuditSpan(), () ->
            copySource(
                key,
                sourceAttributes,
                callbacks.createReadContext(source),
                childDestPath,
                newDestKey)));
  }

  /**
   * This is invoked to copy a file or directory marker.
   * It may be called in its own thread.
   * @param srcKey source key
   * @param srcAttributes status of the source object
   * @param destination destination as a qualified path.
   * @param destinationKey destination key
   * @return the destination path.
   * @throws IOException failure
   */
  @Retries.RetryTranslated
  private Path copySource(
      final String srcKey,
      final S3ObjectAttributes srcAttributes,
      final S3AReadOpContext readContext,
      final Path destination,
      final String destinationKey) throws IOException {
    long len = srcAttributes.getLen();
    try (DurationInfo ignored = new DurationInfo(LOG, false,
        "Copy file from %s to %s (length=%d)", srcKey, destinationKey, len)) {
      callbacks.copyFile(srcKey, destinationKey,
          srcAttributes, readContext);
    }
    return destination;
  }

  /**
   * Remove source objects.
   * @param keys list of keys to delete
   * @throws IOException failure
   */
  @Retries.RetryTranslated
  private void removeSourceObjects(
      final List<ObjectIdentifier> keys)
      throws IOException {
    // remove the keys

    // list what is being deleted for the interest of anyone
    // who is trying to debug why objects are no longer there.
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initiating delete operation for {} objects", keys.size());
      for (ObjectIdentifier objectIdentifier : keys) {
        LOG.debug(" {} {}", objectIdentifier.key(),
            objectIdentifier.versionId() != null ? objectIdentifier.versionId() : "");
      }
    }

    Invoker.once("rename " + sourcePath + " to " + destPath,
        sourcePath.toString(), () ->
            callbacks.removeKeys(
                keys,
                false
            ));
  }

  /**
   * Turns a path (relative or otherwise) into an S3 key, adding a trailing
   * "/" if the path is not the root <i>and</i> does not already have a "/"
   * at the end.
   *
   * @param key s3 key or ""
   * @return the with a trailing "/", or, if it is the root key, "",
   */
  private String maybeAddTrailingSlash(String key) {
    if (!key.isEmpty() && !key.endsWith("/")) {
      return key + '/';
    } else {
      return key;
    }
  }

  /**
   * Convert a passed in exception (expected to be an IOE or AWS exception)
   * into an IOException.
   * @param ex exception caught
   * @return the exception to throw in the failure handler.
   */
  protected IOException convertToIOException(final Exception ex) {
    if (ex instanceof IOException) {
      return (IOException) ex;
    } else if (ex instanceof SdkException) {
      return translateException("rename " + sourcePath + " to " + destPath,
          sourcePath.toString(),
          (SdkException) ex);
    } else {
      // should never happen, but for completeness
      return new IOException(ex);
    }
  }
}
