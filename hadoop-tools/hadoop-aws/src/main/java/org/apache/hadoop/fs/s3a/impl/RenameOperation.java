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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.transfer.model.CopyResult;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.RenameFailedException;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.RenameTracker;
import org.apache.hadoop.util.DurationInfo;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A_BLOCK_SIZE;
import static org.apache.hadoop.fs.s3a.S3AUtils.objectRepresentsDirectory;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.submit;
import static org.apache.hadoop.fs.s3a.impl.CallableSupplier.waitForCompletion;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.DEFAULT_BLOCKSIZE;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.RENAME_PARALLEL_LIMIT;

/**
 * A parallelized rename operation which updates the metastore in the
 * process, through whichever {@link RenameTracker} the store provides.
 * The parallel execution is in groups of size
 * {@link InternalConstants#RENAME_PARALLEL_LIMIT}; it is only
 * after one group completes that the next group is initiated.
 * Once enough files have been copied that they meet the
 * {@link InternalConstants#MAX_ENTRIES_TO_DELETE} threshold, a delete
 * is initiated.
 * If it succeeds, the rename continues with the next group of files.
 *
 * The RenameTracker has the task of keeping the metastore up to date
 * as the rename proceeds.
 *
 * The rename operation implements the classic HDFS rename policy of
 * rename(file, dir) renames the file under the directory.
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
   * Rename tracker.
   */
  private RenameTracker renameTracker;

  /**
   * List of active copies.
   */
  private final List<CompletableFuture<Path>> activeCopies =
      new ArrayList<>(RENAME_PARALLEL_LIMIT);

  /**
   * list of keys to delete on the next (bulk) delete call.
   */
  private final List<DeleteObjectsRequest.KeyVersion> keysToDelete =
      new ArrayList<>();

  /**
   * List of paths to delete, which will be passed to the rename
   * tracker after the deletion succeeds.
   */
  private final List<Path> pathsToDelete = new ArrayList<>();

  private final long blocksize;

  /**
   * Initiate the rename.
   *
   * @param storeContext store context
   * @param sourcePath source path
   * @param sourceKey key of source
   * @param sourceStatus pre-fetched source status
   * @param destPath destination path.
   * @param destKey destination key
   * @param destStatus destination status.
   * @param callbacks callback provider
   * @param pageSize size of delete requests
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
      final int pageSize) {
    super(storeContext);
    this.sourcePath = sourcePath;
    this.sourceKey = sourceKey;
    this.sourceStatus = sourceStatus;
    this.destPath = destPath;
    this.destKey = destKey;
    this.destStatus = destStatus;
    this.callbacks = callbacks;
    blocksize = storeContext.getConfiguration()
        .getLongBytes(FS_S3A_BLOCK_SIZE, DEFAULT_BLOCKSIZE);
    this.pageSize = pageSize;
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
   * @param path path to the object
   * @param key key of the object.
   */
  private void queueToDelete(Path path, String key) {
    pathsToDelete.add(path);
    keysToDelete.add(new DeleteObjectsRequest.KeyVersion(key));
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
        keysToDelete,
        pathsToDelete);
    // now reset the lists.
    keysToDelete.clear();
    pathsToDelete.clear();
  }

  @Retries.RetryMixed
  public Long execute() throws IOException {
    executeOnlyOnce();
    final StoreContext storeContext = getStoreContext();
    final MetadataStore metadataStore = checkNotNull(
        storeContext.getMetadataStore(),
        "No metadata store in context");

    // Validation completed: time to begin the operation.
    // The store-specific rename tracker is used to keep the store
    // to date with the in-progress operation.
    // for the null store, these are all no-ops.
    renameTracker = metadataStore.initiateRenameOperation(
        storeContext,
        sourcePath, sourceStatus, destPath);


    // Ok! Time to start
    try {
      if (sourceStatus.isFile()) {
        renameFileToDest();
      } else {
        recursiveDirectoryRename();
      }
    } catch (AmazonClientException | IOException ex) {
      // rename failed.
      // block for all ongoing copies to complete, successfully or not
      try {
        completeActiveCopies("failure handling");
      } catch (IOException e) {
        // a failure to update the metastore after a rename failure is what
        // we'd see on a network problem, expired credentials and other
        // unrecoverable errors.
        // Downgrading to warn because an exception is already
        // about to be thrown.
        LOG.warn("While completing all active copies", e);
      }
      // notify the rename tracker of the failure
      throw renameTracker.renameFailed(ex);
    }

    // At this point the rename has completed successfully in the S3 store.
    // Tell the metastore this fact and let it complete its changes
    renameTracker.completeRename();

    callbacks.finishRename(sourcePath, destPath);
    return bytesCopied.get();
  }

  /**
   * The source is a file: rename it to the destination.
   * @throws IOException failure
   */
  protected void renameFileToDest() throws IOException {
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
    copySourceAndUpdateTracker(
        sourcePath,
        sourceKey,
        sourceAttributes,
        readContext,
        copyDestinationPath,
        copyDestinationKey,
        false);
    bytesCopied.addAndGet(sourceStatus.getLen());
    // delete the source
    callbacks.deleteObjectAtPath(sourcePath, sourceKey, true, null);
    // and update the tracker
    renameTracker.sourceObjectsDeleted(Lists.newArrayList(sourcePath));
  }

  /**
   * Execute a full recursive rename.
   * The source is a file: rename it to the destination.
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

    if (destStatus != null
        && destStatus.isEmptyDirectory() == Tristate.TRUE) {
      // delete unnecessary fake directory at the destination.
      // this MUST be done before anything else so that
      // rollback code doesn't get confused and insert a tombstone
      // marker.
      LOG.debug("Deleting fake directory marker at destination {}",
          destStatus.getPath());
      callbacks.deleteObjectAtPath(destStatus.getPath(), dstKey, false, null);
    }

    Path parentPath = storeContext.keyToPath(srcKey);
    final RemoteIterator<S3ALocatedFileStatus> iterator =
        callbacks.listFilesAndEmptyDirectories(parentPath,
            sourceStatus,
            true,
            true);
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
      Path childSourcePath = storeContext.keyToPath(key);

      // mark for deletion on a successful copy.
      queueToDelete(childSourcePath, key);

      // the destination key is that of the key under the source tree,
      // remapped under the new destination path.
      String newDestKey =
          dstKey + key.substring(srcKey.length());
      Path childDestPath = storeContext.keyToPath(newDestKey);

      // now begin the single copy
      CompletableFuture<Path> copy = initiateCopy(child, key,
          childSourcePath, newDestKey, childDestPath);
      activeCopies.add(copy);
      bytesCopied.addAndGet(sourceStatus.getLen());

      if (activeCopies.size() == RENAME_PARALLEL_LIMIT) {
        // the limit of active copies has been reached;
        // wait for completion or errors to surface.
        LOG.debug("Waiting for active copies to complete");
        completeActiveCopies("batch threshold reached");
      }
      if (keysToDelete.size() == pageSize) {
        // finish ongoing copies then delete all queued keys.
        // provided the parallel limit is a factor of the max entry
        // constant, this will not need to block for the copy, and
        // simply jump straight to the delete.
        completeActiveCopiesAndDeleteSources("paged delete");
      }
    } // end of iteration through the list

    // await the final set of copies and their deletion
    // This will notify the renameTracker that these objects
    // have been deleted.
    completeActiveCopiesAndDeleteSources("final copy and delete");

    // We moved all the children, now move the top-level dir
    // Empty directory should have been added as the object summary
    renameTracker.moveSourceDirectory();
  }

  /**
   * Initiate a copy operation in the executor.
   * @param source status of the source object.
   * @param key source key
   * @param childSourcePath source as a path.
   * @param newDestKey destination key
   * @param childDestPath destination path.
   * @return the future.
   */
  protected CompletableFuture<Path> initiateCopy(
      final S3ALocatedFileStatus source,
      final String key,
      final Path childSourcePath,
      final String newDestKey,
      final Path childDestPath) {
    S3ObjectAttributes sourceAttributes =
        callbacks.createObjectAttributes(
            source.getPath(),
            source.getETag(),
            source.getVersionId(),
            source.getLen());
    // queue the copy operation for execution in the thread pool
    return submit(getStoreContext().getExecutor(), () ->
        copySourceAndUpdateTracker(
            childSourcePath,
            key,
            sourceAttributes,
            callbacks.createReadContext(source),
            childDestPath,
            newDestKey,
            true));
  }

  /**
   * This invoked to copy a file or directory marker then update the
   * rename operation on success.
   * It may be called in its own thread.
   * @param sourceFile source path of the copy; may have a trailing / on it.
   * @param srcKey source key
   * @param srcAttributes status of the source object
   * @param destination destination as a qualified path.
   * @param destinationKey destination key
   * @param addAncestors should ancestors be added to the metastore?
   * @return the destination path.
   * @throws IOException failure
   */
  @Retries.RetryTranslated
  private Path copySourceAndUpdateTracker(
      final Path sourceFile,
      final String srcKey,
      final S3ObjectAttributes srcAttributes,
      final S3AReadOpContext readContext,
      final Path destination,
      final String destinationKey,
      final boolean addAncestors) throws IOException {
    long len = srcAttributes.getLen();
    CopyResult copyResult;
    try (DurationInfo ignored = new DurationInfo(LOG, false,
        "Copy file from %s to %s (length=%d)", srcKey, destinationKey, len)) {
      copyResult = callbacks.copyFile(srcKey, destinationKey,
          srcAttributes, readContext);
    }
    if (objectRepresentsDirectory(srcKey, len)) {
      renameTracker.directoryMarkerCopied(
          sourceFile,
          destination,
          addAncestors);
    } else {
      S3ObjectAttributes destAttributes = new S3ObjectAttributes(
          destination,
          copyResult,
          srcAttributes.getServerSideEncryptionAlgorithm(),
          srcAttributes.getServerSideEncryptionKey(),
          len);
      renameTracker.fileCopied(
          sourceFile,
          srcAttributes,
          destAttributes,
          destination,
          blocksize,
          addAncestors);
    }
    return destination;
  }

  /**
   * Remove source objects and update the metastore by way of
   * the rename tracker.
   * @param keys list of keys to delete
   * @param paths list of paths matching the keys to delete 1:1.
   * @throws IOException failure
   */
  @Retries.RetryTranslated
  private void removeSourceObjects(
      final List<DeleteObjectsRequest.KeyVersion> keys,
      final List<Path> paths)
      throws IOException {
    List<Path> undeletedObjects = new ArrayList<>();
    try {
      // remove the keys
      // this will update the metastore on a failure, but on
      // a successful operation leaves the store as is.
      callbacks.removeKeys(
          keys,
          false,
          undeletedObjects,
          renameTracker.getOperationState(),
          true);
      // and clear the list.
    } catch (AmazonClientException | IOException e) {
      // Failed.
      // Notify the rename operation.
      // removeKeys will have already purged the metastore of
      // all keys it has known to delete; this is just a final
      // bit of housekeeping and a chance to tune exception
      // reporting.
      // The returned IOE is rethrown.
      throw renameTracker.deleteFailed(e, paths, undeletedObjects);
    }
    renameTracker.sourceObjectsDeleted(paths);
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

}
