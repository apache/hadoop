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

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.amazonaws.SdkBaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.fs.s3a.impl.AbstractStoreOperation;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.S3AUtils.translateException;

/**
 * A class which manages updating the metastore with the rename process
 * as initiated in the S3AFilesystem rename.
 * <p>
 * Subclasses must provide an implementation and return it in
 * {@code MetadataStore.initiateRenameOperation()}.
 * <p>
 * The {@link #operationState} field/constructor argument is an opaque state to
 * be passed down to the metastore in its move operations; this allows the
 * stores to manage ongoing state -while still being able to share
 * rename tracker implementations.
 * <p>
 * This is to avoid performance problems wherein the progressive rename
 * tracker causes the store to repeatedly create and write duplicate
 * ancestor entries for every file added.
 */
public abstract class RenameTracker extends AbstractStoreOperation {

  public static final Logger LOG = LoggerFactory.getLogger(
      RenameTracker.class);

  /** source path. */
  private final Path sourceRoot;

  /** destination path. */
  private final Path dest;

  /**
   * Track the duration of this operation.
   */
  private final DurationInfo durationInfo;

  /**
   * Generated name for strings.
   */
  private final String name;

  /**
   * Any ongoing state supplied to the rename tracker
   * which is to be passed in with each move operation.
   * This must be closed at the end of the tracker's life.
   */
  private final BulkOperationState operationState;

  /**
   * The metadata store for this tracker.
   * Always non-null.
   * <p>
   * This is passed in separate from the store context to guarantee
   * that whichever store creates a tracker is explicitly bound to that
   * instance.
   */
  private final MetadataStore metadataStore;

  /**
   * Constructor.
   * @param name tracker name for logs.
   * @param storeContext store context.
   * @param metadataStore the store
   * @param sourceRoot source path.
   * @param dest destination path.
   * @param operationState ongoing move state.
   */
  protected RenameTracker(
      final String name,
      final StoreContext storeContext,
      final MetadataStore metadataStore,
      final Path sourceRoot,
      final Path dest,
      final BulkOperationState operationState) {
    super(checkNotNull(storeContext));
    checkNotNull(storeContext.getUsername(), "No username");
    this.metadataStore = checkNotNull(metadataStore);
    this.sourceRoot = checkNotNull(sourceRoot);
    this.dest = checkNotNull(dest);
    this.operationState = operationState;
    this.name = String.format("%s (%s, %s)", name, sourceRoot, dest);
    durationInfo = new DurationInfo(LOG, false,
        name +" (%s, %s)", sourceRoot, dest);
  }

  @Override
  public String toString() {
    return name;
  }

  public Path getSourceRoot() {
    return sourceRoot;
  }

  public Path getDest() {
    return dest;
  }

  public String getOwner() {
    return getStoreContext().getUsername();
  }

  public BulkOperationState getOperationState() {
    return operationState;
  }

  /**
   * Get the metadata store.
   * @return a non-null store.
   */
  protected MetadataStore getMetadataStore() {
    return metadataStore;
  }

  /**
   * A file has been copied.
   *
   * @param childSource source of the file. This may actually be different
   * from the path of the sourceAttributes. (HOW?)
   * @param sourceAttributes status of source.
   * @param destAttributes destination attributes
   * @param destPath destination path.
   * @param blockSize block size.
   * @param addAncestors should ancestors be added?
   * @throws IOException failure.
   */
  public abstract void fileCopied(
      Path childSource,
      S3ObjectAttributes sourceAttributes,
      S3ObjectAttributes destAttributes,
      Path destPath,
      long blockSize,
      boolean addAncestors) throws IOException;

  /**
   * A directory marker has been copied.
   * @param sourcePath source path.
   * @param destPath destination path.
   * @param addAncestors should ancestors be added?
   * @throws IOException failure.
   */
  public void directoryMarkerCopied(
      Path sourcePath,
      Path destPath,
      boolean addAncestors) throws IOException {
  }

  /**
   * The delete failed.
   * <p>
   * By the time this is called, the metastore will already have
   * been updated with the results of any partial delete failure,
   * such that all files known to have been deleted will have been
   * removed.
   * @param e exception
   * @param pathsToDelete paths which were to be deleted.
   * @param undeletedObjects list of objects which were not deleted.
   */
  public IOException deleteFailed(
      final Exception e,
      final List<Path> pathsToDelete,
      final List<Path> undeletedObjects) {

    return convertToIOException(e);
  }

  /**
   * Top level directory move.
   * This is invoked after all child entries have been copied
   * @throws IOException on failure
   */
  public void moveSourceDirectory() throws IOException {
  }

  /**
   * Note that source objects have been deleted.
   * The metastore will already have been updated.
   * @param paths path of objects deleted.
   */
  public void sourceObjectsDeleted(
      final Collection<Path> paths) throws IOException {
  }

  /**
   * Complete the operation.
   * @throws IOException failure.
   */
  public void completeRename() throws IOException {
    IOUtils.cleanupWithLogger(LOG, operationState);
    noteRenameFinished();
  }

  /**
   * Note that the rename has finished by closing the duration info;
   * this will log the duration of the operation at debug.
   */
  protected void noteRenameFinished() {
    durationInfo.close();
  }

  /**
   * Rename has failed.
   * <p>
   * The metastore now needs to be updated with its current state
   * even though the operation is incomplete.
   * Implementations MUST NOT throw exceptions here, as this is going to
   * be invoked in an exception handler.
   * catch and log or catch and return/wrap.
   * <p>
   * The base implementation returns the IOE passed in and translates
   * any AWS exception into an IOE.
   * @param ex the exception which caused the failure.
   * This is either an IOException or and AWS exception
   * @return an IOException to throw in an exception.
   */
  public IOException renameFailed(Exception ex) {
    LOG.debug("Rename has failed", ex);
    IOUtils.cleanupWithLogger(LOG, operationState);
    noteRenameFinished();
    return convertToIOException(ex);
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
    } else if (ex instanceof SdkBaseException) {
      return translateException("rename " + sourceRoot + " to " + dest,
          sourceRoot.toString(),
          (SdkBaseException) ex);
    } else {
      // should never happen, but for completeness
      return new IOException(ex);
    }
  }
}
