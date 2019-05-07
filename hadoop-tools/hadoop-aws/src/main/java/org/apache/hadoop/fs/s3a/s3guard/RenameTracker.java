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
import java.util.List;

import com.amazonaws.SdkBaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.fs.s3a.impl.StoreOperation;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.fs.s3a.S3AUtils.translateException;

/**
 * A class which manages updating the metastore with the rename process
 * as initiated in the S3AFilesystem rename.
 * Subclasses must provide an implementation and return it in
 * {@link MetadataStore#initiateRenameOperation(StoreContext, Path, FileStatus, Path)}.
 */
public abstract class RenameTracker extends StoreOperation {

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
   * constructor.
   * @param name tracker name for logs.
   * @param storeContext store context.
   * @param sourceRoot source path.
   * @param dest destination path.
   */
  protected RenameTracker(
      final String name,
      final StoreContext storeContext,
      final Path sourceRoot,
      final Path dest) {
    super(storeContext);
    this.sourceRoot = sourceRoot;
    this.dest = dest;
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

  /**
   * A file has been copied.
   *
   * @param childSource source of the file. This may actually be different
   * from the path of the sourceStatus.
   * @param sourceStatus status of source.
   * @param destPath destination path.
   * @param blockSize block size.
   * @param addAncestors should ancestors be added?
   * @throws IOException failure.
   */
  public abstract void fileCopied(
      Path childSource,
      FileStatus sourceStatus,
      Path destPath,
      long blockSize,
      boolean addAncestors) throws IOException;

  /**
   * A directory marker has been copied.
   * @param sourceStatus status of source.
   * @param destPath destination path.
   * @param addAncestors should ancestors be added?
   * @throws IOException failure.
   */
  public void directoryMarkerCopied(
      FileStatus sourceStatus,
      Path destPath,
      boolean addAncestors) throws IOException {

  }

  /**
   * The delete failed.
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
   * @throws IOException on failure
   */
  public void moveSourceDirectory() throws IOException {

  }

  /**
   * Note that source objects have been deleted.
   * The metastore will already have been updated.
   * @param paths keys of objects deleted.
   */
  public void sourceObjectsDeleted(
      final List<Path> paths) throws IOException {
  }

  /**
   * Complete the operation.
   * @throws IOException failure.
   */
  public void completeRename() throws IOException {
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
   * The metastore now needs to be updated with its current state
   * even though the operation is incomplete.
   * Implementations MUST NOT throw exceptions here, as this is going to
   * be invoked in an exception handler.
   * catch and log or catch and return/wrap.
   *
   * The base implementation returns the IOE passed in and translates
   * any AWS exception into an IOE.
   * @param ex the exception which caused the failure.
   * This is either an IOException or and AWS exception
   * @return an IOException to throw in an exception.
   */
  public IOException renameFailed(Exception ex) {
    LOG.debug("Rename has failed", ex);
    noteRenameFinished();
    return convertToIOException(ex);
  }

  /**
   * Convert a passed in exception (expected to be an IOE or AWS exception
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
      // should never happen, but for strictness
      return new IOException(ex);
    }
  }
}
