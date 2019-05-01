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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * The base class for a rename operation.
 */
public abstract class RenameOperation implements Closeable {


  /** source path. */
  private final Path sourceRoot;

  /** destination path. */
  private final Path dest;

  private final String owner;

  /**
   * constructor.
   * @param sourceRoot source path.
   * @param dest destination path.
   * @param owner
   */

  public RenameOperation(
      final Path sourceRoot,
      final Path dest,
      final String owner) {
    this.sourceRoot = sourceRoot;
    this.dest = dest;
    this.owner = owner;
  }

  public Path getSourceRoot() {
    return sourceRoot;
  }

  public Path getDest() {
    return dest;
  }

  public String getOwner() {
    return owner;
  }

  @Override
  public void close() throws IOException {

  }

  /**
   * A file has been copied.
   *
   * @param childSource
   * @param sourceStatus status of source.
   * @param destPath destination path.
   * @param blockSize block size.
   * @param addAncestors should ancestors be added?
   * @throws IOException failure.
   */
  public abstract void fileCopied(
      final Path childSource, FileStatus sourceStatus,
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
  public abstract void directoryMarkerCopied(
      FileStatus sourceStatus,
      Path destPath,
      boolean addAncestors) throws IOException;

  /**
   * The delete failed.
   * By the time this is called, the metastore will already have
   * been updated with the results of any partial delete failure,
   * such that all files known to have been deleted will have been
   * removed.
   * @param undeletedObjects list of objects which were not deleted.
   */
  public void deleteFailed(
      final List<DeleteObjectsRequest.KeyVersion> keysToDelete,
      final List<Path> undeletedObjects) {

  }

  /**
   * Top level directory move.
   * @throws IOException on failure
   */
  public void noteSourceDirectoryMoved() throws IOException {

  }

  /**
   * Note that source objects have been deleted.
   * The metastore will already have been updated.
   * @param keys keys of objects deleted.
   */
  public void sourceObjectsDeleted(
      final List<DeleteObjectsRequest.KeyVersion> keys) throws IOException {
  }


  /**
   * Complete the operation.
   * @throws IOException failure.
   */
  public abstract void complete() throws IOException ;

  /**
   * Rename has failed.
   * The metastore now needs to be updated with its current state
   * even though the operation is incomplete.
   * @throws IOException failure.
   */
  public IOException renameFailed(Exception ex) throws IOException {


    return null;
  }
}
