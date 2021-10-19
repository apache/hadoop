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
import java.io.InterruptedIOException;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.transfer.model.CopyResult;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.s3guard.BulkOperationState;

/**
 * These are all the callbacks which the {@link RenameOperation}
 * and {@link DeleteOperation } operations need,
 * derived from the appropriate S3AFileSystem methods.
 */
public interface OperationCallbacks {

  /**
   * Create the attributes of an object for subsequent use.
   * @param path path path of the request.
   * @param eTag the eTag of the S3 object
   * @param versionId S3 object version ID
   * @param len length of the file
   * @return attributes to use when building the query.
   */
  S3ObjectAttributes createObjectAttributes(
      Path path,
      String eTag,
      String versionId,
      long len);

  /**
   * Create the attributes of an object for subsequent use.
   * @param fileStatus file status to build from.
   * @return attributes to use when building the query.
   */
  S3ObjectAttributes createObjectAttributes(
      S3AFileStatus fileStatus);

  /**
   * Create the read context for reading from the referenced file,
   * using FS state as well as the status.
   * @param fileStatus file status.
   * @return a context for read and select operations.
   */
  S3AReadOpContext createReadContext(
      FileStatus fileStatus);

  /**
   * The rename has finished; perform any store cleanup operations
   * such as creating/deleting directory markers.
   * @param sourceRenamed renamed source
   * @param destCreated destination file created.
   * @throws IOException failure
   */
  void finishRename(Path sourceRenamed, Path destCreated) throws IOException;

  /**
   * Delete an object, also updating the metastore.
   * This call does <i>not</i> create any mock parent entries.
   * Retry policy: retry untranslated; delete considered idempotent.
   * @param path path to delete
   * @param key key of entry
   * @param isFile is the path a file (used for instrumentation only)
   * @param operationState (nullable) operational state for a bulk update
   * @throws AmazonClientException problems working with S3
   * @throws IOException IO failure in the metastore
   */
  @Retries.RetryTranslated
  void deleteObjectAtPath(Path path,
      String key,
      boolean isFile,
      BulkOperationState operationState)
      throws IOException;

  /**
   * Recursive list of files and directory markers.
   *
   * @param path path to list from
   * @param status optional status of path to list.
   * @param collectTombstones should tombstones be collected from S3Guard?
   * @param includeSelf should the listing include this path if present?
   * @return an iterator.
   * @throws IOException failure
   */
  @Retries.RetryTranslated
  RemoteIterator<S3ALocatedFileStatus> listFilesAndDirectoryMarkers(
      Path path,
      S3AFileStatus status,
      boolean collectTombstones,
      boolean includeSelf) throws IOException;

  /**
   * Copy a single object in the bucket via a COPY operation.
   * There's no update of metadata, directory markers, etc.
   * Callers must implement.
   * @param srcKey source object path
   * @param srcAttributes S3 attributes of the source object
   * @param readContext the read context
   * @return the result of the copy
   * @throws InterruptedIOException the operation was interrupted
   * @throws IOException Other IO problems
   */
  @Retries.RetryTranslated
  CopyResult copyFile(String srcKey,
      String destKey,
      S3ObjectAttributes srcAttributes,
      S3AReadOpContext readContext)
      throws IOException;

  /**
   * Remove keys from the store, updating the metastore on a
   * partial delete represented as a MultiObjectDeleteException failure by
   * deleting all those entries successfully deleted and then rethrowing
   * the MultiObjectDeleteException.
   * @param keysToDelete collection of keys to delete on the s3-backend.
   *        if empty, no request is made of the object store.
   * @param deleteFakeDir indicates whether this is for deleting fake dirs.
   * @param undeletedObjectsOnFailure List which will be built up of all
   * files that were not deleted. This happens even as an exception
   * is raised.
   * @param operationState bulk operation state
   * @param quiet should a bulk query be quiet, or should its result list
   * all deleted keys
   * @return the deletion result if a multi object delete was invoked
   * and it returned without a failure, else null.
   * @throws InvalidRequestException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   * @throws MultiObjectDeleteException one or more of the keys could not
   * be deleted in a multiple object delete operation.
   * @throws AmazonClientException amazon-layer failure.
   * @throws IOException other IO Exception.
   */
  @Retries.RetryMixed
  DeleteObjectsResult removeKeys(
      List<DeleteObjectsRequest.KeyVersion> keysToDelete,
      boolean deleteFakeDir,
      List<Path> undeletedObjectsOnFailure,
      BulkOperationState operationState,
      boolean quiet)
      throws MultiObjectDeleteException, AmazonClientException,
      IOException;

  /**
   * Is the path for this instance considered authoritative on the client,
   * that is: will listing/status operations only be handled by the metastore,
   * with no fallback to S3.
   * @param p path
   * @return true iff the path is authoritative on the client.
   */
  boolean allowAuthoritative(Path p);

  /**
   * Create an iterator over objects in S3 only; S3Guard
   * is not involved.
   * The listing includes the key itself, if found.
   * @param path  path of the listing.
   * @param key object key
   * @return iterator with the first listing completed.
   * @throws IOException failure.
   */
  @Retries.RetryTranslated
  RemoteIterator<S3AFileStatus> listObjects(
      Path path,
      String key)
      throws IOException;
}
