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

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;

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
   * @return a context for read operations.
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
   * Delete an object.
   * This call does <i>not</i> create any mock parent entries.
   * Retry policy: retry untranslated; delete considered idempotent.
   * @param path path to delete
   * @param key key of entry
   * @param isFile is the path a file (used for instrumentation only)
   * @throws IOException from invoker signature only -should not be raised.
   */
  @Retries.RetryTranslated
  void deleteObjectAtPath(Path path,
      String key,
      boolean isFile)
      throws IOException;

  /**
   * Recursive list of files and directory markers.
   *
   * @param path path to list from
   * @param status optional status of path to list.
   * @param includeSelf should the listing include this path if present?
   * @return an iterator.
   * @throws IOException failure
   */
  @Retries.RetryTranslated
  RemoteIterator<S3ALocatedFileStatus> listFilesAndDirectoryMarkers(
      Path path,
      S3AFileStatus status,
      boolean includeSelf) throws IOException;

  /**
   * Copy a single object in the bucket via a COPY operation.
   * There's no update of metadata, directory markers, etc.
   * Callers must implement.
   * @param srcKey source object path
   * @param destKey destination object path
   * @param srcAttributes S3 attributes of the source object
   * @param readContext the read context
   * @return the result of the copy
   * @throws InterruptedIOException the operation was interrupted
   * @throws IOException Other IO problems
   */
  @Retries.RetryTranslated
  CopyObjectResponse copyFile(String srcKey,
      String destKey,
      S3ObjectAttributes srcAttributes,
      S3AReadOpContext readContext)
      throws IOException;

  /**
   * Remove keys from the store.
   * @param keysToDelete collection of keys to delete on the s3-backend.
   *        if empty, no request is made of the object store.
   * @param deleteFakeDir indicates whether this is for deleting fake dirs.
   * @throws InvalidRequestException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   * @throws MultiObjectDeleteException one or more of the keys could not
   * be deleted in a multiple object delete operation.
   * @throws AwsServiceException amazon-layer failure.
   * @throws IOException other IO Exception.
   */
  @Retries.RetryRaw
  void removeKeys(
          List<ObjectIdentifier> keysToDelete,
          boolean deleteFakeDir)
      throws MultiObjectDeleteException, AwsServiceException,
      IOException;

  /**
   * Create an iterator over objects in S3.
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

  /**
   * Abort multipart uploads under a path; paged.
   * @param prefix prefix for uploads to abort
   * @return a count of aborts
   * @throws IOException trouble; FileNotFoundExceptions are swallowed.
   */
  @Retries.RetryTranslated
  default long abortMultipartUploadsUnderPrefix(String prefix)
      throws IOException {
    return 0;
  }
}
