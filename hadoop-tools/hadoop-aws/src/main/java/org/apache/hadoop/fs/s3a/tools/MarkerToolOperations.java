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

package org.apache.hadoop.fs.s3a.tools;

import java.io.IOException;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;

import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.s3guard.BulkOperationState;

/**
 * Operations which must be offered by the store for {@link MarkerTool}.
 * These are a proper subset of {@code OperationCallbacks}; this interface
 * strips down those provided to the tool.
 */
public interface MarkerToolOperations {

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

}
