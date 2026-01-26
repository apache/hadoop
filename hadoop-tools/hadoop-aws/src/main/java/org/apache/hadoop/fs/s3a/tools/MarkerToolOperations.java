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

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteException;


/**
 * Operations which must be offered by the store for {@link MarkerTool}.
 * These are a proper subset of {@code OperationCallbacks}; this interface
 * strips down those provided to the tool.
 */
public interface MarkerToolOperations {

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
   * Remove keys from the store.
   * @param keysToDelete collection of keys to delete on the s3-backend.
   *        if empty, no request is made of the object store.
   * @param deleteFakeDir indicates whether this is for deleting fake dirs.
   * all deleted keys
   * @throws InvalidRequestException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   * @throws MultiObjectDeleteException one or more of the keys could not
   * be deleted in a multiple object delete operation.
   * @throws AwsServiceException amazon-layer failure.
   * @throws IOException other IO Exception.
   */
  @Retries.RetryMixed
  void removeKeys(
      List<ObjectIdentifier> keysToDelete,
      boolean deleteFakeDir)
      throws MultiObjectDeleteException, AwsServiceException,
             IOException;

}
