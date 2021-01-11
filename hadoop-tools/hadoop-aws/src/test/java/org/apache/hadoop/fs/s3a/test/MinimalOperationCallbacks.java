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

package org.apache.hadoop.fs.s3a.test;

import java.io.IOException;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.transfer.model.CopyResult;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.impl.OperationCallbacks;
import org.apache.hadoop.fs.s3a.s3guard.BulkOperationState;

/**
 * Stub implementation of {@link OperationCallbacks}.
 */
public class MinimalOperationCallbacks
    implements OperationCallbacks {

  @Override
  public S3ObjectAttributes createObjectAttributes(
      Path path,
      String eTag,
      String versionId,
      long len) {
    return null;
  }

  @Override
  public S3ObjectAttributes createObjectAttributes(
      S3AFileStatus fileStatus) {
    return null;
  }

  @Override
  public S3AReadOpContext createReadContext(
      FileStatus fileStatus) {
    return null;
  }

  @Override
  public void finishRename(
      Path sourceRenamed,
      Path destCreated)
      throws IOException {

  }

  @Override
  public void deleteObjectAtPath(
      Path path,
      String key,
      boolean isFile,
      BulkOperationState operationState)
      throws IOException {

  }

  @Override
  public RemoteIterator<S3ALocatedFileStatus> listFilesAndDirectoryMarkers(
      final Path path,
      final S3AFileStatus status,
      final boolean collectTombstones,
      final boolean includeSelf) throws IOException {
    return null;
  }

  @Override
  public CopyResult copyFile(
      String srcKey,
      String destKey,
      S3ObjectAttributes srcAttributes,
      S3AReadOpContext readContext)
      throws IOException {
    return null;
  }

  @Override
  public DeleteObjectsResult removeKeys(
      List<DeleteObjectsRequest.KeyVersion> keysToDelete,
      boolean deleteFakeDir,
      List<Path> undeletedObjectsOnFailure,
      BulkOperationState operationState,
      boolean quiet)
      throws MultiObjectDeleteException, AmazonClientException,
             IOException {
    return null;
  }

  @Override
  public boolean allowAuthoritative(Path p) {
    return false;
  }

  @Override
  public RemoteIterator<S3AFileStatus> listObjects(
      Path path,
      String key)
      throws IOException {
    return null;
  }
}
