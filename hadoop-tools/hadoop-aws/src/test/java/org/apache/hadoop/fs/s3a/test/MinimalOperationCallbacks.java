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

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteException;
import org.apache.hadoop.fs.s3a.impl.OperationCallbacks;

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
      boolean isFile)
      throws IOException {

  }

  @Override
  public RemoteIterator<S3ALocatedFileStatus> listFilesAndDirectoryMarkers(
      final Path path,
      final S3AFileStatus status,
      final boolean includeSelf) throws IOException {
    return null;
  }

  @Override
  public CopyObjectResponse copyFile(
      String srcKey,
      String destKey,
      S3ObjectAttributes srcAttributes,
      S3AReadOpContext readContext)
      throws IOException {
    return null;
  }

  @Override
  public void removeKeys(
          List<ObjectIdentifier> keysToDelete,
          boolean deleteFakeDir)
      throws MultiObjectDeleteException, AwsServiceException,
             IOException {
  }

  @Override
  public RemoteIterator<S3AFileStatus> listObjects(
      Path path,
      String key)
      throws IOException {
    return null;
  }
}
