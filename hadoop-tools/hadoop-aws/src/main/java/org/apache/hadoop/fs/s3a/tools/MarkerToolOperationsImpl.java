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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.impl.OperationCallbacks;
import org.apache.hadoop.fs.s3a.s3guard.BulkOperationState;

/**
 * Implement the marker tool operations by forwarding to the
 * {@link OperationCallbacks} instance provided in the constructor.
 */
public class MarkerToolOperationsImpl implements MarkerToolOperations {

  private final OperationCallbacks operationCallbacks;

  /**
   * Constructor.
   * @param operations implementation of the operations
   */
  public MarkerToolOperationsImpl(final OperationCallbacks operations) {
    this.operationCallbacks = operations;
  }

  @Override
  public RemoteIterator<S3AFileStatus> listObjects(final Path path,
      final String key)
      throws IOException {
    return operationCallbacks.listObjects(path, key);
  }

  @Override
  public DeleteObjectsResult removeKeys(
      final List<DeleteObjectsRequest.KeyVersion> keysToDelete,
      final boolean deleteFakeDir,
      final List<Path> undeletedObjectsOnFailure,
      final BulkOperationState operationState,
      final boolean quiet)
      throws MultiObjectDeleteException, AmazonClientException, IOException {
    return operationCallbacks.removeKeys(keysToDelete, deleteFakeDir,
        undeletedObjectsOnFailure, operationState, quiet);
  }

}
