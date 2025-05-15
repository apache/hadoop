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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import org.apache.hadoop.fs.s3a.S3AStore;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectInputStreamCallbacks;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.util.functional.CallableRaisingIOE;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.util.LambdaUtils.eval;

/**
 * Callbacks for object stream operations.
 */
public class InputStreamCallbacksImpl implements ObjectInputStreamCallbacks {

  private static final Logger LOG = LoggerFactory.getLogger(InputStreamCallbacksImpl.class);

  /**
   * Audit span to activate before each call.
   */
  private final AuditSpan auditSpan;

  /**
   * store operations.
   */
  private final S3AStore store;

  /**
   * crypto FS operations.
   */
  private final S3AFileSystemOperations fsOperations;

  /**
   * A (restricted) thread pool for asynchronous operations.
   */
  private final ThreadPoolExecutor threadPool;

  /**
   * Create.
   * @param auditSpan Audit span to activate before each call.
   * @param store store operations
   * @param fsOperations crypto FS operations.
   * @param threadPool thread pool for async operations.
   */
  public InputStreamCallbacksImpl(
      final AuditSpan auditSpan,
      final S3AStore store,
      final S3AFileSystemOperations fsOperations,
      final ThreadPoolExecutor threadPool) {
    this.auditSpan = requireNonNull(auditSpan);
    this.store = requireNonNull(store);
    this.fsOperations = requireNonNull(fsOperations);
    this.threadPool = requireNonNull(threadPool);
  }

  /**
   * Closes the audit span.
   */
  @Override
  public void close()  {
    auditSpan.close();
  }

  @Override
  public GetObjectRequest.Builder newGetRequestBuilder(final String key) {
    // active the audit span used for the operation
    try (AuditSpan span = auditSpan.activate()) {
      return store.getRequestFactory().newGetObjectRequestBuilder(key);
    }
  }

  @Override
  public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest request) throws
      IOException {
    // active the audit span used for the operation
    try (AuditSpan span = auditSpan.activate()) {
      return fsOperations.getObject(store, request, store.getRequestFactory());
    }
  }

  @Override
  public <T> CompletableFuture<T> submit(final CallableRaisingIOE<T> operation) {
    CompletableFuture<T> result = new CompletableFuture<>();
    threadPool.submit(() ->
        eval(result, () -> {
          LOG.debug("Starting submitted operation in {}", auditSpan.getSpanId());
          try (AuditSpan span = auditSpan.activate()) {
            return operation.apply();
          } finally {
            LOG.debug("Completed submitted operation in {}", auditSpan.getSpanId());
          }
        }));
    return result;
  }
}
