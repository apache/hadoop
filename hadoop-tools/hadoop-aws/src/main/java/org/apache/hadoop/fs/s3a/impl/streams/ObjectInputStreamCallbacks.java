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

package org.apache.hadoop.fs.s3a.impl.streams;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.util.functional.CallableRaisingIOE;

/**
 * Callbacks for reading object data from the S3 Store.
 */
public interface ObjectInputStreamCallbacks extends Closeable {

  /**
   * Create a GET request builder.
   * @param key object key
   * @return the request builder
   */
  GetObjectRequest.Builder newGetRequestBuilder(String key);

  /**
   * Execute the request.
   * When CSE is enabled with reading of unencrypted data, The object is checked if it is
   * encrypted and if so, the request is made with encrypted S3 client. If the object is
   * not encrypted, the request is made with unencrypted s3 client.
   * @param request the request
   * @return the response
   * @throws IOException on any failure.
   */
  @Retries.OnceRaw
  ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest request) throws IOException;

  /**
   * Submit some asynchronous work, for example, draining a stream.
   * @param operation operation to invoke
   * @param <T> return type
   * @return a future.
   */
  <T> CompletableFuture<T> submit(CallableRaisingIOE<T> operation);

}
