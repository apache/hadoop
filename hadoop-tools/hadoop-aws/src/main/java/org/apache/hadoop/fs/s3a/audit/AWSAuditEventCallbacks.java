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

package org.apache.hadoop.fs.s3a.audit;

import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;


/**
 * Callbacks for audit spans. This is implemented
 * in the span manager as well as individual audit spans.
 * If any of the code in a callback raises an InterruptedException,
 * it must be caught and {@code Thread.interrupt()} called to
 * redeclare the thread as interrupted. The AWS SDK will
 * detect this and raise an exception.
 *
 * Look at the documentation for
 * {@code ExecutionInterceptor} for details
 * on the callbacks.
 */
public interface AWSAuditEventCallbacks extends ExecutionInterceptor {

  /**
   * Return a span ID which must be unique for all spans within
   * everywhere. That effectively means part of the
   * span SHOULD be derived from a UUID.
   * Callers MUST NOT make any assumptions about the actual
   * contents or structure of this string other than the
   * uniqueness.
   * @return a non-empty string
   */
  String getSpanId();

  /**
   * Get the name of the operation.
   * @return the operation name.
   */
  String getOperationName();

  /**
   * Callback when a request is created in the S3A code.
   * This is called in {@code RequestFactoryImpl} after
   * each request is created.
   * It is not invoked on any AWS requests created in the SDK.
   * Avoid raising exceptions or talking to any remote service;
   * this callback is for annotation rather than validation.
   * @param builder the request builder.
   */
  default void requestCreated(SdkRequest.Builder builder) {}

}
