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

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.SdkBaseException;
import com.amazonaws.handlers.HandlerAfterAttemptContext;
import com.amazonaws.handlers.HandlerBeforeAttemptContext;
import com.amazonaws.http.HttpResponse;

import org.apache.hadoop.fs.s3a.Retries;

/**
 * Callbacks for audit spans. This is implemented
 * in the span manager as well as individual audit spans.
 * If any of the code in a callback raises an InterruptedException,
 * it must be caught and {@code Thread.interrupt()} called to
 * redeclare the thread as interrupted. The AWS SDK will
 * detect this and raise an exception.
 *
 * Look at the documentation for
 * {@code com.amazonaws.handlers.IRequestHandler2} for details
 * on the callbacks.
 */
public interface AWSAuditEventCallbacks {

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
   * @param request request request.
   * @param <T> type of request
   * @return the request, possibly modified.
   */
  default <T extends AmazonWebServiceRequest> T requestCreated(T request) {
    return request;
  }

  /**
   * Preflight preparation of AWS request.
   * @param request request
   * @param <T> type of request
   * @return an updated request.
   * @throws AuditFailureException for generic audit failures
   * @throws SdkBaseException for other reasons.
   */
  @Retries.OnceRaw
  default <T extends AmazonWebServiceRequest> T beforeExecution(T request)
      throws AuditFailureException, SdkBaseException {
    return request;
  }

  /**
   * Callback after S3 responded to a request.
   * @param request request
   * @param response response.
   * @throws AuditFailureException for generic audit failures
   * @throws SdkBaseException for other reasons.
   */
  default void afterResponse(Request<?> request,
      Response<?> response)
      throws AuditFailureException, SdkBaseException {
  }

  /**
   * Callback after a request resulted in an error.
   * @param request request
   * @param response response.
   * @param exception exception raised.
   * @throws AuditFailureException for generic audit failures
   * @throws SdkBaseException for other reasons.
   */
  default void afterError(Request<?> request,
      Response<?> response,
      Exception exception)
      throws AuditFailureException, SdkBaseException {
  }

  /**
   * Request before marshalling.
   * @param request request
   * @return possibly modified request.
   */
  default AmazonWebServiceRequest beforeMarshalling(
      AmazonWebServiceRequest request) {
    return request;
  }

  /**
   * Request before marshalling.
   * @param request request
   */
  default void beforeRequest(Request<?> request) {
  }

  /**
   * Before any attempt is made.
   * @param context full context, including the request.
   */
  default void beforeAttempt(HandlerBeforeAttemptContext context) {
  }

  /**
   * After any attempt is made.
   * @param context full context, including the request.
   */
  default void afterAttempt(
      HandlerAfterAttemptContext context) {
  }

  /**
   * Before unmarshalling the response.
   * @param request request made.
   * @param httpResponse response received
   * @return updated response.
   */
  default HttpResponse beforeUnmarshalling(
      final Request<?> request,
      final HttpResponse httpResponse) {
    return httpResponse;
  }
}
