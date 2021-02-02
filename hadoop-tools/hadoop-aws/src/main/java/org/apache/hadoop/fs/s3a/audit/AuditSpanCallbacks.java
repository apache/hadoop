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

import org.apache.hadoop.fs.s3a.Retries;

/**
 * Callbacks for audit spans. This is implemented
 * in the span manager as well as individual audit spans.
 * If any of the code in a callback raises an InterruptedException,
 * it must be caught and {@code Thread.interrupt()} called to
 * redeclare the thread as interrupted. The AWS SDK will
 * detect this and raise an exception.
 */
public interface AuditSpanCallbacks {

  /**
   * Callback when a request is created in the S3A code.
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
      throws AuditFailureException, SdkBaseException{
  }

}
