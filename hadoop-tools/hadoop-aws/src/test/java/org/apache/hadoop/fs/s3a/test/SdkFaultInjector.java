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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import org.apache.hadoop.conf.Configuration;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.audit.AuditTestSupport.enableLoggingAuditor;
import static org.apache.hadoop.fs.s3a.audit.AuditTestSupport.resetAuditOptions;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_EXECUTION_INTERCEPTORS;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_500_INTERNAL_SERVER_ERROR;

/**
 * This runs inside the AWS execution pipeline so can insert faults and so
 * trigger recovery in the SDK.
 * It is wired up through the auditor mechanism.
 * <p>
 * This uses the evaluator function {@link #evaluator} to determine if
 * the request type is that for which failures are targeted;
 * When there is a match then the failure count
 * is decremented and, if the count is still positive, an error is raised with the
 * error code defined in {@link #FAILURE_STATUS_CODE}.
 * This happens <i>after</i> the request has already succeeded against the S3 store:
 * whatever was requested has actually already happened.
 */
public final class SdkFaultInjector implements ExecutionInterceptor {

  private static final Logger LOG =
      LoggerFactory.getLogger(SdkFaultInjector.class);

  private static final AtomicInteger FAILURE_STATUS_CODE =
      new AtomicInteger(SC_500_INTERNAL_SERVER_ERROR);

  /**
   * Always allow requests.
   */
  public static final Function<Context.ModifyHttpResponse, Boolean>
      ALWAYS_ALLOW = (c) -> false;

  /**
   * How many requests with the matching evaluator to fail on.
   */
  public static final AtomicInteger REQUEST_FAILURE_COUNT = new AtomicInteger(1);

  /**
   * Evaluator for responses.
   */
  private static Function<Context.ModifyHttpResponse, Boolean> evaluator = ALWAYS_ALLOW;


  /**
   * Action to take on failure.
   */
  private static BiFunction<SdkRequest, SdkHttpResponse, SdkHttpResponse>
      action = SdkFaultInjector::patchStatusCode;

  /**
   * Update the value of {@link #FAILURE_STATUS_CODE}.
   * @param value new value
   */
  public static void setFailureStatusCode(int value) {
    FAILURE_STATUS_CODE.set(value);
  }


  /**
   * Set the evaluator function used to determine whether or not to raise
   * an exception.
   * @param value new evaluator.
   */
  public static void setEvaluator(Function<Context.ModifyHttpResponse, Boolean> value) {
    evaluator = value;
  }


  /**
   * Reset fault injection.
   * The evaluator will enable everything;
   * the failure action is set to
   * {@link #patchStatusCode(SdkRequest, SdkHttpResponse)}.
   */
  public static void resetFaultInjector() {
    setEvaluator(ALWAYS_ALLOW);
    setAction(SdkFaultInjector::patchStatusCode);
  }

  /**
   * Set the failure count.
   * @param count failure count
   */
  public static void setRequestFailureCount(int count) {
    LOG.debug("Failure count set to {}", count);
    REQUEST_FAILURE_COUNT.set(count);
  }

  /**
   * Set up the request failure conditions.
   * @param attempts how many times to fail before succeeding
   * @param condition condition to trigger the failure
   */
  public static void setRequestFailureConditions(final int attempts,
      final Function<Context.ModifyHttpResponse, Boolean> condition) {
    setRequestFailureCount(attempts);
    setEvaluator(condition);
  }

  /**
   * Set the action to invoke.
   * @param action new action.
   */
  public static void setAction(BiFunction<SdkRequest, SdkHttpResponse, SdkHttpResponse> action) {
    SdkFaultInjector.action = requireNonNull(action);
  }

  /**
   * Is the response being processed from a GET request?
   * @param context request context.
   * @return true if the request is of the right type.
   */
  public static boolean isGetRequest(final Context.ModifyHttpResponse context) {
    return context.httpRequest().method().equals(SdkHttpMethod.GET);
  }

  /**
   * Is the response being processed from a PUT request?
   * @param context request context.
   * @return true if the request is of the right type.
   */
  public static boolean isPutRequest(final Context.ModifyHttpResponse context) {
    return context.httpRequest().method().equals(SdkHttpMethod.PUT);
  }

  /**
   * Is the response being processed from any POST request?
   * @param context request context.
   * @return true if the request is of the right type.
   */
  public static boolean isPostRequest(final Context.ModifyHttpResponse context) {
    return context.httpRequest().method().equals(SdkHttpMethod.POST);
  }

  /**
   * Is the request a commit completion request?
   * @param context response
   * @return true if the predicate matches
   */
  public static boolean isCompleteMultipartUploadRequest(
      final Context.ModifyHttpResponse context) {
    return context.request() instanceof CompleteMultipartUploadRequest;
  }

  /**
   * Is the request a part upload?
   * @param context response
   * @return true if the predicate matches
   */
  public static boolean isPartUpload(final Context.ModifyHttpResponse context) {
    return context.request() instanceof UploadPartRequest;
  }
  /**
   * Is the request a multipart upload abort?
   * @param context response
   * @return true if the predicate matches
   */
  public static boolean isMultipartAbort(final Context.ModifyHttpResponse context) {
    return context.request() instanceof AbortMultipartUploadRequest;
  }



  /**
   * Review response from S3 and optionall modify its status code.
   * @return the original response or a copy with a different status code.
   */
  @Override
  public SdkHttpResponse modifyHttpResponse(final Context.ModifyHttpResponse context,
      final ExecutionAttributes executionAttributes) {
    SdkRequest request = context.request();
    SdkHttpResponse httpResponse = context.httpResponse();
    if (evaluator.apply(context) && shouldFail()) {

      return action.apply(request, httpResponse);

    } else {
      // pass unchanged
      return httpResponse;
    }
  }

  /**
   * The default fault injector: patch the status code with the value in
   * {@link #FAILURE_STATUS_CODE}.
   * @param request original request
   * @param httpResponse ongoing response
   * @return modified response.
   */
  public static SdkHttpResponse patchStatusCode(final SdkRequest request,
      final SdkHttpResponse httpResponse) {
    // fail the request
    final int code = FAILURE_STATUS_CODE.get();
    LOG.info("Fault Injector returning {} error code for request {}",
        code, request);

    return httpResponse.copy(b -> {
      b.statusCode(code);
    });
  }

  /**
   * Should the request fail based on the failure count?
   * @return true if the request count means a request must fail
   */
  private static boolean shouldFail() {
    return REQUEST_FAILURE_COUNT.decrementAndGet() > 0;
  }

  /**
   * Add fault injection.
   * This wires up auditing as needed.
   * @param conf configuration to patch
   * @return patched configuration
   */
  public static Configuration addFaultInjection(Configuration conf) {
    resetAuditOptions(conf);
    enableLoggingAuditor(conf);
    // use the fault injector
    conf.set(AUDIT_EXECUTION_INTERCEPTORS, SdkFaultInjector.class.getName());
    return conf;
  }
}
