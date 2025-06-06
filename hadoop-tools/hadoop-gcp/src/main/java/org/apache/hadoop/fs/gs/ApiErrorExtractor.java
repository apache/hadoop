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

package org.apache.hadoop.fs.gs;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonError.ErrorInfo;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpStatusCodes;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Translates exceptions from API calls into higher-level meaning, while allowing injectability for
 * testing how API errors are handled.
 */
class ApiErrorExtractor {

  /** Singleton instance of the ApiErrorExtractor. */
  public static final ApiErrorExtractor INSTANCE = new ApiErrorExtractor();

  public static final int STATUS_CODE_RANGE_NOT_SATISFIABLE = 416;

  public static final String GLOBAL_DOMAIN = "global";
  public static final String USAGE_LIMITS_DOMAIN = "usageLimits";

  public static final String RATE_LIMITED_REASON = "rateLimitExceeded";
  public static final String USER_RATE_LIMITED_REASON = "userRateLimitExceeded";

  public static final String QUOTA_EXCEEDED_REASON = "quotaExceeded";

  // These come with "The account for ... has been disabled" message.
  public static final String ACCOUNT_DISABLED_REASON = "accountDisabled";

  // These come with "Project marked for deletion" message.
  public static final String ACCESS_NOT_CONFIGURED_REASON = "accessNotConfigured";

  // These are 400 error codes with "resource 'xyz' is not ready" message.
  // These sometimes happens when create operation is still in-flight but resource
  // representation is already available via get call.
  public static final String RESOURCE_NOT_READY_REASON = "resourceNotReady";

  // HTTP 413 with message "Value for field 'foo' is too large".
  public static final String FIELD_SIZE_TOO_LARGE_REASON = "fieldSizeTooLarge";

  // HTTP 400 message for 'USER_PROJECT_MISSING' error.
  public static final String USER_PROJECT_MISSING_MESSAGE =
      "Bucket is a requester pays bucket but no user project provided.";

  // The debugInfo field present on Errors collection in GoogleJsonException
  // as an unknown key.
  private static final String DEBUG_INFO_FIELD = "debugInfo";

  /**
   * Determines if the given exception indicates intermittent request failure or failure caused by
   * user error.
   */
  public boolean requestFailure(IOException e) {
    HttpResponseException httpException = getHttpResponseException(e);
    return httpException != null
        && (accessDenied(httpException)
            || badRequest(httpException)
            || internalServerError(httpException)
            || rateLimited(httpException)
            || IoExceptionHelper.isSocketError(httpException)
            || unauthorized(httpException));
  }

  /**
   * Determines if the given exception indicates 'access denied'. Recursively checks getCause() if
   * outer exception isn't an instance of the correct class.
   *
   * <p>Warning: this method only checks for access denied status code, however this may include
   * potentially recoverable reason codes such as rate limiting. For alternative, see {@link
   * #accessDeniedNonRecoverable(IOException)}.
   */
  public boolean accessDenied(IOException e) {
    return recursiveCheckForCode(e, HttpStatusCodes.STATUS_CODE_FORBIDDEN);
  }

  /** Determines if the given exception indicates bad request. */
  public boolean badRequest(IOException e) {
    return recursiveCheckForCode(e, HttpStatusCodes.STATUS_CODE_BAD_REQUEST);
  }

  /**
   * Determines if the given exception indicates the request was unauthenticated. This can be caused
   * by attaching invalid credentials to a request.
   */
  public boolean unauthorized(IOException e) {
    return recursiveCheckForCode(e, HttpStatusCodes.STATUS_CODE_UNAUTHORIZED);
  }

  /**
   * Determines if the exception is a non-recoverable access denied code (such as account closed or
   * marked for deletion).
   */
  public boolean accessDeniedNonRecoverable(IOException e) {
    ErrorInfo errorInfo = getErrorInfo(e);
    String reason = errorInfo != null ? errorInfo.getReason() : null;
    return ACCOUNT_DISABLED_REASON.equals(reason) || ACCESS_NOT_CONFIGURED_REASON.equals(reason);
  }

  /** Determines if the exception is a client error. */
  public boolean clientError(IOException e) {
    HttpResponseException httpException = getHttpResponseException(e);
    return httpException != null && getHttpStatusCode(httpException) / 100 == 4;
  }

  /** Determines if the exception is an internal server error. */
  public boolean internalServerError(IOException e) {
    HttpResponseException httpException = getHttpResponseException(e);
    return httpException != null && getHttpStatusCode(httpException) / 100 == 5;
  }

  /**
   * Determines if the given exception indicates 'item already exists'. Recursively checks
   * getCause() if outer exception isn't an instance of the correct class.
   */
  public boolean itemAlreadyExists(IOException e) {
    return recursiveCheckForCode(e, HttpStatusCodes.STATUS_CODE_CONFLICT);
  }

  /**
   * Determines if the given exception indicates 'item not found'. Recursively checks getCause() if
   * outer exception isn't an instance of the correct class.
   */
  public boolean itemNotFound(IOException e) {
    return recursiveCheckForCode(e, HttpStatusCodes.STATUS_CODE_NOT_FOUND);
  }

  /**
   * Determines if the given exception indicates 'field size too large'. Recursively checks
   * getCause() if outer exception isn't an instance of the correct class.
   */
  public boolean fieldSizeTooLarge(IOException e) {
    ErrorInfo errorInfo = getErrorInfo(e);
    return errorInfo != null && FIELD_SIZE_TOO_LARGE_REASON.equals(errorInfo.getReason());
  }

  /**
   * Determines if the given exception indicates 'resource not ready'. Recursively checks getCause()
   * if outer exception isn't an instance of the correct class.
   */
  public boolean resourceNotReady(IOException e) {
    ErrorInfo errorInfo = getErrorInfo(e);
    return errorInfo != null && RESOURCE_NOT_READY_REASON.equals(errorInfo.getReason());
  }

  /**
   * Determines if the given IOException indicates 'precondition not met' Recursively checks
   * getCause() if outer exception isn't an instance of the correct class.
   */
  public boolean preconditionNotMet(IOException e) {
    return recursiveCheckForCode(e, HttpStatusCodes.STATUS_CODE_PRECONDITION_FAILED);
  }

  /**
   * Determines if the given exception indicates 'range not satisfiable'. Recursively checks
   * getCause() if outer exception isn't an instance of the correct class.
   */
  public boolean rangeNotSatisfiable(IOException e) {
    return recursiveCheckForCode(e, STATUS_CODE_RANGE_NOT_SATISFIABLE);
  }

  /**
   * Determines if a given Throwable is caused by a rate limit being applied. Recursively checks
   * getCause() if outer exception isn't an instance of the correct class.
   *
   * @param e The Throwable to check.
   * @return True if the Throwable is a result of rate limiting being applied.
   */
  public boolean rateLimited(IOException e) {
    ErrorInfo errorInfo = getErrorInfo(e);
    if (errorInfo != null) {
      String domain = errorInfo.getDomain();
      boolean isRateLimitedOrGlobalDomain =
          USAGE_LIMITS_DOMAIN.equals(domain) || GLOBAL_DOMAIN.equals(domain);
      String reason = errorInfo.getReason();
      boolean isRateLimitedReason =
          RATE_LIMITED_REASON.equals(reason) || USER_RATE_LIMITED_REASON.equals(reason);
      return isRateLimitedOrGlobalDomain && isRateLimitedReason;
    }
    return false;
  }

  /**
   * Determines if a given Throwable is caused by Quota Exceeded. Recursively checks getCause() if
   * outer exception isn't an instance of the correct class.
   */
  public boolean quotaExceeded(IOException e) {
    ErrorInfo errorInfo = getErrorInfo(e);
    return errorInfo != null && QUOTA_EXCEEDED_REASON.equals(errorInfo.getReason());
  }

  /**
   * Determines if the given exception indicates that 'userProject' is missing in request.
   * Recursively checks getCause() if outer exception isn't an instance of the correct class.
   */
  public boolean userProjectMissing(IOException e) {
    GoogleJsonError jsonError = getJsonError(e);
    return jsonError != null
        && jsonError.getCode() == HttpStatusCodes.STATUS_CODE_BAD_REQUEST
        && USER_PROJECT_MISSING_MESSAGE.equals(jsonError.getMessage());
  }

  /** Extracts the error message. */
  public String getErrorMessage(IOException e) {
    // Prefer to use message from GJRE.
    GoogleJsonError jsonError = getJsonError(e);
    return jsonError == null ? e.getMessage() : jsonError.getMessage();
  }

  /**
   * Converts the exception to a user-presentable error message. Specifically, extracts message
   * field for HTTP 4xx codes, and creates a generic "Internal Server Error" for HTTP 5xx codes.
   *
   * @param e the exception
   * @param action the description of the action being performed at the time of error.
   * @see #toUserPresentableMessage(IOException, String)
   */
  public IOException toUserPresentableException(IOException e, String action) throws IOException {
    throw new IOException(toUserPresentableMessage(e, action), e);
  }

  /**
   * Converts the exception to a user-presentable error message. Specifically, extracts message
   * field for HTTP 4xx codes, and creates a generic "Internal Server Error" for HTTP 5xx codes.
   */
  public String toUserPresentableMessage(IOException e, @Nullable String action) {
    String message = "Internal server error";
    if (clientError(e)) {
      message = getErrorMessage(e);
    }
    return action == null
        ? message
        : String.format("Encountered an error while %s: %s", action, message);
  }

  /** See {@link #toUserPresentableMessage(IOException, String)}. */
  public String toUserPresentableMessage(IOException e) {
    return toUserPresentableMessage(e, null);
  }

  @Nullable
  public String getDebugInfo(IOException e) {
    ErrorInfo errorInfo = getErrorInfo(e);
    return errorInfo != null ? (String) errorInfo.getUnknownKeys().get(DEBUG_INFO_FIELD) : null;
  }

  /**
   * Returns HTTP status code from the given exception.
   *
   * <p>Note: GoogleJsonResponseException.getStatusCode() method is marked final therefore it cannot
   * be mocked using Mockito. We use this helper so that we can override it in tests.
   */
  protected int getHttpStatusCode(HttpResponseException e) {
    return e.getStatusCode();
  }

  /**
   * Get the first ErrorInfo from an IOException if it is an instance of
   * GoogleJsonResponseException, otherwise return null.
   */
  @Nullable
  protected ErrorInfo getErrorInfo(IOException e) {
    GoogleJsonError jsonError = getJsonError(e);
    List<ErrorInfo> errors = jsonError != null ? jsonError.getErrors() : ImmutableList.of();
    return errors != null ? Iterables.getFirst(errors, null) : null;
  }

  /** If the exception is a GoogleJsonResponseException, get the error details, else return null. */
  @Nullable
  protected GoogleJsonError getJsonError(IOException e) {
    GoogleJsonResponseException jsonException = getJsonResponseException(e);
    return jsonException == null ? null : jsonException.getDetails();
  }

  /** Recursively checks getCause() if outer exception isn't an instance of the correct class. */
  protected boolean recursiveCheckForCode(IOException e, int code) {
    HttpResponseException httpException = getHttpResponseException(e);
    return httpException != null && getHttpStatusCode(httpException) == code;
  }

  @Nullable
  public static GoogleJsonResponseException getJsonResponseException(Throwable throwable) {
    Throwable cause = throwable;
    while (cause != null) {
      if (cause instanceof GoogleJsonResponseException) {
        return (GoogleJsonResponseException) cause;
      }
      cause = cause.getCause();
    }
    return null;
  }

  @Nullable
  public static HttpResponseException getHttpResponseException(Throwable throwable) {
    Throwable cause = throwable;
    while (cause != null) {
      if (cause instanceof HttpResponseException) {
        return (HttpResponseException) cause;
      }
      cause = cause.getCause();
    }
    return null;
  }
}
