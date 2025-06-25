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

import javax.annotation.Nullable;

import com.google.api.client.http.HttpStatusCodes;
import com.google.cloud.storage.StorageException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Implementation for {@link ErrorTypeExtractor} for exception specifically thrown from gRPC path.
 */
final class ErrorTypeExtractor {

  static boolean bucketAlreadyExists(Exception e) {
    ErrorType errorType = getErrorType(e);
    if (errorType == ErrorType.ALREADY_EXISTS) {
      return true;
    } else if (errorType == ErrorType.FAILED_PRECONDITION) {
      // The gRPC API currently throws a FAILED_PRECONDITION status code instead of ALREADY_EXISTS,
      // so we handle both these conditions in the interim.
      StatusRuntimeException statusRuntimeException = getStatusRuntimeException(e);
      return statusRuntimeException != null
          && BUCKET_ALREADY_EXISTS_MESSAGE.equals(statusRuntimeException.getMessage());
    }
    return false;
  }

  @Nullable
  static private StatusRuntimeException getStatusRuntimeException(Exception e) {
    Throwable cause = e;
    // Keeping a counter to break early from the loop to avoid infinite loop condition due to
    // cyclic exception chains.
    int currentExceptionDepth = 0, maxChainDepth = 1000;
    while (cause != null && currentExceptionDepth < maxChainDepth) {
      if (cause instanceof StatusRuntimeException) {
        return (StatusRuntimeException) cause;
      }
      cause = cause.getCause();
      currentExceptionDepth++;
    }
    return null;
  }

  enum ErrorType {
    NOT_FOUND, OUT_OF_RANGE, ALREADY_EXISTS, FAILED_PRECONDITION, INTERNAL, RESOURCE_EXHAUSTED,
    UNAVAILABLE, UNKNOWN
  }

  private static final String BUCKET_ALREADY_EXISTS_MESSAGE =
      "FAILED_PRECONDITION: Your previous request to create the named bucket succeeded and you "
          + "already own it.";

  private ErrorTypeExtractor() {
  }

  static ErrorType getErrorType(Exception error) {
    switch (Status.fromThrowable(error).getCode()) {
    case NOT_FOUND:
      return ErrorType.NOT_FOUND;
    case OUT_OF_RANGE:
      return ErrorType.OUT_OF_RANGE;
    case ALREADY_EXISTS:
      return ErrorType.ALREADY_EXISTS;
    case FAILED_PRECONDITION:
      return ErrorType.FAILED_PRECONDITION;
    case RESOURCE_EXHAUSTED:
      return ErrorType.RESOURCE_EXHAUSTED;
    case INTERNAL:
      return ErrorType.INTERNAL;
    case UNAVAILABLE:
      return ErrorType.UNAVAILABLE;
    default:
      return getErrorTypeFromStorageException(error);
    }
  }

  private static ErrorType getErrorTypeFromStorageException(Exception error) {
    if (error instanceof StorageException) {
      StorageException se = (StorageException) error;
      int httpCode = se.getCode();

      if (httpCode == HttpStatusCodes.STATUS_CODE_PRECONDITION_FAILED) {
        return ErrorType.FAILED_PRECONDITION;
      }

      if (httpCode == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
        return ErrorType.NOT_FOUND;
      }

      if (httpCode == HttpStatusCodes.STATUS_CODE_SERVICE_UNAVAILABLE) {
        return ErrorType.UNAVAILABLE;
      }
    }

    return ErrorType.UNKNOWN;
  }
}
