/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.gs;

import io.grpc.Status;

/**
 * Implementation for {@link ErrorTypeExtractor} for exception specifically thrown from gRPC path.
 */
final class ErrorTypeExtractor {

  enum ErrorType {
    NOT_FOUND, OUT_OF_RANGE, ALREADY_EXISTS, FAILED_PRECONDITION, INTERNAL, RESOURCE_EXHAUSTED,
    UNAVAILABLE, UNKNOWN
  }

  //  public static final ErrorTypeExtractor INSTANCE = new ErrorTypeExtractor();

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
      return ErrorType.UNKNOWN;
    }
  }
}
