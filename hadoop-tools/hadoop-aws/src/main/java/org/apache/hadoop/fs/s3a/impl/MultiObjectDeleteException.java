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
import java.nio.file.AccessDeniedException;
import java.util.List;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.s3a.AWSS3IOException;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_200_OK;

/**
 * Exception raised in {@link S3AFileSystem#deleteObjects} when
 * one or more of the keys could not be deleted.
 *
 * Used to reproduce the behaviour of SDK v1 for partial failures
 * on DeleteObjects. In SDK v2, the errors are returned as part of
 * the response objects.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MultiObjectDeleteException extends S3Exception {

  private static final Logger LOG = LoggerFactory.getLogger(
      MultiObjectDeleteException.class);

  /**
   * This is the exception exit code if access was denied on a delete.
   * {@value}.
   */
  public static final String ACCESS_DENIED = "AccessDenied";

  /**
   * Field value for the superclass builder: {@value}.
   */
  private static final int STATUS_CODE = SC_200_OK;

  /**
   * Field value for the superclass builder: {@value}.
   */
  private static final String ERROR_CODE = "MultiObjectDeleteException";

  /**
   * Field value for the superclass builder: {@value}.
   */
  private static final String SERVICE_NAME = "Amazon S3";

  /**
   * Extracted error list.
   */
  private final List<S3Error> errors;

  public MultiObjectDeleteException(List<S3Error> errors) {
    super(builder()
        .message(errors.toString())
        .awsErrorDetails(
            AwsErrorDetails.builder()
                .errorCode(ERROR_CODE)
                .errorMessage(ERROR_CODE)
                .serviceName(SERVICE_NAME)
                .sdkHttpResponse(SdkHttpResponse.builder()
                    .statusCode(STATUS_CODE)
                    .build())
                .build())
        .statusCode(STATUS_CODE));
    this.errors = errors;
  }

  public List<S3Error> errors() {
    return errors;
  }

  /**
   * A {@code MultiObjectDeleteException} is raised if one or more
   * paths listed in a bulk DELETE operation failed.
   * The top-level exception is therefore just "something wasn't deleted",
   * but doesn't include the what or the why.
   * This translation will extract an AccessDeniedException if that's one of
   * the causes, otherwise grabs the status code and uses it in the
   * returned exception.
   * @param message text for the exception
   * @return an IOE with more detail.
   */
  public IOException translateException(final String message) {
    LOG.info("Bulk delete operation failed to delete all objects;"
            + " failure count = {}",
        errors().size());
    final StringBuilder result = new StringBuilder(
        errors().size() * 256);
    result.append(message).append(": ");
    String exitCode = "";
    for (S3Error error : errors()) {
      String code = error.code();
      String item = String.format("%s: %s%s: %s%n", code, error.key(),
          (error.versionId() != null
              ? (" (" + error.versionId() + ")")
              : ""),
          error.message());
      LOG.info(item);
      result.append(item);
      if (exitCode == null || exitCode.isEmpty() || ACCESS_DENIED.equals(code)) {
        exitCode = code;
      }
    }
    if (ACCESS_DENIED.equals(exitCode)) {
      return (IOException) new AccessDeniedException(result.toString())
          .initCause(this);
    } else {
      return new AWSS3IOException(result.toString(), this);
    }
  }
}
