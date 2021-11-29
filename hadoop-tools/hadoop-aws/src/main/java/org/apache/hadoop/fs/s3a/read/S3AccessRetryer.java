/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.s3a.read;

import org.apache.hadoop.fs.common.Validate;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Encapsulates retry related functionality when accessing S3.
 */
public class S3AccessRetryer {
  private static final Logger LOG = LoggerFactory.getLogger(S3AccessRetryer.class);

  // Default value of delay before first retry.
  public static final int RETRY_BASE_DELAY_MS = 500;

  // Default value of maximum delay after which no additional retries will be attempted.
  public static final int RETRY_MAX_BACKOFF_TIME_MS = 5000;

  private int retryDelay;
  private int retryMaxDelay;

  public S3AccessRetryer() {
    this(RETRY_BASE_DELAY_MS, RETRY_MAX_BACKOFF_TIME_MS);
  }

  /**
   * Constructs an instance of {@link S3AccessRetryer}.
   *
   * @param baseDelay the delay before attempting the first retry.
   * @param maxDelay the amount of delay after which no additional retries will be attempted.
   */
  public S3AccessRetryer(int baseDelay, int maxDelay) {
    Validate.checkPositiveInteger(baseDelay, "baseDelay");
    Validate.checkGreater(maxDelay, "maxDelay", baseDelay, "baseDelay");

    this.retryDelay = baseDelay;
    this.retryMaxDelay = maxDelay;
  }

  /**
   * Determines whether the caller should retry an S3 access attempt.
   * If a retry is needed, performs a {@code Thread.sleep()} before
   * returning true; otherwise, returns false immediately.
   *
   * @param e the exception encountered by the caller.
   */
  public boolean retry(Exception e) {
    if (isRetryable(e) && (this.retryDelay <= this.retryMaxDelay)) {
      try {
        Thread.sleep(this.retryDelay);
      } catch (InterruptedException ie) {
        return false;
      }
      this.retryDelay *= 2;
      return true;
    }

    return false;
  }

  /**
   * Determines whether the current exception is retryable.
   */
  private boolean isRetryable(Exception e) {
    if (e instanceof IOException) {
      // All IOException instances are retryable.
      return true;
    } else if (e instanceof AmazonS3Exception) {
      // An AmazonS3Exception may be retryable if it originated from the server side.
      // At present, we retry only one specific case of server side exception (code 503).
      AmazonS3Exception s3e = (AmazonS3Exception) e;
      boolean result = (s3e.getErrorType() == AmazonServiceException.ErrorType.Service)
          && (s3e.getStatusCode() == 503);
      if (result) {
        logAdditionalInfo(s3e);
      }
      return result;
    } else {
      return false;
    }
  }

  private void logAdditionalInfo(AmazonS3Exception e) {
    StringBuilder sb = new StringBuilder();
    sb.append(e.toString());
    if (e.getAdditionalDetails() != null) {
      sb.append("\n");
      for (Map.Entry entry : e.getAdditionalDetails().entrySet()) {
        sb.append(String.format("%s = %s\n", entry.getKey(), entry.getValue()));
      }
    }
    LOG.warn(sb.toString());
  }
}
