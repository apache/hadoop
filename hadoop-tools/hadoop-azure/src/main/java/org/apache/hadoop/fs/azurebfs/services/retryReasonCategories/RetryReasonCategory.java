/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services.retryReasonCategories;

import java.util.Locale;

/**
 * Provides methods to define if given exception can be categorised to certain category.
 * Each category has a different implementation of the abstract class.
 */
public abstract class RetryReasonCategory {

  /**
   * Returns if given server response error can be categorised by the implementation.
   *
   * @param ex exception captured in the server response.
   * @param statusCode statusCode on the server response
   * @param serverErrorMessage serverErrorMessage on the server response.
   *
   * @return <ol><li>true if server response error can be categorised by the implementation</li>
   * <li>false if response error can not be categorised by the implementation</li></ol>
   */
  abstract Boolean canCapture(Exception ex,
      Integer statusCode,
      String serverErrorMessage);

  /**
   * Returns the abbreviation corresponding to the server response error.
   *
   * @param statusCode statusCode on the server response
   * @param serverErrorMessage serverErrorMessage on the server response.
   *
   * @return abbreviation on the basis of the statusCode and the serverErrorMessage
   */
  abstract String getAbbreviation(Integer statusCode, String serverErrorMessage);

  /**
   * Converts the server-error response to an abbreviation if the response can be
   * categorised by the implementation.
   *
   * @param ex exception received while making API request
   * @param statusCode statusCode received in the server-response
   * @param serverErrorMessage error-message received in the server-response
   *
   * @return abbreviation if the server-response can be categorised by the implementation.
   * null if the server-response can not be categorised by the implementation.
   */
  public String captureAndGetAbbreviation(Exception ex,
      Integer statusCode,
      String serverErrorMessage) {
    if (canCapture(ex, statusCode, serverErrorMessage)) {
      return getAbbreviation(statusCode, serverErrorMessage);
    }
    return null;
  }

  /**
   * Checks if a required search-string is in the exception's message.
   */
  Boolean checkExceptionMessage(final Exception exceptionCaptured,
      final String search) {
    if (search == null) {
      return false;
    }
    if (exceptionCaptured != null
        && exceptionCaptured.getMessage() != null
        && exceptionCaptured.getMessage()
        .toLowerCase(Locale.US)
        .contains(search.toLowerCase(Locale.US))) {
      return true;
    }
    return false;
  }
}
