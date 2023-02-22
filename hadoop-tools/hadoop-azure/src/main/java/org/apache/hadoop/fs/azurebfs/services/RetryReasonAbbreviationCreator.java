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

package org.apache.hadoop.fs.azurebfs.services;

import java.util.Locale;

/**
 * Interface to be implemented by each enum in {@link RetryReason}. The methods
 * of the interface define if the given enum can be applied on the given server
 * response.
 * */
public interface RetryReasonAbbreviationCreator {

  /**
   * Returns an abbreviation if the {@link RetryReason} enum can be applied on
   * the server response.
   * @param ex exception captured in the server API call.
   * @param statusCode statusCode on the server response
   * @param serverErrorMessage serverErrorMessage on the server response.
   * @return <ol><li>null if the enum can not be used on the server response</li>
   * <li>abbreviation corresponding to the server response.</li></ol>
   * */
  String capturableAndGetAbbreviation(Exception ex,
      Integer statusCode,
      String serverErrorMessage);

  default String buildFromExceptionMessage(final Exception exceptionCaptured,
      final String search,
      final String result) {
    if (exceptionCaptured != null
        && exceptionCaptured.getMessage() != null
        && exceptionCaptured.getMessage().toLowerCase(Locale.US).contains(search.toLowerCase(Locale.US))) {
      return result;
    }
    return null;
  }
}
