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

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.azurebfs.services.retryReasonAbbreviationCreation.ClientErrorRetryReason;
import org.apache.hadoop.fs.azurebfs.services.retryReasonAbbreviationCreation.ConnectionResetRetryReason;
import org.apache.hadoop.fs.azurebfs.services.retryReasonAbbreviationCreation.ConnectionTimeoutRetryReason;
import org.apache.hadoop.fs.azurebfs.services.retryReasonAbbreviationCreation.ReadTimeoutRetryReason;
import org.apache.hadoop.fs.azurebfs.services.retryReasonAbbreviationCreation.RetryReasonAbbreviationCreator;
import org.apache.hadoop.fs.azurebfs.services.retryReasonAbbreviationCreation.ServerErrorRetryReason;
import org.apache.hadoop.fs.azurebfs.services.retryReasonAbbreviationCreation.UnknownHostRetryReason;
import org.apache.hadoop.fs.azurebfs.services.retryReasonAbbreviationCreation.UnknownIOExceptionRetryReason;
import org.apache.hadoop.fs.azurebfs.services.retryReasonAbbreviationCreation.UnknownSocketExceptionRetryReason;


/**
 * In case of retry, this enum would give the information on the reason for
 * previous API call.
 * */
public class RetryReason {

  private static List<RetryReasonAbbreviationCreator> rankedReasons
      = new LinkedList<RetryReasonAbbreviationCreator>() {{
    add(new ServerErrorRetryReason());
    add(new ClientErrorRetryReason());
    add(new UnknownIOExceptionRetryReason());
    add(new UnknownSocketExceptionRetryReason());
    add(new ConnectionTimeoutRetryReason());
    add(new ReadTimeoutRetryReason());
    add(new UnknownHostRetryReason());
    add(new ConnectionResetRetryReason());
  }};

  static String getAbbreviation(Exception ex,
      Integer statusCode,
      String storageErrorMessage) {
    String result = null;

    for (RetryReasonAbbreviationCreator retryReasonAbbreviationCreator : rankedReasons) {
      final String abbreviation
          = retryReasonAbbreviationCreator.captureAndGetAbbreviation(ex,
          statusCode, storageErrorMessage);
      if (abbreviation != null) {
        result = abbreviation;
      }
    }
    return result;
  }
}
