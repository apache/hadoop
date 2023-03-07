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

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.EGRESS_OVER_ACCOUNT_LIMIT;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.INGRESS_OVER_ACCOUNT_LIMIT;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_RESET_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_RESET_MESSAGE;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_TIMEOUT_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_TIMEOUT_JDK_MESSAGE;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.EGRESS_LIMIT_BREACH_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.INGRESS_LIMIT_BREACH_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.IO_EXCEPTION_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.OPERATION_BREACH_MESSAGE;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.OPERATION_LIMIT_BREACH_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.READ_TIMEOUT_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.READ_TIMEOUT_JDK_MESSAGE;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.SOCKET_EXCEPTION_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.UNKNOWN_HOST_EXCEPTION_ABBREVIATION;

public class TestRetryReason {

  @Test
  public void test4xxStatusRetryReason() {
    Assertions.assertThat(RetryReason.getAbbreviation(null, HTTP_FORBIDDEN, null))
        .describedAs("Abbreviation for 4xx should be equal to 4xx")
        .isEqualTo(HTTP_FORBIDDEN + "");
  }

  @Test
  public void testConnectionResetRetryReason() {
    SocketException connReset = new SocketException(CONNECTION_RESET_MESSAGE.toUpperCase());
    Assertions.assertThat(RetryReason.getAbbreviation(connReset, null, null)).isEqualTo(CONNECTION_RESET_ABBREVIATION);
  }

  @Test
  public void testConnectionTimeoutRetryReason() {
    SocketTimeoutException connectionTimeoutException = new SocketTimeoutException(CONNECTION_TIMEOUT_JDK_MESSAGE);
    Assertions.assertThat(RetryReason.getAbbreviation(connectionTimeoutException, null, null)).isEqualTo(
        CONNECTION_TIMEOUT_ABBREVIATION
    );
  }

  @Test
  public void testReadTimeoutRetryReason() {
    SocketTimeoutException connectionTimeoutException = new SocketTimeoutException(READ_TIMEOUT_JDK_MESSAGE);
    Assertions.assertThat(RetryReason.getAbbreviation(connectionTimeoutException, null, null)).isEqualTo(
        READ_TIMEOUT_ABBREVIATION
    );
  }

  @Test
  public void testEgressLimitRetryReason() {
    Assertions.assertThat(RetryReason.getAbbreviation(null, HTTP_UNAVAILABLE, EGRESS_OVER_ACCOUNT_LIMIT.getErrorMessage())).isEqualTo(
        EGRESS_LIMIT_BREACH_ABBREVIATION
    );
  }

  @Test
  public void testIngressLimitRetryReason() {
    Assertions.assertThat(RetryReason.getAbbreviation(null, HTTP_UNAVAILABLE, INGRESS_OVER_ACCOUNT_LIMIT.getErrorMessage())).isEqualTo(
        INGRESS_LIMIT_BREACH_ABBREVIATION
    );
  }

  @Test
  public void testOperationLimitRetryReason() {
    Assertions.assertThat(RetryReason.getAbbreviation(null, HTTP_UNAVAILABLE, OPERATION_BREACH_MESSAGE)).isEqualTo(
        OPERATION_LIMIT_BREACH_ABBREVIATION
    );
  }

  @Test
  public void test503UnknownRetryReason() {
    Assertions.assertThat(RetryReason.getAbbreviation(null, HTTP_UNAVAILABLE, null)).isEqualTo(
        "503"
    );
  }

  @Test
  public void test500RetryReason() {
    Assertions.assertThat(RetryReason.getAbbreviation(null, HTTP_INTERNAL_ERROR, null)).isEqualTo(
        "500"
    );
  }

  @Test
  public void testUnknownHostRetryReason() {
    Assertions.assertThat(RetryReason.getAbbreviation(new UnknownHostException(), null, null)).isEqualTo(
        UNKNOWN_HOST_EXCEPTION_ABBREVIATION
    );
  }

  @Test
  public void testUnknownIOExceptionRetryReason() {
    Assertions.assertThat(RetryReason.getAbbreviation(new IOException(), null, null)).isEqualTo(
        IO_EXCEPTION_ABBREVIATION
    );
  }

  @Test
  public void testUnknownSocketException() {
    Assertions.assertThat(RetryReason.getAbbreviation(new SocketException(), null, null)).isEqualTo(
        SOCKET_EXCEPTION_ABBREVIATION
    );
  }
}
