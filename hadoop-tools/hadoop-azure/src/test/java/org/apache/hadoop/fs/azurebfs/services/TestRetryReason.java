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

import org.junit.jupiter.api.Test;

import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.EGRESS_OVER_ACCOUNT_LIMIT;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.INGRESS_OVER_ACCOUNT_LIMIT;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.TPS_OVER_ACCOUNT_LIMIT;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_RESET_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_RESET_MESSAGE;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_TIMEOUT_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.CONNECTION_TIMEOUT_JDK_MESSAGE;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.EGRESS_LIMIT_BREACH_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.INGRESS_LIMIT_BREACH_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.IO_EXCEPTION_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.TPS_LIMIT_BREACH_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.READ_TIMEOUT_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.READ_TIMEOUT_JDK_MESSAGE;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.SOCKET_EXCEPTION_ABBREVIATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryReasonConstants.UNKNOWN_HOST_EXCEPTION_ABBREVIATION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRetryReason {

  @Test
  public void test4xxStatusRetryReason() {
    assertThat(RetryReason.getAbbreviation(null, HTTP_FORBIDDEN, null))
        .describedAs("Abbreviation for 4xx should be equal to 4xx")
        .isEqualTo(HTTP_FORBIDDEN + "");
  }

  @Test
  public void testConnectionResetRetryReason() {
    SocketException connReset = new SocketException(CONNECTION_RESET_MESSAGE.toUpperCase());
    assertThat(RetryReason.getAbbreviation(connReset, null, null)).isEqualTo(CONNECTION_RESET_ABBREVIATION);
  }

  @Test
  public void testConnectionTimeoutRetryReason() {
    SocketTimeoutException connectionTimeoutException = new SocketTimeoutException(CONNECTION_TIMEOUT_JDK_MESSAGE);
    assertThat(RetryReason.getAbbreviation(connectionTimeoutException, null, null)).isEqualTo(
        CONNECTION_TIMEOUT_ABBREVIATION
    );
  }

  @Test
  public void testReadTimeoutRetryReason() {
    SocketTimeoutException connectionTimeoutException = new SocketTimeoutException(READ_TIMEOUT_JDK_MESSAGE);
    assertThat(RetryReason.getAbbreviation(connectionTimeoutException, null, null)).isEqualTo(
        READ_TIMEOUT_ABBREVIATION
    );
  }

  @Test
  public void testEgressLimitRetryReason() {
    assertThat(RetryReason.getAbbreviation(null, HTTP_UNAVAILABLE, EGRESS_OVER_ACCOUNT_LIMIT.getErrorMessage())).isEqualTo(
        EGRESS_LIMIT_BREACH_ABBREVIATION
    );
  }

  @Test
  public void testIngressLimitRetryReason() {
    assertThat(RetryReason.getAbbreviation(null, HTTP_UNAVAILABLE, INGRESS_OVER_ACCOUNT_LIMIT.getErrorMessage())).isEqualTo(
        INGRESS_LIMIT_BREACH_ABBREVIATION
    );
  }

  @Test
  public void testOperationLimitRetryReason() {
    assertThat(RetryReason.getAbbreviation(null, HTTP_UNAVAILABLE, TPS_OVER_ACCOUNT_LIMIT.getErrorMessage())).isEqualTo(
        TPS_LIMIT_BREACH_ABBREVIATION
    );
  }

  @Test
  public void test503UnknownRetryReason() {
    assertThat(RetryReason.getAbbreviation(null, HTTP_UNAVAILABLE, null)).isEqualTo(
        "503"
    );
  }

  @Test
  public void test500RetryReason() {
    assertThat(RetryReason.getAbbreviation(null, HTTP_INTERNAL_ERROR, null)).isEqualTo(
        "500"
    );
  }

  @Test
  public void testUnknownHostRetryReason() {
    assertThat(RetryReason.getAbbreviation(new UnknownHostException(), null, null)).isEqualTo(
        UNKNOWN_HOST_EXCEPTION_ABBREVIATION
    );
  }

  @Test
  public void testUnknownIOExceptionRetryReason() {
    assertThat(RetryReason.getAbbreviation(new IOException(), null, null)).isEqualTo(
        IO_EXCEPTION_ABBREVIATION
    );
  }

  @Test
  public void testUnknownSocketException() {
    assertThat(RetryReason.getAbbreviation(new SocketException(), null, null)).isEqualTo(
        SOCKET_EXCEPTION_ABBREVIATION
    );
  }
}
