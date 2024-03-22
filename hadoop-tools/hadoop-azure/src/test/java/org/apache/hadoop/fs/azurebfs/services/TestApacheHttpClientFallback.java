/**
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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsTestWithTimeout;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APACHE_IMPL;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.JDK_FALLBACK;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.JDK_IMPL;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_APACHE_HTTP_CLIENT_MAX_IO_EXCEPTION_RETRIES;
import static org.apache.hadoop.fs.azurebfs.services.HttpOperationType.APACHE_HTTP_CLIENT;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;


public class TestApacheHttpClientFallback extends AbstractAbfsTestWithTimeout {

  public TestApacheHttpClientFallback() throws Exception {
    super();
  }

  private TracingContext getSampleTracingContext() {
    String correlationId, fsId;
    TracingHeaderFormat format;
    correlationId = "test-corr-id";
    fsId = "test-filesystem-id";
    format = TracingHeaderFormat.ALL_ID_FORMAT;
    TracingContext tc = Mockito.spy(new TracingContext(correlationId, fsId,
        FSOperationType.TEST_OP, true, format, null));
    Mockito.doAnswer(answer -> {
          answer.callRealMethod();
          HttpOperation op = answer.getArgument(0);
          if (op instanceof AbfsAHCHttpOperation) {
            Assertions.assertThat(tc.getHeader()).endsWith(APACHE_IMPL);
          }
          if (op instanceof AbfsHttpOperation) {
            if (ApacheHttpClientHealthMonitor.usable()) {
              Assertions.assertThat(tc.getHeader()).endsWith(JDK_IMPL);
            } else {
              Assertions.assertThat(tc.getHeader()).endsWith(JDK_FALLBACK);
            }
          }
          return null;
        })
        .when(tc)
        .constructHeader(Mockito.any(HttpOperation.class),
            Mockito.nullable(String.class), Mockito.nullable(String.class));
    return tc;
  }

  @Test
  public void testMultipleFailureLeadToFallback()
      throws Exception {
    TracingContext tc = getSampleTracingContext();
    int[] retryIteration = {0};
    intercept(IOException.class, () -> {
      getMockRestOperation(retryIteration).execute(tc);
    });
    intercept(IOException.class, () -> {
      getMockRestOperation(retryIteration).execute(tc);
    });
  }

  private AbfsRestOperation getMockRestOperation(int[] retryIteration)
      throws IOException {
    AbfsConfiguration configuration = Mockito.mock(AbfsConfiguration.class);
    Mockito.doReturn(APACHE_HTTP_CLIENT)
        .when(configuration)
        .getPreferredHttpOperationType();
    Mockito.doReturn(DEFAULT_APACHE_HTTP_CLIENT_MAX_IO_EXCEPTION_RETRIES)
        .when(configuration)
        .getMaxApacheHttpClientIoExceptions();
    AbfsClient client = Mockito.mock(AbfsClient.class);
    Mockito.doReturn(Mockito.mock(ExponentialRetryPolicy.class))
        .when(client)
        .getExponentialRetryPolicy();

    AbfsRetryPolicy retryPolicy = Mockito.mock(AbfsRetryPolicy.class);
    Mockito.doReturn(retryPolicy)
        .when(client)
        .getRetryPolicy(Mockito.nullable(String.class));

    Mockito.doAnswer(answer -> {
          if (retryIteration[0]
              < DEFAULT_APACHE_HTTP_CLIENT_MAX_IO_EXCEPTION_RETRIES) {
            retryIteration[0]++;
            return true;
          } else {
            return false;
          }
        })
        .when(retryPolicy)
        .shouldRetry(Mockito.anyInt(), Mockito.nullable(Integer.class));

    AbfsThrottlingIntercept abfsThrottlingIntercept = Mockito.mock(
        AbfsThrottlingIntercept.class);
    Mockito.doNothing()
        .when(abfsThrottlingIntercept)
        .updateMetrics(Mockito.any(AbfsRestOperationType.class),
            Mockito.any(HttpOperation.class));
    Mockito.doNothing()
        .when(abfsThrottlingIntercept)
        .sendingRequest(Mockito.any(AbfsRestOperationType.class),
            Mockito.nullable(AbfsCounters.class));
    Mockito.doReturn(abfsThrottlingIntercept).when(client).getIntercept();


    AbfsRestOperation op = Mockito.spy(new AbfsRestOperation(
        AbfsRestOperationType.ReadFile,
        client,
        AbfsHttpConstants.HTTP_METHOD_GET,
        new URL("http://localhost"),
        new ArrayList<>(),
        null,
        configuration,
        "clientId"
    ));

    Mockito.doReturn(null).when(op).getClientLatency();

    Mockito.doReturn(createApacheHttpOp())
        .when(op)
        .createAbfsHttpOperation();
    Mockito.doReturn(createAhcHttpOp())
        .when(op)
        .createAbfsAHCHttpOperation();

    Mockito.doAnswer(answer -> {
      return answer.getArgument(0);
    }).when(op).createNewTracingContext(Mockito.nullable(TracingContext.class));

    Mockito.doNothing()
        .when(op)
        .signRequest(Mockito.any(HttpOperation.class), Mockito.anyInt());

    Mockito.doAnswer(answer -> {
      HttpOperation operation = Mockito.spy(
          (HttpOperation) answer.callRealMethod());
      Assertions.assertThat(operation).isInstanceOf(
          retryIteration[0]
              < DEFAULT_APACHE_HTTP_CLIENT_MAX_IO_EXCEPTION_RETRIES
              ? AbfsAHCHttpOperation.class
              : AbfsHttpOperation.class);
      Mockito.doReturn(HTTP_OK).when(operation).getStatusCode();
      Mockito.doThrow(new IOException("Test Exception"))
          .when(operation)
          .processResponse(Mockito.nullable(byte[].class), Mockito.anyInt(),
              Mockito.anyInt());
      Mockito.doCallRealMethod().when(operation).getTracingContextSuffix();
      return operation;
    }).when(op).createHttpOperation();
    return op;
  }

  private AbfsAHCHttpOperation createAhcHttpOp() {
    AbfsAHCHttpOperation ahcOp = Mockito.mock(AbfsAHCHttpOperation.class);
    Mockito.doCallRealMethod().when(ahcOp).getTracingContextSuffix();
    return ahcOp;
  }

  private AbfsHttpOperation createApacheHttpOp() {
    AbfsHttpOperation httpOperationMock = Mockito.mock(AbfsHttpOperation.class);
    Mockito.doCallRealMethod()
        .when(httpOperationMock)
        .getTracingContextSuffix();
    return httpOperationMock;
  }

  @Test
  public void testTcHeaderOnJDKClientUse() {
    TracingContext tc = getSampleTracingContext();
    AbfsHttpOperation op = Mockito.mock(AbfsHttpOperation.class);
    Mockito.doCallRealMethod().when(op).getTracingContextSuffix();
    tc.constructHeader(op, null, null);
  }
}
