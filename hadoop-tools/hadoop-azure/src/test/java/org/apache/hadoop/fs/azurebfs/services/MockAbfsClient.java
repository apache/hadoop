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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsFastpathException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters.Mode;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_GET;
import static org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType.FastpathClose;

public class MockAbfsClient extends AbfsClient {

  int errStatus = 0;
  boolean mockRequestException = false;
  boolean mockConnectionException = false;
  boolean forceFastpathReadAlways = true;

  public MockAbfsClient(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final AccessTokenProvider tokenProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    super(baseUrl, sharedKeyCredentials, abfsConfiguration, tokenProvider,
        abfsClientContext);
  }

  public MockAbfsClient(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final SASTokenProvider sasTokenProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    super(baseUrl, sharedKeyCredentials, abfsConfiguration, sasTokenProvider,
        abfsClientContext);
  }

  public MockAbfsClient(final AbfsClient client) throws IOException {
    super(client.getBaseUrl(), client.getSharedKeyCredentials(),
        client.getAbfsConfiguration(), client.getTokenProvider(),
        client.getAbfsClientContext());
  }

  public AbfsRestOperation read(String path,
      byte[] buffer,
      String cachedSasToken,
      ReadRequestParameters reqParams,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (forceFastpathReadAlways) {
      // Forcing read over fastpath even if InputStream determined REST mode
      // becase of Fastpath open failure. This is for mock tests to fail
      // if fastpath connection didnt work rather than reporting a successful test
      // run due to REST fallback
      reqParams.setMode(ReadRequestParameters.Mode.FASTPATH_CONNECTION_MODE);
    }
    return super.read(path, buffer, cachedSasToken, reqParams, tracingContext);
  }

  protected AbfsRestOperation executeFastpathRead(String path,
      ReadRequestParameters reqParams,
      URL url,
      List<AbfsHttpHeader> requestHeaders,
      byte[] buffer,
      String sasTokenForReuse,
      TracingContext  tracingContext) throws AzureBlobFileSystemException {
    final AbfsRestIODataParameters ioDataParams = new AbfsRestIODataParameters(buffer,
        reqParams.getBufferOffset(),
        reqParams.getReadLength(),
        reqParams.getFastpathFileHandle());
    final MockAbfsRestOperation op = new MockAbfsRestOperation(
        AbfsRestOperationType.FastpathRead,
        this,
        HTTP_METHOD_GET,
        url,
        requestHeaders,
        ioDataParams,
        sasTokenForReuse);
    try {
      signalErrorConditionToMockRestOp(op);
      op.execute(tracingContext);
      return op;
    } catch (AbfsFastpathException ex) {
      if (mockErrorConditionSet()) {
        forceFastpathReadAlways = false;
        // execute original abfsclient behaviour
        reqParams.setMode(Mode.HTTP_CONNECTION_MODE);
        if (ex.getCause() instanceof com.azure.storage.fastpath.exceptions.FastpathRequestException) {
          tracingContext.setFastpathStatus(FastpathStatus.REQ_FAIL_REST_FALLBACK);
        } else {
          tracingContext.setFastpathStatus(FastpathStatus.CONN_FAIL_REST_FALLBACK);
        }

        return read(path, buffer, op.getSasToken(), reqParams, tracingContext);
      } else {
        // Stop REST fall back for mock tests
        throw ex;
      }
    }
  }

  protected AbfsRestOperation executeFastpathOpen(URL url,
      List<AbfsHttpHeader> requestHeaders,
      String sasTokenForReuse,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final MockAbfsRestOperation op = new MockAbfsRestOperation(
        AbfsRestOperationType.FastpathOpen,
        this,
        HTTP_METHOD_GET,
        url,
        requestHeaders,
        sasTokenForReuse);
    signalErrorConditionToMockRestOp(op);
    op.execute(tracingContext);
    return op;
  }

  protected AbfsRestOperation executeFastpathClose(URL url,
      List<AbfsHttpHeader> requestHeaders,
      String sasTokenForReuse,
      String fastpathFileHandle,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final MockAbfsRestOperation op = new MockAbfsRestOperation(
        FastpathClose,
        this,
        HTTP_METHOD_GET,
        url,
        requestHeaders,
        sasTokenForReuse,
        fastpathFileHandle);
    signalErrorConditionToMockRestOp(op);
    op.execute(tracingContext);
    return op;
  }

  public AbfsCounters getAbfsCounters() {
    return super.getAbfsCounters();
  }

  private void signalErrorConditionToMockRestOp(MockAbfsRestOperation op) {
    if (errStatus != 0) {
      op.induceError(errStatus);
    }

    if (mockRequestException) {
      op.induceRequestException();
    }

    if (mockConnectionException) {
      op.induceConnectionException();
    }
  }

  public void induceError(int httpStatus) {
    errStatus = httpStatus;
  }

  public void induceRequestException() {
    mockRequestException = true;
  }

  public void induceConnectionException() {
    mockConnectionException = true;
  }

  public void resetAllMockErrStates() {
    errStatus = 0;
    mockRequestException = false;
    mockConnectionException = false;
  }

  private boolean mockErrorConditionSet() {
    return ((errStatus != 0) || mockRequestException || mockConnectionException);
  }
}
