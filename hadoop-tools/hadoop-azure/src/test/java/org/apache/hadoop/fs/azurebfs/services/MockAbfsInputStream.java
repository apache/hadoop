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
import java.util.UUID;

import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class MockAbfsInputStream extends AbfsInputStream {

  int errStatus = 0;
  boolean mockRequestException = false;
  boolean mockConnectionException = false;

  public MockAbfsInputStream(final AbfsClient mockClient,
      final org.apache.hadoop.fs.FileSystem.Statistics statistics,
      final String path,
      final long contentLength,
      final AbfsInputStreamContext abfsInputStreamContext,
      final String eTag,
      TracingContext tracingContext) {
    super(mockClient, statistics, path, contentLength, abfsInputStreamContext,
        eTag,
        new TracingContext("MockFastpathTest",
            UUID.randomUUID().toString(), FSOperationType.OPEN, TracingHeaderFormat.ALL_ID_FORMAT,
            null));
  }

  public MockAbfsInputStream(final AbfsClient client, final AbfsInputStream in)
      throws IOException {
    super(new MockAbfsClient(client), in.getFSStatistics(), in.getPath(),
        in.getContentLength(), in.getContext().withFastpathEnabledState(true),
        in.getETag(),
        in.getTracingContext());

  }

  protected boolean checkFastpathStatus() {
    try {
      AbfsRestOperation op;
      this.tracingContext.setFastpathStatus(FastpathStatus.FASTPATH);
      op = executeFastpathOpen(path, eTag);
      this.fastpathFileHandle = op.getFastpathFileHandle();
      LOG.debug("Fastpath handled opened {}", this.fastpathFileHandle);
    } catch (AzureBlobFileSystemException e) {
      LOG.debug("Fastpath status check (Fastpath open) failed with {}", e);
      this.tracingContext.setFastpathStatus(FastpathStatus.CONN_FAIL_REST_FALLBACK);
      return false;
    }

    return true;
  }

  protected AbfsRestOperation executeFastpathOpen(String path, String eTag, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    signalErrorConditionToMockClient();
    return ((MockAbfsClient)client).fastPathOpen(path, eTag, tracingContext);
  }

  protected AbfsRestOperation executeFastpathClose(String path, String eTag, String fastpathFileHandle, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    signalErrorConditionToMockClient();
    return ((MockAbfsClient)client).fastPathClose(path, eTag, fastpathFileHandle, tracingContext);
  }

  protected AbfsRestOperation executeRead(String path, byte[] b, String sasToken, ReadRequestParameters reqParam, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    signalErrorConditionToMockClient();
    return ((MockAbfsClient)client).read(path, b, sasToken, reqParam, tracingContext);
  }

  private void signalErrorConditionToMockClient() {
    if (errStatus != 0) {
      ((MockAbfsClient) client).induceError(errStatus);
    }

    if (mockRequestException) {
      ((MockAbfsClient) client).induceRequestException();
    }

    if (mockConnectionException) {
      ((MockAbfsClient) client).induceConnectionException();
    }
  }

  public AbfsClient getClient() {
    return this.client;
  }

  public Statistics getFSStatistics() {
    return super.getFSStatistics();
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
}
