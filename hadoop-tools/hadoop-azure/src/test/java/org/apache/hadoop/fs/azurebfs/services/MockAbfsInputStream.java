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

import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;

public class MockAbfsInputStream extends AbfsInputStream {

  public MockAbfsInputStream(final AbfsClient client,
      final org.apache.hadoop.fs.FileSystem.Statistics statistics,
      final String path,
      final long contentLength,
      final org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamContext abfsInputStreamContext,
      final String eTag) {
    super(new MockAbfsClient(client), statistics, path, contentLength, abfsInputStreamContext,
        eTag);
  }

  public MockAbfsInputStream(final AbfsClient client, final AbfsInputStream in) {
    super(new MockAbfsClient(client), in.getFSStatistics(), in.getPath(), in.getContentLength(), in.getContext(),
        in.getETag());
  }

  protected boolean checkFastpathStatus() {
    try {
      AbfsRestOperation op;
      op = executeFastpathOpen(path, eTag);

      this.fastpathFileHandle = op.getFastpathFileHandle();
      LOG.debug("Fastpath handled opened {}", this.fastpathFileHandle);
    } catch (AzureBlobFileSystemException e) {
      LOG.debug("Fastpath status check (Fastpath open) failed with {}", e);
      return false;
    }

    return true;
  }

  protected AbfsRestOperation executeFastpathOpen(String path, String eTag)
      throws AzureBlobFileSystemException {
    return ((MockAbfsClient)client).fastPathOpen(path, eTag);
  }

  protected AbfsRestOperation executeFastpathClose(String path, String eTag, String fastpathFileHandle)
      throws AzureBlobFileSystemException {
    return ((MockAbfsClient)client).fastPathClose(path, eTag, fastpathFileHandle);
  }

  protected AbfsRestOperation executeRead(String path, byte[] b, String sasToken, ReadRequestParameters reqParam)
      throws AzureBlobFileSystemException {
    return ((MockAbfsClient)client).read(path, b, sasToken, reqParam);
  }

  public AbfsClient getClient() {
    return this.client;
  }

  public Statistics getFSStatistics() {
    return super.getFSStatistics();
  }
}
