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

package org.apache.hadoop.fs.azurebfs;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;

/**
 * Test filesystem initialization and creation.
 */
public class ITestAzureBlobFileSystemInitAndCreate extends
    AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemInitAndCreate() throws Exception {
    this.getConfiguration().unset(ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION);
  }

  @Override
  public void setup() {
  }

  @Override
  public void teardown() {
  }

  @Test (expected = FileNotFoundException.class)
  public void ensureFilesystemWillNotBeCreatedIfCreationConfigIsNotSet() throws Exception {
    final AzureBlobFileSystem fs = this.createFileSystem();
    FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
  }

  @Test
  public void ensureFilesystemWillBeCreatedIfCreationConfigIsSet() throws Exception {
    final AzureBlobFileSystem fs = createFileSystem();
    Configuration config = getRawConfiguration();
    fs.initialize(FileSystem.getDefaultUri(config), config);

    // Make sure createFileSystemIfNotExists is working as intended.
    final MockAzureBlobFileSystemStore store = new MockAzureBlobFileSystemStore(config);
    fs.setAbfsStore(store);
    fs.createFileSystemIfNotExist(getTestTracingContext(fs, true));
    assert(store.isCreateFileSystemCalled);
  }

  /**
   * Mock AzureBlobFileSystemStore to simulate container already exists
   * exception when calling createFileSystem command.
   */
  static class MockAzureBlobFileSystemStore extends AzureBlobFileSystemStore {
    boolean isCreateFileSystemCalled = false;

    public MockAzureBlobFileSystemStore(Configuration config) throws IOException {
      super(FileSystem.getDefaultUri(config), true, config, null);
    }

    @Override
    public void createFilesystem(TracingContext tracingContext) throws AzureBlobFileSystemException {
      isCreateFileSystemCalled = true;
      // Make sure createFileSystemIfNotExists works when the filesystem/container already exists.
      throw new AbfsRestOperationException(
          AzureServiceErrorCode.FILE_SYSTEM_ALREADY_EXISTS.getStatusCode(),
          AzureServiceErrorCode.FILE_SYSTEM_ALREADY_EXISTS.getErrorCode(),
          "This container is already exists", null);
    }
  }
}
