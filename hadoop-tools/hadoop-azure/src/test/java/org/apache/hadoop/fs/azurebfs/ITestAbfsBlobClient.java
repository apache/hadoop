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

package org.apache.hadoop.fs.azurebfs;

import java.net.URI;
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.utils.Base64;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_APPEND_BLOB_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_CONTRACT_TEST_URI;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_APPENDBLOB_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONTAINER_PREFIX;

/**
 * Test class to test AbfsBlobClient APIs.
 * Todo: [FnsOverBlob] - Add more tests to cover all APIs once they are ready
 */
public class ITestAbfsBlobClient {

  @Test
  public void testAbfsBlobClient() throws Exception {
    try (AzureBlobFileSystem fs = getBlobFileSystem()) {
      AbfsClient client = fs.getAbfsStore().getClient();
      Assertions.assertThat(client).isInstanceOf(AbfsBlobClient.class);
      // Make sure all client.REST_API_CALLS succeed with right parameters
      testClientAPIs(client, getTestTracingContext(fs));
    } catch (AzureBlobFileSystemException ex) {
      // Todo: [FnsOverBlob] - Remove this block once all Blob Endpoint Support is ready.
      Assertions.assertThat(ex.getMessage()).contains("Blob Endpoint Support is not yet implemented");
    }
  }

  private void testClientAPIs(AbfsClient client, TracingContext tracingContext) throws Exception {
    // 1. Set File System Properties
    String val1 = Base64.encode("value1".getBytes());
    String val2 = Base64.encode("value2".getBytes());
    String properties = "key1=" + val1 + ",key2=" + val2;
    client.setFilesystemProperties(properties, tracingContext);

    // 2. Get File System Properties
    client.getFilesystemProperties(tracingContext);

    // 3. Create Path
    client.createPath("/test", true, true, null, false, null, null,  tracingContext);
    client.createPath("/dir", false, true, null, false, null, null,  tracingContext);
    client.createPath("/dir/test", true, true, null, false, null, null,  tracingContext);

    // 4. List Path
    client.listPath("/", false, 5, null, tracingContext);

    // 5. Acquire lease
    client.acquireLease("/dir/test", 5, tracingContext);

    // 6. Set Path Properties
    client.setPathProperties("/test", properties, tracingContext, null);

    // 7. Get Path Status
    client.getPathStatus("/test", true, tracingContext, null);

    // N. Delete File System
    client.deleteFilesystem(tracingContext);
  }

  private AzureBlobFileSystem getBlobFileSystem() throws Exception {
    Configuration rawConfig = new Configuration();
    rawConfig.addResource(TEST_CONFIGURATION_FILE_NAME);

    String fileSystemName = TEST_CONTAINER_PREFIX + UUID.randomUUID().toString();
    String accountName = rawConfig.get(FS_AZURE_ACCOUNT_NAME, "");
    if (accountName.isEmpty()) {
      // check if accountName is set using different config key
      accountName = rawConfig.get(FS_AZURE_ABFS_ACCOUNT_NAME, "");
    }
    Assume.assumeFalse("Skipping test as account name is not provided", accountName.isEmpty());

    Assume.assumeFalse("Blob Endpoint Works only with FNS Accounts",
        rawConfig.getBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, true));
    accountName = setBlobEndpoint(accountName);

    AbfsConfiguration abfsConfig = new AbfsConfiguration(rawConfig, accountName);
    AuthType authType = abfsConfig.getEnum(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, AuthType.SharedKey);
    String abfsScheme = authType == AuthType.SharedKey ? FileSystemUriSchemes.ABFS_SCHEME
        : FileSystemUriSchemes.ABFS_SECURE_SCHEME;
    final String abfsUrl = fileSystemName + "@" + accountName;
    URI defaultUri = null;

    try {
      defaultUri = new URI(abfsScheme, abfsUrl, null, null, null);
    } catch (Exception ex) {
      throw new AssertionError(ex);
    }

    String testUrl = defaultUri.toString();
    abfsConfig.set(FS_DEFAULT_NAME_KEY, defaultUri.toString());
    abfsConfig.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);
    if (rawConfig.getBoolean(FS_AZURE_TEST_APPENDBLOB_ENABLED, false)) {
      String appendblobDirs = testUrl + "," + abfsConfig.get(FS_AZURE_CONTRACT_TEST_URI);
      rawConfig.set(FS_AZURE_APPEND_BLOB_KEY, appendblobDirs);
    }

    return (AzureBlobFileSystem) FileSystem.newInstance(rawConfig);
  }

  private String setBlobEndpoint(String accountName) {
    return accountName.replace(".dfs.", ".blob.");
  }

  public TracingContext getTestTracingContext(AzureBlobFileSystem fs) {
    String correlationId = "test-corr-id", fsId = "test-filesystem-id";
    TracingHeaderFormat format = TracingHeaderFormat.ALL_ID_FORMAT;
    return new TracingContext(correlationId, fsId, FSOperationType.TEST_OP, false, format, null);
  }
}
