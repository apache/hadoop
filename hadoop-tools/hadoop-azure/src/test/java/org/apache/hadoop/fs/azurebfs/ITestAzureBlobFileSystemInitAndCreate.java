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
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.enums.Trilean;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COLON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DOT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SPLIT_NO_LIMIT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_IS_HNS_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.apache.hadoop.fs.azurebfs.services.RetryPolicyConstants.EXPONENTIAL_RETRY_POLICY_ABBREVIATION;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.ArgumentMatchers.any;

import org.junit.jupiter.api.Assertions;

/**
 * Test filesystem initialization and creation.
 */
public class ITestAzureBlobFileSystemInitAndCreate extends
    AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemInitAndCreate() throws Exception {
    this.getConfiguration().unset(ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION);
  }

  @BeforeEach
  @Override
  public void setup() throws Exception {
    super.setup();
  }

  @AfterEach
  @Override
  public void teardown() {
  }

  @Test
  public void ensureFilesystemWillNotBeCreatedIfCreationConfigIsNotSet() throws Exception {
      Assertions.assertThrows(FileNotFoundException.class, () -> {
          final AzureBlobFileSystem fs = this.createFileSystem();
          FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
      });
  }

  @Test
  public void testGetAclCallOnHnsConfigAbsence() throws Exception {
    AzureBlobFileSystem fs = ((AzureBlobFileSystem) FileSystem.newInstance(
        getRawConfiguration()));
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsClient client = Mockito.spy(fs.getAbfsStore().getClient(AbfsServiceType.DFS));
    Mockito.doReturn(client).when(store).getClient(AbfsServiceType.DFS);
    store.getAbfsConfiguration().setIsNamespaceEnabledAccountForTesting(Trilean.UNKNOWN);

    TracingContext tracingContext = getSampleTracingContext(fs, true);
    Mockito.doReturn(Mockito.mock(AbfsRestOperation.class))
        .when(client)
        .getAclStatus(Mockito.anyString(), any(TracingContext.class));
    store.getIsNamespaceEnabled(tracingContext);

    Mockito.verify(client, Mockito.times(1))
        .getAclStatus(Mockito.anyString(), any(TracingContext.class));
  }

  @Test
  public void testNoGetAclCallOnHnsConfigPresence() throws Exception {
    AzureBlobFileSystem fs = ((AzureBlobFileSystem) FileSystem.newInstance(
        getRawConfiguration()));
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    Mockito.doReturn(client).when(store).getClient();

    Mockito.doReturn(true)
        .when(store)
        .isNamespaceEnabled();

    TracingContext tracingContext = getSampleTracingContext(fs, true);
    store.getIsNamespaceEnabled(tracingContext);

    Mockito.verify(client, Mockito.times(0))
        .getAclStatus(Mockito.anyString(), any(TracingContext.class));
  }

  @Test
  public void testFileSystemInitFailsIfNotAbleToDetermineAccountType() throws Exception {
    AzureBlobFileSystem fs = ((AzureBlobFileSystem) FileSystem.newInstance(
        getRawConfiguration()));
    AzureBlobFileSystem mockedFs = Mockito.spy(fs);
    Mockito.doThrow(
        new AbfsRestOperationException(HTTP_UNAVAILABLE, "Throttled",
            "Throttled", null)).when(mockedFs).getIsNamespaceEnabled(any());

    intercept(AzureBlobFileSystemException.class,
        FS_AZURE_ACCOUNT_IS_HNS_ENABLED, () ->
            mockedFs.initialize(fs.getUri(), getRawConfiguration()));
  }

  /**
   * Test to verify that the fnsEndptConvertedIndicator ("T") is present in the tracing header
   * after endpoint conversion during AzureBlobFileSystem initialization.
   *
   * @throws Exception if any error occurs during the test
   */
  @Test
  public void testFNSEndptConvertedIndicatorInHeader() throws Exception {
    assumeHnsDisabled();
    String scheme = "abfs";
    String dfsDomain = "dfs.core.windows.net";
    String endptConversionIndicatorInTc = "T";
    Configuration conf = new Configuration(getRawConfiguration());
    conf.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);

    String dfsUri = String.format("%s://%s@%s.%s/",
            scheme, getFileSystemName(),
            getAccountName().substring(0, getAccountName().indexOf(DOT)),
            dfsDomain);

    // Initialize filesystem with DFS endpoint
    try (AzureBlobFileSystem fs =
                 (AzureBlobFileSystem) FileSystem.newInstance(new URI(dfsUri), conf)) {
      AzureBlobFileSystem spiedFs = Mockito.spy(fs);
      AzureBlobFileSystemStore spiedStore = Mockito.spy(spiedFs.getAbfsStore());
      AbfsClient spiedClient = Mockito.spy(spiedStore.getClient());

      Mockito.doReturn(spiedStore).when(spiedFs).getAbfsStore();
      Mockito.doReturn(spiedClient).when(spiedStore).getClient();

      // re-init the FS so the spy wiring is used
      spiedFs.initialize(fs.getUri(), conf);
      ArgumentCaptor<TracingContext> ctxCaptor = ArgumentCaptor.forClass(TracingContext.class);
      Mockito.verify(spiedClient, Mockito.atLeastOnce())
              .getFilesystemProperties(ctxCaptor.capture());

      TracingContext captured = ctxCaptor.getValue();

      AbfsHttpOperation abfsHttpOperation = Mockito.mock(AbfsHttpOperation.class);
      captured.constructHeader(abfsHttpOperation, null,
              EXPONENTIAL_RETRY_POLICY_ABBREVIATION);

      // The tracing context being used FS Initialization should have the endpoint conversion indicator set to 'T'
      final int endpointConversionIndicatorIndex  = 14;
      String endpointConversionIndicator = captured.getHeader().split(COLON, SPLIT_NO_LIMIT)[endpointConversionIndicatorIndex ];
      Assertions.assertFalse(endpointConversionIndicator.isEmpty(), "Endpoint conversion indicator should be present");
      Assertions.assertEquals(endptConversionIndicatorInTc, endpointConversionIndicator, "Endpoint conversion indicator should be 'T'");
    }
  }
}
