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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
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
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.apache.hadoop.fs.azurebfs.enums.Trilean;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.*;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.*;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_BLOB_DOMAIN_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_DFS_DOMAIN_NAME;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.INCORRECT_INGRESS_TYPE;
import static org.apache.hadoop.fs.azurebfs.services.RetryPolicyConstants.EXPONENTIAL_RETRY_POLICY_ABBREVIATION;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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
    //super.setup();
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
    doReturn(client).when(store).getClient(AbfsServiceType.DFS);
    store.getAbfsConfiguration().setIsNamespaceEnabledAccountForTesting(Trilean.UNKNOWN);

    TracingContext tracingContext = getSampleTracingContext(fs, true);
    doReturn(Mockito.mock(AbfsRestOperation.class))
        .when(client)
        .getAclStatus(Mockito.anyString(), any(TracingContext.class));
    store.getIsNamespaceEnabled(tracingContext);

    verify(client, Mockito.times(1))
        .getAclStatus(Mockito.anyString(), any(TracingContext.class));
  }

  @Test
  public void testNoGetAclCallOnHnsConfigPresence() throws Exception {
    AzureBlobFileSystem fs = ((AzureBlobFileSystem) FileSystem.newInstance(
        getRawConfiguration()));
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    doReturn(client).when(store).getClient();

    doReturn(true)
        .when(store)
        .isNamespaceEnabled();

    TracingContext tracingContext = getSampleTracingContext(fs, true);
    store.getIsNamespaceEnabled(tracingContext);

    verify(client, Mockito.times(0))
        .getAclStatus(Mockito.anyString(), any(TracingContext.class));
  }

  /**
   * Test to verify that the initialization of the AzureBlobFileSystem fails
   * when an invalid ingress service type is configured.
   *
   * This test sets up a configuration with an invalid ingress service type
   * (DFS) for a Blob endpoint and expects an InvalidConfigurationValueException
   * to be thrown during the initialization of the filesystem.
   *
   * @throws Exception if an error occurs during the test execution
   */
  @Test
  public void testFileSystemInitializationFailsForInvalidIngress() throws Exception {
    assumeHnsDisabled();
    Configuration configuration = new Configuration(getRawConfiguration());
    String defaultUri = configuration.get(FS_DEFAULT_NAME_KEY);
    String accountKey = configuration.get(
        accountProperty(FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME, getAccountName()),
        configuration.get(FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME));
    configuration.set(FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME,
        accountKey.replace(ABFS_DFS_DOMAIN_NAME, ABFS_BLOB_DOMAIN_NAME));
    configuration.set(FS_AZURE_INGRESS_SERVICE_TYPE, AbfsServiceType.DFS.name());
    String blobUri = defaultUri.replace(ABFS_DFS_DOMAIN_NAME, ABFS_BLOB_DOMAIN_NAME);
    intercept(InvalidConfigurationValueException.class,
        INCORRECT_INGRESS_TYPE, () ->
            FileSystem.newInstance(new Path(blobUri).toUri(), configuration));
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

  @Test
  public void testFNSEndptConvertedIndicatorInHeaderAfterInitialize() throws Exception {
    Configuration conf = new Configuration(getRawConfiguration());
    conf.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);

    String dfsUri = String.format("%s://%s@%s.%s/",
            "abfs", getFileSystemName(),
            getAccountName().substring(0, getAccountName().indexOf('.')),
            "dfs.core.windows.net");

    AzureBlobFileSystem fs =
            (AzureBlobFileSystem) FileSystem.newInstance(new URI(dfsUri), conf);

    AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    AzureBlobFileSystemStore spiedStore = Mockito.spy(spiedFs.getAbfsStore());
    AbfsClient spiedClient = Mockito.spy(spiedStore.getClient());

    Mockito.doReturn(spiedStore).when(spiedFs).getAbfsStore();
    Mockito.doReturn(spiedClient).when(spiedStore).getClient();

// re-init the FS so the spy wiring is used
    spiedFs.initialize(fs.getUri(), conf);

// ---- Capturing the TracingContext ----
    ArgumentCaptor<TracingContext> ctxCaptor = ArgumentCaptor.forClass(TracingContext.class);

// Trigger the flow
    //spiedFs.listStatus(new Path("/")); // or whatever causes createFilesystem() internally

// Verify & capture
    verify(spiedClient, atLeastOnce())
            .getFilesystemProperties(ctxCaptor.capture());

// Extract captured value
    TracingContext captured = ctxCaptor.getValue();
    System.out.print(captured.getFNSEndptConvertedIndicator());

    AbfsHttpOperation abfsHttpOperation = Mockito.mock(AbfsHttpOperation.class);
    captured.constructHeader(abfsHttpOperation, null,
            EXPONENTIAL_RETRY_POLICY_ABBREVIATION);
    String endpointConversionIndicator = captured.getHeader().split(COLON, SPLIT_NO_LIMIT)[15];
    System.out.print("hellooo"+captured.getHeader());
    org.assertj.core.api.Assertions.assertThat(endpointConversionIndicator)
            .describedAs("Endpoint conversion indicator should be present")
            .isNotEmpty();

//    List<TracingContext> tracingContextList = ctxCaptor.getAllValues();
//
//      for (TracingContext tracingContext : tracingContextList) {
//          System.out.println(tracingContext);
//      }
//    System.out.println("Captured context: " + captured.getHeader());

  }


  /**
   * Test that FNSEndptConvertedIndicator ("T") is added to the header after endpoint conversion in initialize().
   */
//  @Test
//  public void testFNSEndptConvertedIndicatorInHeaderAfterInitialize() throws Exception {
//      String scheme = "abfs";
//      String dfsDomain = "dfs.core.windows.net";
//      String accountNameNoDns = getAccountName().substring(0, getAccountName().indexOf('.'));
//
//      Configuration conf = new Configuration(getRawConfiguration());
//      conf.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);
//
//      String dfsUri = String.format("%s://%s@%s.%s/",
//              scheme, getFileSystemName(), accountNameNoDns, dfsDomain);
//
//      AzureBlobFileSystem fs = (AzureBlobFileSystem)
//              FileSystem.newInstance(new URI(dfsUri), conf);
//
//    AzureBlobFileSystem spiedFs = Mockito.spy(fs);
//    AzureBlobFileSystemStore spiedStore = Mockito.spy(spiedFs.getAbfsStore());
//    AbfsClient spiedClient = Mockito.spy(spiedStore.getClient());
//    Mockito.doReturn(spiedClient).when(spiedStore).getClient();
//    Mockito.doReturn(spiedStore).when(spiedFs).getAbfsStore();
//
//    // Spy FS so private/inner transitions are preserved
//   // AzureBlobFileSystem fs = Mockito.spy(realFs);
//
//// Create a mock store and inject it
////    AzureBlobFileSystemStore mockStore = Mockito.mock(AzureBlobFileSystemStore.class);
////    doReturn(mockStore).when(fs).getAbfsStore();
//
//      // Initialize (this triggers account check, endpoint reset, and createFileSystem call)
//    spiedFs.initialize(fs.getUri(), conf);
//
//      // Capture the TracingContext passed into createFileSystem
//    ArgumentCaptor<TracingContext> captor = ArgumentCaptor.forClass(TracingContext.class);
//
//// Now call initialize (this triggers the conversion + createFileSystem)
//
//// Verify and capture
////    verify(fs, times(2))
////            .createFileSystem(captor.capture());
////
////    TracingContext ctx = captor.getValue();
////    System.out.println("hiii"+ctx);
////    Assertions.assertNotNull(ctx, "Expected a TracingContext passed into createFileSystem");
////
////// Now! the header exists
////    String header = ctx.getHeader();
////    System.out.println("hiiiiiiiii"+header);
//
////    ArgumentCaptor<TracingContext> captor9 = ArgumentCaptor.forClass(TracingContext.class);
////
////    verify(spiedFs.getAbfsStore().getClient(), times(1)).createFilesystem(captor9.capture());
//
//    TracingContext[] capturedContext = new TracingContext[1];
//    doAnswer(invocation -> {
//      capturedContext[0] = invocation.getArgument(0);
//      return null; // or appropriate return value
//    }).when(spiedFs.getAbfsStore().getClient()).createFilesystem(any(TracingContext.class));
//
////    List<TracingContext> tracingContextList = captor9.getAllValues();
////    System.out.println(tracingContextList);
//
////    Assertions.assertNotNull(header, "Tracing header should be generated");
////    Assertions.assertTrue(header.endsWith("T"),
////            "Expected FNSEndptConvertedIndicator 'T' at the end of the header, got: " + header);
////
////    System.out.println("Captured Header: " + header);
//
//
//
////    String scheme = "abfs";
////    String dfsDomain = "dfs.core.windows.net";
////    String accountNameNoDns = getAccountName().substring(0,
////            getAccountName().indexOf("."));
////    Configuration conf = new Configuration(getRawConfiguration());
////    conf.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);
////
////    String dfsUri = String.format("%s://%s@%s.%s/", scheme, getFileSystemName(),
////            accountNameNoDns, dfsDomain);
////
////    AzureBlobFileSystem fs = (AzureBlobFileSystem)
////            FileSystem.newInstance(new URI(dfsUri), conf);
////
////    // triggers endpoint conversion but NO header yet
////    fs.initialize(fs.getUri(), conf);
////
////    // FORCE an operation to cause constructHeader() execution
////    fs.create(new Path("/testHeaderTrigger"));
////
////    // Now the tracing header is actually created
////    String header = fs.getInitFSTracingHeader();
////    System.out.println("Header = " + header);
////
////    Assertions.assertNotNull(header, "Tracing header should be available after actual request");
////    Assertions.assertTrue(header.endsWith("T"),
////            "Tracing header should end with 'T' for FNSEndptConvertedIndicator, but was: " + header);
//  }

}
