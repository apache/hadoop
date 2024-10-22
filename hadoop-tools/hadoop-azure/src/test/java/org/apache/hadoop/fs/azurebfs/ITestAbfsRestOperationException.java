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

import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.oauth2.RetryTestTokenProvider;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CUSTOM_TOKEN_FETCH_RETRY_COUNT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_IS_HNS_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.accountProperty;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_ABFS_ACCOUNT_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Verify the AbfsRestOperationException error message format.
 * */
public class ITestAbfsRestOperationException extends AbstractAbfsIntegrationTest{
  private static final String RETRY_TEST_TOKEN_PROVIDER =
      "org.apache.hadoop.fs.azurebfs.oauth2.RetryTestTokenProvider";

  public ITestAbfsRestOperationException() throws Exception {
    super();
  }

  @Test
  public void testAbfsRestOperationExceptionFormat() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    Path nonExistedFilePath1 = new Path("nonExistedPath1");
    Path nonExistedFilePath2 = new Path("nonExistedPath2");
    try {
      fs.getFileStatus(nonExistedFilePath1);
    } catch (Exception ex) {
      String errorMessage = ex.getLocalizedMessage();
      String[] errorFields = errorMessage.split(",");

      // Expected Fields are: Message, StatusCode, Method, URL, ActivityId(rId)
      Assertions.assertThat(errorFields)
          .describedAs("Number of Fields in exception message are not as expected")
          .hasSize(5);
      // Check status message, status code, HTTP Request Type and URL.
      Assertions.assertThat(errorFields[0].trim())
          .describedAs("Error Message Field in exception message is wrong")
          .isEqualTo("Operation failed: \"The specified path does not exist.\"");
      Assertions.assertThat(errorFields[1].trim())
          .describedAs("Status Code Field in exception message "
              + "should be \"404\"")
          .isEqualTo("404");
      Assertions.assertThat(errorFields[2].trim())
          .describedAs("Http Rest Method Field in exception message "
              + "should be \"HEAD\"")
          .isEqualTo("HEAD");
      Assertions.assertThat(errorFields[3].trim())
          .describedAs("Url Field in exception message"
              + " should start with \"http\"")
          .startsWith("http");
      Assertions.assertThat(errorFields[4].trim())
          .describedAs("ActivityId Field in exception message "
              + "should start with \"rId:\"")
          .startsWith("rId:");
    }

    try {
      fs.listFiles(nonExistedFilePath2, false);
    } catch (Exception ex) {
      // verify its format
      String errorMessage = ex.getLocalizedMessage();
      String[] errorFields = errorMessage.split(",");
      // Expected Fields are: Message, StatusCode, Method, URL, ActivityId(rId), StorageErrorCode, StorageErrorMessage.
      Assertions.assertThat(errorFields)
          .describedAs("Number of Fields in exception message are not as expected")
          .hasSize(7);
      // Check status message, status code, HTTP Request Type and URL.
      Assertions.assertThat(errorFields[0].trim())
          .describedAs("Error Message Field in exception message is wrong")
          .isEqualTo("Operation failed: \"The specified path does not exist.\"");
      Assertions.assertThat(errorFields[1].trim())
          .describedAs("Status Code Field in exception message"
              + " should be \"404\"")
          .isEqualTo("404");
      Assertions.assertThat(errorFields[2].trim())
          .describedAs("Http Rest Method Field in exception message"
              + " should be \"GET\"")
          .isEqualTo("GET");
      Assertions.assertThat(errorFields[3].trim())
          .describedAs("Url Field in exception message"
              + " should start with \"http\"")
          .startsWith("http");
      Assertions.assertThat(errorFields[4].trim())
          .describedAs("ActivityId Field in exception message"
              + " should start with \"rId:\"")
          .startsWith("rId:");
      // Check storage error code and storage error message.
      Assertions.assertThat(errorFields[5].trim())
          .describedAs("StorageErrorCode Field in exception message"
              + " should be \"PathNotFound\"")
          .isEqualTo("PathNotFound");
      Assertions.assertThat(errorFields[6].trim())
          .describedAs("StorageErrorMessage Field in exception message"
              + " should contain \"RequestId\"")
          .contains("RequestId");
      Assertions.assertThat(errorFields[6].trim())
          .describedAs("StorageErrorMessage Field in exception message"
              + " should contain \"Time\"")
          .contains("Time");
    }
  }

  @Test
  public void testCustomTokenFetchRetryCount() throws Exception {
    testWithDifferentCustomTokenFetchRetry(0);
    testWithDifferentCustomTokenFetchRetry(3);
    testWithDifferentCustomTokenFetchRetry(5);
  }

  public void testWithDifferentCustomTokenFetchRetry(int numOfRetries) throws Exception {
    AzureBlobFileSystem fs = this.getFileSystem();
    Configuration config = getCustomAuthConfiguration(numOfRetries);
    try (AzureBlobFileSystem fs1 = (AzureBlobFileSystem) FileSystem.newInstance(fs.getUri(),
        config)) {
      RetryTestTokenProvider retryTestTokenProvider
              = RetryTestTokenProvider.getCurrentRetryTestProviderInstance(
              getAccessTokenProvider(fs1));
      retryTestTokenProvider.resetStatusToFirstTokenFetch();

      intercept(Exception.class,
              () -> {
                fs1.getFileStatus(new Path("/"));
              });

      // Number of retries done should be as configured
      Assertions.assertThat(retryTestTokenProvider.getRetryCount())
              .describedAs("Number of token fetch retries done does not "
                      + "match with fs.azure.custom.token.fetch.retry.count configured")
              .isEqualTo(numOfRetries);
    }
  }

  @Test
  public void testAuthFailException() throws Exception {
    Configuration config = getCustomAuthConfiguration(0);
    final AzureBlobFileSystem fs = getFileSystem(config);
    AbfsRestOperationException e = intercept(AbfsRestOperationException.class, () -> {
      fs.getFileStatus(new Path("/"));
    });

    String errorDesc = "Should throw RestOp exception on AAD failure";
    Assertions.assertThat(e.getStatusCode())
        .describedAs("Incorrect status code: " + errorDesc).isEqualTo(-1);
    Assertions.assertThat(e.getErrorCode())
        .describedAs("Incorrect error code: " + errorDesc)
        .isEqualTo(AzureServiceErrorCode.UNKNOWN);
    Assertions.assertThat(e.getErrorMessage())
        .describedAs("Incorrect error message: " + errorDesc)
        .contains("Auth failure: ");
  }

  /**
   * Returns a configuration with a custom token provider configured. {@link RetryTestTokenProvider}
   * @param numOfRetries Number of retries to be configured for token fetch.
   * @return Configuration
   */
  private Configuration getCustomAuthConfiguration(final int numOfRetries) {
    Configuration config = new Configuration(this.getRawConfiguration());
    String accountName = config.get(FS_AZURE_ABFS_ACCOUNT_NAME);
    // Setup to configure custom token provider.
    config.set(accountProperty(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, accountName), "Custom");
    config.set(accountProperty(FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME, accountName),
        RETRY_TEST_TOKEN_PROVIDER);
    config.setInt(AZURE_CUSTOM_TOKEN_FETCH_RETRY_COUNT, numOfRetries);
    // Stop filesystem creation as it will lead to calls to store.
    config.setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, false);
    config.setBoolean(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, config.getBoolean(
        FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, true));
    return config;
  }
}
