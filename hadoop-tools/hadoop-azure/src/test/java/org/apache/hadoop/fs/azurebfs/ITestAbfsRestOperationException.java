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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.oauth2.RetryTestTokenProvider;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Verify the AbfsRestOperationException error message format.
 * */
public class ITestAbfsRestOperationException extends AbstractAbfsIntegrationTest{
  public ITestAbfsRestOperationException() throws Exception {
    super();
  }

  @Test
  public void testAbfsRestOperationExceptionFormat() throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    Path nonExistedFilePath1 = new Path("nonExistedPath1");
    Path nonExistedFilePath2 = new Path("nonExistedPath2");
    try {
      FileStatus fileStatus = fs.getFileStatus(nonExistedFilePath1);
    } catch (Exception ex) {
      String errorMessage = ex.getLocalizedMessage();
      String[] errorFields = errorMessage.split(",");

      Assert.assertEquals(4, errorFields.length);
      // Check status message, status code, HTTP Request Type and URL.
      Assert.assertEquals("Operation failed: \"The specified path does not exist.\"", errorFields[0].trim());
      Assert.assertEquals("404", errorFields[1].trim());
      Assert.assertEquals("HEAD", errorFields[2].trim());
      Assert.assertTrue(errorFields[3].trim().startsWith("http"));
    }

    try {
      fs.listFiles(nonExistedFilePath2, false);
    } catch (Exception ex) {
      // verify its format
      String errorMessage = ex.getLocalizedMessage();
      String[] errorFields = errorMessage.split(",");

      Assert.assertEquals(6, errorFields.length);
      // Check status message, status code, HTTP Request Type and URL.
      Assert.assertEquals("Operation failed: \"The specified path does not exist.\"", errorFields[0].trim());
      Assert.assertEquals("404", errorFields[1].trim());
      Assert.assertEquals("GET", errorFields[2].trim());
      Assert.assertTrue(errorFields[3].trim().startsWith("http"));
      // Check storage error code and storage error message.
      Assert.assertEquals("PathNotFound", errorFields[4].trim());
      Assert.assertTrue(errorFields[5].contains("RequestId")
              && errorFields[5].contains("Time"));
    }
  }

  @Test
  public void testRequestRetryConfig() throws Exception {
    testRetryLogic(0);
    testRetryLogic(3);
  }

  public void testRetryLogic(int numOfRetries) throws Exception {
    AzureBlobFileSystem fs = this.getFileSystem();

    Configuration config = new Configuration(this.getRawConfiguration());
    String accountName = config.get("fs.azure.abfs.account.name");
    // Setup to configure custom token provider
    config.set("fs.azure.account.auth.type." + accountName, "Custom");
    config.set("fs.azure.account.oauth.provider.type." + accountName, "org.apache.hadoop.fs"
        + ".azurebfs.oauth2.RetryTestTokenProvider");
    config.set("fs.azure.io.retry.max.retries", Integer.toString(numOfRetries));
    // Stop filesystem creation as it will lead to calls to store.
    config.set("fs.azure.createRemoteFileSystemDuringInitialization", "false");

    final AzureBlobFileSystem fs1 =
        (AzureBlobFileSystem) FileSystem.newInstance(fs.getUri(),
        config);
    RetryTestTokenProvider.ResetStatusToFirstTokenFetch();

    intercept(Exception.class,
        ()-> {
          fs1.getFileStatus(new Path("/"));
        });

    // Number of retries done should be as configured
    Assert.assertTrue(
        "Number of token fetch retries (" + RetryTestTokenProvider.reTryCount
            + ") done, does not match with max " + "retry count configured (" + numOfRetries
            + ")", RetryTestTokenProvider.reTryCount == numOfRetries);
  }
}