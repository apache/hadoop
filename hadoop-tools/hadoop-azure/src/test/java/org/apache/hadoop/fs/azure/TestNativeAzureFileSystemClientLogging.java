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

package org.apache.hadoop.fs.azure;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.StringTokenizer;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.apache.log4j.Logger;
import org.junit.Test;

/**
 * Test to validate Azure storage client side logging. Tests works only when
 * testing with Live Azure storage because Emulator does not have support for
 * client-side logging.
 *
 */
public class TestNativeAzureFileSystemClientLogging extends
    NativeAzureFileSystemBaseTest {

  private AzureBlobStorageTestAccount testAccount;

  // Core-site config controlling Azure Storage Client logging
  private static final String KEY_LOGGING_CONF_STRING = "fs.azure.storage.client.logging";

  // Temporary directory created using WASB.
  private static final String TEMP_DIR = "tempDir";

  /*
   * Helper method to verify the client logging is working. This check primarily
   * checks to make sure we see a line in the logs corresponding to the entity
   * that is created during test run.
   */
  private boolean verifyStorageClientLogs(String capturedLogs, String entity)
      throws Exception {

    URI uri = testAccount.getRealAccount().getBlobEndpoint();
    String container = testAccount.getRealContainer().getName();
    String validateString = uri + Path.SEPARATOR + container + Path.SEPARATOR
        + entity;
    boolean entityFound = false;

    StringTokenizer tokenizer = new StringTokenizer(capturedLogs, "\n");

    while (tokenizer.hasMoreTokens()) {
      String token = tokenizer.nextToken();
      if (token.contains(validateString)) {
        entityFound = true;
        break;
      }
    }
    return entityFound;
  }

  /*
   * Helper method that updates the core-site config to enable/disable logging.
   */
  private void updateFileSystemConfiguration(Boolean loggingFlag)
      throws Exception {

    Configuration conf = fs.getConf();
    conf.set(KEY_LOGGING_CONF_STRING, loggingFlag.toString());
    URI uri = fs.getUri();
    fs.initialize(uri, conf);
  }

  // Using WASB code to communicate with Azure Storage.
  private void performWASBOperations() throws Exception {

    Path tempDir = new Path(Path.SEPARATOR + TEMP_DIR);
    fs.mkdirs(tempDir);
    fs.delete(tempDir, true);
  }

  @Test
  public void testLoggingEnabled() throws Exception {

    LogCapturer logs = LogCapturer.captureLogs(new Log4JLogger(Logger
        .getRootLogger()));

    // Update configuration based on the Test.
    updateFileSystemConfiguration(true);

    performWASBOperations();

    assertTrue(verifyStorageClientLogs(logs.getOutput(), TEMP_DIR));
  }

  @Test
  public void testLoggingDisabled() throws Exception {

    LogCapturer logs = LogCapturer.captureLogs(new Log4JLogger(Logger
        .getRootLogger()));

    // Update configuration based on the Test.
    updateFileSystemConfiguration(false);

    performWASBOperations();

    assertFalse(verifyStorageClientLogs(logs.getOutput(), TEMP_DIR));
  }

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    testAccount = AzureBlobStorageTestAccount.create();
    return testAccount;
  }
}