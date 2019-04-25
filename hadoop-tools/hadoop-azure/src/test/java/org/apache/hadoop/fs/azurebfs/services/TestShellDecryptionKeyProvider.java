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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.File;
import java.nio.charset.Charset;

import org.junit.Assert;
import org.junit.Test;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.KeyProviderException;
import org.apache.hadoop.util.Shell;

import static org.junit.Assert.assertEquals;

/**
 * Test ShellDecryptionKeyProvider.
 *
 */
public class TestShellDecryptionKeyProvider {
  public static final Log LOG = LogFactory
      .getLog(TestShellDecryptionKeyProvider.class);
  private static final File TEST_ROOT_DIR = new File(System.getProperty(
      "test.build.data", "/tmp"), "TestShellDecryptionKeyProvider");

  @Test
  public void testScriptPathNotSpecified() throws Exception {
    if (!Shell.WINDOWS) {
      return;
    }
    ShellDecryptionKeyProvider provider = new ShellDecryptionKeyProvider();
    Configuration conf = new Configuration();
    String account = "testacct";
    String key = "key";

    conf.set(ConfigurationKeys.FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME + account, key);

    try {
      provider.getStorageAccountKey(account, conf);
      Assert
          .fail("fs.azure.shellkeyprovider.script is not specified, we should throw");
    } catch (KeyProviderException e) {
      LOG.info("Received an expected exception: " + e.getMessage());
    }
  }

  @Test
  public void testValidScript() throws Exception {
    if (!Shell.WINDOWS) {
      return;
    }
    String expectedResult = "decretedKey";

    // Create a simple script which echoes the given key plus the given
    // expected result (so that we validate both script input and output)
    File scriptFile = new File(TEST_ROOT_DIR, "testScript.cmd");
    FileUtils.writeStringToFile(scriptFile, "@echo %1 " + expectedResult,
        Charset.forName("UTF-8"));

    ShellDecryptionKeyProvider provider = new ShellDecryptionKeyProvider();
    Configuration conf = new Configuration();
    String account = "testacct";
    String key = "key1";
    conf.set(ConfigurationKeys.FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME  + account, key);
    conf.set(ConfigurationKeys.AZURE_KEY_ACCOUNT_SHELLKEYPROVIDER_SCRIPT,
        "cmd /c " + scriptFile.getAbsolutePath());

    String result = provider.getStorageAccountKey(account, conf);
    assertEquals(key + " " + expectedResult, result);
  }
}
