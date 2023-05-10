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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ProviderUtils;

/**
 * Key provider that simply returns the storage account key from the
 * configuration as plaintext.
 */
@InterfaceAudience.Private
public class SimpleKeyProvider implements KeyProvider {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleKeyProvider.class);

  protected static final String KEY_ACCOUNT_KEY_PREFIX =
      "fs.azure.account.key.";

  @Override
  public String getStorageAccountKey(String accountName, Configuration conf)
      throws KeyProviderException {
    String key = null;
    try {
      Configuration c = ProviderUtils.excludeIncompatibleCredentialProviders(
          conf, NativeAzureFileSystem.class);
      char[] keyChars = c.getPassword(getStorageAccountKeyName(accountName));
      if (keyChars != null) {
        key = new String(keyChars);
      }
    } catch(IOException ioe) {
      LOG.warn("Unable to get key from credential providers.", ioe);
    }
    return key;
  }

  protected String getStorageAccountKeyName(String accountName) {
    return KEY_ACCOUNT_KEY_PREFIX + accountName;
  }
}
