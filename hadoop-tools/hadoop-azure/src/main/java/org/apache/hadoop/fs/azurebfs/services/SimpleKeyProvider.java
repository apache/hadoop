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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.KeyProviderException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Key provider that simply returns the storage account key from the
 * configuration as plaintext.
 */
public class SimpleKeyProvider implements KeyProvider {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleKeyProvider.class);

  @Override
  public String getStorageAccountKey(String accountName, Configuration rawConfig)
      throws KeyProviderException {
    String key = null;

    try {
      AbfsConfiguration abfsConfig = new AbfsConfiguration(rawConfig, accountName);
      key = abfsConfig.getPasswordString(ConfigurationKeys.FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME);
    } catch(IllegalAccessException | InvalidConfigurationValueException e) {
      throw new KeyProviderException("Failure to initialize configuration", e);
    } catch(IOException ioe) {
      LOG.warn("Unable to get key from credential providers. {}", ioe);
    }

    return key;
  }
}
