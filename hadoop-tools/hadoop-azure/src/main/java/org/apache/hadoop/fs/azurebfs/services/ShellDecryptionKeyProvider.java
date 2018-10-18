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
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.KeyProviderException;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shell decryption key provider which invokes an external script that will
 * perform the key decryption.
 */
public class ShellDecryptionKeyProvider extends SimpleKeyProvider {
  private static final Logger LOG = LoggerFactory.getLogger(ShellDecryptionKeyProvider.class);

  @Override
  public String getStorageAccountKey(String accountName, Configuration rawConfig)
      throws KeyProviderException {
    String envelope = super.getStorageAccountKey(accountName, rawConfig);

    AbfsConfiguration abfsConfig;
    try {
      abfsConfig = new AbfsConfiguration(rawConfig, accountName);
    } catch(IllegalAccessException | IOException e) {
      throw new KeyProviderException("Unable to get key from credential providers.", e);
    }

    final String command = abfsConfig.get(ConfigurationKeys.AZURE_KEY_ACCOUNT_SHELLKEYPROVIDER_SCRIPT);
    if (command == null) {
      throw new KeyProviderException(
          "Script path is not specified via fs.azure.shellkeyprovider.script");
    }

    String[] cmd = command.split(" ");
    String[] cmdWithEnvelope = Arrays.copyOf(cmd, cmd.length + 1);
    cmdWithEnvelope[cmdWithEnvelope.length - 1] = envelope;

    String decryptedKey = null;
    try {
      decryptedKey = Shell.execCommand(cmdWithEnvelope);
    } catch (IOException ex) {
      throw new KeyProviderException(ex);
    }

    // trim any whitespace
    return decryptedKey.trim();
  }
}
