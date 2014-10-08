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
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;

/**
 * Shell decryption key provider which invokes an external script that will
 * perform the key decryption.
 */
@InterfaceAudience.Private
public class ShellDecryptionKeyProvider extends SimpleKeyProvider {
  static final String KEY_ACCOUNT_SHELLKEYPROVIDER_SCRIPT =
      "fs.azure.shellkeyprovider.script";

  @Override
  public String getStorageAccountKey(String accountName, Configuration conf)
      throws KeyProviderException {
    String envelope = super.getStorageAccountKey(accountName, conf);

    final String command = conf.get(KEY_ACCOUNT_SHELLKEYPROVIDER_SCRIPT);
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
