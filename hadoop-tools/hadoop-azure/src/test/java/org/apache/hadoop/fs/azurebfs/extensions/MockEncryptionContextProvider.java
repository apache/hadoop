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

package org.apache.hadoop.fs.azurebfs.extensions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.security.ABFSKey;

import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.ENCRYPTION_KEY_LEN;

public class MockEncryptionContextProvider implements EncryptionContextProvider {
  private HashMap<String, String> pathToContextMap = new HashMap<>();
  private HashMap<String, byte[]> contextToKeyByteMap = new HashMap<>();
  @Override
  public void initialize(Configuration configuration, String accountName,
      String fileSystem) throws IOException {
  }

  @Override
  public ABFSKey getEncryptionContext(String path)
      throws IOException {
    String newContext = UUID.randomUUID().toString();
    pathToContextMap.put(path, newContext);
    byte[] newKey = new byte[ENCRYPTION_KEY_LEN];
    new Random().nextBytes(newKey);
    ABFSKey key = new ABFSKey(newKey);
    contextToKeyByteMap.put(newContext, key.getEncoded());
    return new ABFSKey(newContext.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public ABFSKey getEncryptionKey(String path, ABFSKey encryptionContext) throws IOException {
    String encryptionContextString =
        new String(encryptionContext.getEncoded(), StandardCharsets.UTF_8);
    if (!encryptionContextString.equals(pathToContextMap.get(path))) {
      throw new IOException("encryption context does not match path");
    }
    return new ABFSKey(contextToKeyByteMap.get(encryptionContextString));
  }

  public byte[] getEncryptionKeyForTest(String encryptionContext) {
    return contextToKeyByteMap.get(encryptionContext);
  }

  public String getEncryptionContextForTest(String path) {
    return pathToContextMap.get(path);
  }
}


