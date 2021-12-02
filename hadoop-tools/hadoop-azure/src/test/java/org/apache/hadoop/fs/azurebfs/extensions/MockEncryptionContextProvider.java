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

import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.classification.VisibleForTesting;

import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.ENCRYPTION_KEY_LEN;

public class MockEncryptionContextProvider implements EncryptionContextProvider {
  private HashMap<String, String> pathToContextMap = new HashMap<>();
  private HashMap<String, Key> contextToKeyMap = new HashMap<>();
  @Override
  public void initialize(Configuration configuration, String accountName,
      String fileSystem) throws IOException {
  }

  @Override
  public SecretKey getEncryptionContext(String path)
      throws IOException {
    String newContext = UUID.randomUUID().toString();
    pathToContextMap.put(path, newContext);
    byte[] newKey = new byte[ENCRYPTION_KEY_LEN];
    new Random().nextBytes(newKey);
    Key key = new Key(newKey);
    contextToKeyMap.put(newContext, key);
    return new Key(newContext.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public SecretKey getEncryptionKey(String path,
      SecretKey encryptionContext) throws IOException {
    String encryptionContextString =
        new String(encryptionContext.getEncoded(), StandardCharsets.UTF_8);
    if (!encryptionContextString.equals(pathToContextMap.get(path))) {
      throw new IOException("encryption context does not match path");
    }
    return contextToKeyMap.get(encryptionContextString);
  }

  @Override
  public void destroy() {
    pathToContextMap = null;
    for (Key encryptionKey : contextToKeyMap.values()) {
      encryptionKey.destroy();
    }
    contextToKeyMap = null;
  }

  class Key implements SecretKey {

    private final byte[] key;

    Key(byte[] secret) {
      key = secret;
    }
    @Override
    public String getAlgorithm() {
      return null;
    }

    @Override
    public String getFormat() {
      return null;
    }

    @Override
    public byte[] getEncoded() {
      return key;
    }

    @Override
    public void destroy() {
      Arrays.fill(key, (byte) 0);
    }
  }

  @VisibleForTesting
  public byte[] getEncryptionKeyForTest(String encryptionContext) {
    return contextToKeyMap.get(encryptionContext).getEncoded();
  }

  @VisibleForTesting
  public String getEncryptionContextForTest(String path) {
    return pathToContextMap.get(path);
  }
}


