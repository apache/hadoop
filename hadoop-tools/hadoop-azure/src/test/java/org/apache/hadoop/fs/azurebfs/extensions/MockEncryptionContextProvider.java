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

import org.apache.hadoop.conf.Configuration;

public class MockEncryptionContextProvider implements EncryptionContextProvider {
  private String dummyKey = "12345678901234567890123456789012";
  private HashMap<String, String> pathToContextMap = new HashMap<>();
  private HashMap<String, Key> contextToKeyMap = new HashMap<>();
  @Override
  public void initialize(Configuration configuration, String accountName,
      String fileSystem) throws IOException {
  }

  @Override
  public SecretKey getEncryptionContext(String path)
      throws IOException {
    String newContext = "context";
    pathToContextMap.put(path, newContext);
    // String key = UUID.randomUUID().toString();
    Key key = new Key(dummyKey.getBytes(StandardCharsets.UTF_8));
    // replace dummyKey with key above once server supports
    contextToKeyMap.put(newContext, key);
    return new Key(newContext.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public SecretKey getEncryptionKey(String path,
      SecretKey encryptionContext) throws IOException {
    if (!new String(encryptionContext.getEncoded()).equals(pathToContextMap.get(path))) {
      throw new IOException("encryption context does not match path");
    }
    return contextToKeyMap.get(new String(encryptionContext.getEncoded()));
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
}


