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

package org.apache.hadoop.fs.azurebfs.security;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.SecretKey;
import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;

import org.apache.hadoop.util.Preconditions;

import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;

public class EncryptionAdapter implements Destroyable {
  private final String path;
  private SecretKey encryptionContext;
  private SecretKey encryptionKey;
  private final EncryptionContextProvider provider;
  private String encodedKey = null;
  private String encodedKeySHA = null;

  public EncryptionAdapter(EncryptionContextProvider provider, String path,
      byte[] encryptionContext) throws IOException {
    this(provider, path);
    Preconditions.checkNotNull(encryptionContext,
        "Encryption context should not be null.");
    this.encryptionContext = new ABFSKey(Base64.getDecoder().decode(encryptionContext));
  }

  public EncryptionAdapter(EncryptionContextProvider provider, String path)
      throws IOException {
    this.provider = provider;
    this.path = path;
  }

  public SecretKey getEncryptionKey() throws IOException {
    if (encryptionKey != null) {
      return encryptionKey;
    }
    encryptionKey = provider.getEncryptionKey(path, encryptionContext);
    return encryptionKey;
  }

  public SecretKey createEncryptionContext() throws IOException {
    encryptionContext = provider.getEncryptionContext(path);
    Preconditions.checkNotNull(encryptionContext,
        "Encryption context should not be null.");
    return encryptionContext;
  }

  public void computeKeys() throws IOException {
    SecretKey key = getEncryptionKey();
    Preconditions.checkNotNull(key, "Encryption key should not be null.");
    encodedKey = getBase64EncodedString(key.getEncoded());
    encodedKeySHA = getBase64EncodedString(getSHA256Hash(key.getEncoded()));
  }

  public String getEncodedKey() throws IOException {
    if (encodedKey == null) {
      computeKeys();
    }
    return encodedKey;
  }

  public String getEncodedKeySHA() throws IOException {
    if (encodedKeySHA == null) {
      computeKeys();
    }
    return encodedKeySHA;
  }

  public void destroy() throws DestroyFailedException {
    encryptionKey.destroy();
    provider.destroy();
  }

  public class ABFSKey implements SecretKey {
    private final byte[] bytes;
    public ABFSKey(byte[] bytes) {
      this.bytes = bytes;
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
      return bytes;
    }

    @Override
    public void destroy() {
      Arrays.fill(bytes, (byte) 0);
    }
  }

  public static byte[] getSHA256Hash(byte[] key) throws IOException {
    try {
      final MessageDigest digester = MessageDigest.getInstance("SHA-256");
      return digester.digest(key);
    } catch (NoSuchAlgorithmException e) {
      throw new IOException(e);
    }
  }

  public static String getBase64EncodedString(byte[] bytes) {
    return Base64.getEncoder().encodeToString(bytes);
  }
}
