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

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;

import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;

public class EncryptionAdapter implements Destroyable {
  String path;
  SecretKey encryptionContext;
  SecretKey encryptionKey;
  EncryptionContextProvider provider;
  String encodedKey = null;
  String encodedKeySHA = null;
  private static final Logger LOG =
      LoggerFactory.getLogger(EncryptionAdapter.class);

  public EncryptionAdapter(EncryptionContextProvider provider, String path,
      byte[] encryptionContext) throws IOException {
    this(provider, path);
    Preconditions.checkNotNull(encryptionContext,
        "Encryption context should not be null.");
    this.encryptionContext = new ABFSSecretKey(encryptionContext);
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
    return provider.getEncryptionKey(path, encryptionContext);
  }

  public SecretKey fetchEncryptionContextAndComputeKeys() throws IOException {
    encryptionContext = provider.getEncryptionContext(path);
    computeKeys();
    return encryptionContext;
  }

  public void computeKeys() throws IOException {
    SecretKey key = getEncryptionKey();
    encodedKey = getBase64EncodedString(new String(key.getEncoded(),
        StandardCharsets.UTF_8));
    encodedKeySHA = getBase64EncodedString(getSHA256Hash(new String(key.getEncoded(),
        StandardCharsets.UTF_8)));
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

  public class ABFSSecretKey implements SecretKey {
    final byte[] secret;
    public ABFSSecretKey(byte[] secret) {
      this.secret = secret;
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
      return secret;
    }

    @Override
    public void destroy() {
      Arrays.fill(secret, (byte) 0);
    }
  }

  public static byte[] getSHA256Hash(String key) throws IOException {
    try {
      final MessageDigest digester = MessageDigest.getInstance("SHA-256");
      return digester.digest(key.getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException e) {
      throw new IOException(e);
    }
  }

  public static String getBase64EncodedString(String key) {
    return getBase64EncodedString(key.getBytes(StandardCharsets.UTF_8));
  }

  public static String getBase64EncodedString(byte[] bytes) {
    return Base64.getEncoder().encodeToString(bytes);
  }
}
