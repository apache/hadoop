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
package org.apache.hadoop.crypto.key;

import java.net.URI;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.hadoop.crypto.key.KeyProvider.KeyVersion;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TestKeyProviderCryptoExtension {

  private static final String CIPHER = "AES";
  private static final String ENCRYPTION_KEY_NAME = "fooKey";

  private static Configuration conf;
  private static KeyProvider kp;
  private static KeyProviderCryptoExtension kpExt;
  private static KeyProvider.Options options;
  private static KeyVersion encryptionKey;

  @BeforeClass
  public static void setup() throws Exception {
    conf = new Configuration();
    kp = new UserProvider.Factory().createProvider(new URI("user:///"), conf);
    kpExt = KeyProviderCryptoExtension.createKeyProviderCryptoExtension(kp);
    options = new KeyProvider.Options(conf);
    options.setCipher(CIPHER);
    options.setBitLength(128);
    encryptionKey =
        kp.createKey(ENCRYPTION_KEY_NAME, SecureRandom.getSeed(16), options);
  }

  @Test
  public void testGenerateEncryptedKey() throws Exception {
    // Generate a new EEK and check it
    KeyProviderCryptoExtension.EncryptedKeyVersion ek1 =
        kpExt.generateEncryptedKey(encryptionKey.getName());
    assertEquals("Version name of EEK should be EEK",
        KeyProviderCryptoExtension.EEK,
        ek1.getEncryptedKeyVersion().getVersionName());
    assertEquals("Name of EEK should be encryption key name",
        ENCRYPTION_KEY_NAME, ek1.getEncryptionKeyName());
    assertNotNull("Expected encrypted key material",
        ek1.getEncryptedKeyVersion().getMaterial());
    assertEquals("Length of encryption key material and EEK material should "
            + "be the same", encryptionKey.getMaterial().length,
        ek1.getEncryptedKeyVersion().getMaterial().length
    );

    // Decrypt EEK into an EK and check it
    KeyVersion k1 = kpExt.decryptEncryptedKey(ek1);
    assertEquals(KeyProviderCryptoExtension.EK, k1.getVersionName());
    assertEquals(encryptionKey.getMaterial().length, k1.getMaterial().length);
    if (Arrays.equals(k1.getMaterial(), encryptionKey.getMaterial())) {
      fail("Encrypted key material should not equal encryption key material");
    }
    if (Arrays.equals(ek1.getEncryptedKeyVersion().getMaterial(),
        encryptionKey.getMaterial())) {
      fail("Encrypted key material should not equal decrypted key material");
    }
    // Decrypt it again and it should be the same
    KeyVersion k1a = kpExt.decryptEncryptedKey(ek1);
    assertArrayEquals(k1.getMaterial(), k1a.getMaterial());

    // Generate another EEK and make sure it's different from the first
    KeyProviderCryptoExtension.EncryptedKeyVersion ek2 =
        kpExt.generateEncryptedKey(encryptionKey.getName());
    KeyVersion k2 = kpExt.decryptEncryptedKey(ek2);
    if (Arrays.equals(k1.getMaterial(), k2.getMaterial())) {
      fail("Generated EEKs should have different material!");
    }
    if (Arrays.equals(ek1.getEncryptedKeyIv(), ek2.getEncryptedKeyIv())) {
      fail("Generated EEKs should have different IVs!");
    }
  }

  @Test
  public void testEncryptDecrypt() throws Exception {
    // Get an EEK
    KeyProviderCryptoExtension.EncryptedKeyVersion eek =
        kpExt.generateEncryptedKey(encryptionKey.getName());
    final byte[] encryptedKeyIv = eek.getEncryptedKeyIv();
    final byte[] encryptedKeyMaterial = eek.getEncryptedKeyVersion()
        .getMaterial();
    // Decrypt it manually
    Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
    cipher.init(Cipher.DECRYPT_MODE,
        new SecretKeySpec(encryptionKey.getMaterial(), "AES"),
        new IvParameterSpec(KeyProviderCryptoExtension.EncryptedKeyVersion
            .deriveIV(encryptedKeyIv)));
    final byte[] manualMaterial = cipher.doFinal(encryptedKeyMaterial);

    // Test the createForDecryption factory method
    EncryptedKeyVersion eek2 =
        EncryptedKeyVersion.createForDecryption(eek.getEncryptionKeyName(),
            eek.getEncryptionKeyVersionName(), eek.getEncryptedKeyIv(),
            eek.getEncryptedKeyVersion().getMaterial());

    // Decrypt it with the API
    KeyVersion decryptedKey = kpExt.decryptEncryptedKey(eek2);
    final byte[] apiMaterial = decryptedKey.getMaterial();

    assertArrayEquals("Wrong key material from decryptEncryptedKey",
        manualMaterial, apiMaterial);
  }
}
