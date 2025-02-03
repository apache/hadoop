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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.junit.jupiter.api.BeforeAll;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.Timeout;

import static org.apache.hadoop.crypto.key.KeyProvider.KeyVersion;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestKeyProviderCryptoExtension {

  private static final String CIPHER = "AES";
  private static final String ENCRYPTION_KEY_NAME = "fooKey";

  private static Configuration conf;
  private static KeyProvider kp;
  private static KeyProviderCryptoExtension kpExt;
  private static KeyProvider.Options options;
  private static KeyVersion encryptionKey;

  @Rule
  public Timeout testTimeout = new Timeout(180000, TimeUnit.MILLISECONDS);

  @BeforeAll
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
    assertEquals(KeyProviderCryptoExtension.EEK,
        ek1.getEncryptedKeyVersion().getVersionName(), "Version name of EEK should be EEK");
    assertEquals(ENCRYPTION_KEY_NAME, ek1.getEncryptionKeyName(),
        "Name of EEK should be encryption key name");
    assertNotNull(ek1.getEncryptedKeyVersion().getMaterial(), "Expected encrypted key material");
    assertEquals(encryptionKey.getMaterial().length,
        ek1.getEncryptedKeyVersion().getMaterial().length,
        "Length of encryption key material and EEK material should be the same"
    );

    // Decrypt EEK into an EK and check it
    KeyVersion k1 = kpExt.decryptEncryptedKey(ek1);
    assertEquals(KeyProviderCryptoExtension.EK, k1.getVersionName());
    assertEquals(encryptionKey.getMaterial().length, k1.getMaterial().length);

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

    assertArrayEquals(manualMaterial, apiMaterial,
        "Wrong key material from decryptEncryptedKey");
  }

  @Test
  public void testReencryptEncryptedKey() throws Exception {
    // Generate a new EEK
    final KeyProviderCryptoExtension.EncryptedKeyVersion ek1 =
        kpExt.generateEncryptedKey(encryptionKey.getName());

    // Decrypt EEK into an EK and check it
    final KeyVersion k1 = kpExt.decryptEncryptedKey(ek1);
    assertEquals(KeyProviderCryptoExtension.EK, k1.getVersionName());
    assertEquals(encryptionKey.getMaterial().length, k1.getMaterial().length);

    // Roll the EK
    kpExt.rollNewVersion(ek1.getEncryptionKeyName());

    // Reencrypt ek1
    final KeyProviderCryptoExtension.EncryptedKeyVersion ek2 =
        kpExt.reencryptEncryptedKey(ek1);
    assertEquals(KeyProviderCryptoExtension.EEK,
        ek2.getEncryptedKeyVersion().getVersionName(), "Version name of EEK should be EEK");
    assertEquals(ENCRYPTION_KEY_NAME, ek2.getEncryptionKeyName(),
        "Name of EEK should be encryption key name");
    assertNotNull(ek2.getEncryptedKeyVersion().getMaterial(), "Expected encrypted key material");
    assertEquals(encryptionKey.getMaterial().length,
        ek2.getEncryptedKeyVersion().getMaterial().length,
        "Length of encryption key material and EEK material should be the same");

    if (Arrays.equals(ek2.getEncryptedKeyVersion().getMaterial(),
        ek1.getEncryptedKeyVersion().getMaterial())) {
      fail("Re-encrypted EEK should have different material");
    }

    // Decrypt the new EEK into an EK and check it
    final KeyVersion k2 = kpExt.decryptEncryptedKey(ek2);
    assertEquals(KeyProviderCryptoExtension.EK, k2.getVersionName());
    assertEquals(encryptionKey.getMaterial().length, k2.getMaterial().length);

    // Re-encrypting the same EEK with the same EK should be deterministic
    final KeyProviderCryptoExtension.EncryptedKeyVersion ek2a =
        kpExt.reencryptEncryptedKey(ek1);
    assertEquals(KeyProviderCryptoExtension.EEK,
        ek2a.getEncryptedKeyVersion().getVersionName(), "Version name of EEK should be EEK");
    assertEquals(ENCRYPTION_KEY_NAME, ek2a.getEncryptionKeyName(),
        "Name of EEK should be encryption key name");
    assertNotNull(ek2a.getEncryptedKeyVersion().getMaterial(), "Expected encrypted key material");
    assertEquals(encryptionKey.getMaterial().length,
        ek2a.getEncryptedKeyVersion().getMaterial().length,
        "Length of encryption key material and EEK material should be the same");

    if (Arrays.equals(ek2a.getEncryptedKeyVersion().getMaterial(),
        ek1.getEncryptedKeyVersion().getMaterial())) {
      fail("Re-encrypted EEK should have different material");
    }
    assertArrayEquals(ek2.getEncryptedKeyVersion().getMaterial(),
        ek2a.getEncryptedKeyVersion().getMaterial());

    // Re-encrypting an EEK with the same version EK should be no-op
    final KeyProviderCryptoExtension.EncryptedKeyVersion ek3 =
        kpExt.reencryptEncryptedKey(ek2);
    assertEquals(KeyProviderCryptoExtension.EEK,
        ek3.getEncryptedKeyVersion().getVersionName(), "Version name of EEK should be EEK");
    assertEquals(ENCRYPTION_KEY_NAME, ek3.getEncryptionKeyName(),
        "Name of EEK should be encryption key name");
    assertNotNull(ek3.getEncryptedKeyVersion().getMaterial(), "Expected encrypted key material");
    assertEquals(encryptionKey.getMaterial().length,
        ek3.getEncryptedKeyVersion().getMaterial().length,
        "Length of encryption key material and EEK material should be the same");

    if (Arrays.equals(ek3.getEncryptedKeyVersion().getMaterial(),
        ek1.getEncryptedKeyVersion().getMaterial())) {
      fail("Re-encrypted EEK should have different material");
    }
    assertArrayEquals(ek2.getEncryptedKeyVersion().getMaterial(),
        ek3.getEncryptedKeyVersion().getMaterial());
  }

  @Test
  public void testReencryptEncryptedKeys() throws Exception {
    List<EncryptedKeyVersion> ekvs = new ArrayList<>(4);
    // Generate 2 new EEKs @v0 and add to the list
    ekvs.add(kpExt.generateEncryptedKey(encryptionKey.getName()));
    ekvs.add(kpExt.generateEncryptedKey(encryptionKey.getName()));

    // Roll the EK
    kpExt.rollNewVersion(ekvs.get(0).getEncryptionKeyName());
    // Generate 1 new EEK @v1 add to the list.
    ekvs.add(kpExt.generateEncryptedKey(encryptionKey.getName()));

    // Roll the EK again
    kpExt.rollNewVersion(ekvs.get(0).getEncryptionKeyName());
    // Generate 1 new EEK @v2 add to the list.
    ekvs.add(kpExt.generateEncryptedKey(encryptionKey.getName()));

    // leave a deep copy of the original, for verification purpose.
    List<EncryptedKeyVersion> ekvsOrig = new ArrayList<>(ekvs.size());
    for (EncryptedKeyVersion ekv : ekvs) {
      ekvsOrig.add(new EncryptedKeyVersion(ekv.getEncryptionKeyName(),
          ekv.getEncryptionKeyVersionName(), ekv.getEncryptedKeyIv(),
          ekv.getEncryptedKeyVersion()));
    }

    // Reencrypt ekvs
    kpExt.reencryptEncryptedKeys(ekvs);

    // Verify each ekv
    for (int i = 0; i < ekvs.size(); ++i) {
      final EncryptedKeyVersion ekv = ekvs.get(i);
      final EncryptedKeyVersion orig = ekvsOrig.get(i);
      assertEquals(KeyProviderCryptoExtension.EEK,
          ekv.getEncryptedKeyVersion().getVersionName(), "Version name should be EEK");
      assertEquals(ENCRYPTION_KEY_NAME, ekv.getEncryptionKeyName(),
          "Encryption key name should be " + ENCRYPTION_KEY_NAME);
      assertNotNull(ekv.getEncryptedKeyVersion().getMaterial(), "Expected encrypted key material");
      assertEquals(encryptionKey.getMaterial().length,
          ekv.getEncryptedKeyVersion().getMaterial().length,
          "Length of encryption key material and EEK material should be the same");
      assertFalse(Arrays.equals(ekv.getEncryptedKeyVersion().getMaterial(),
          encryptionKey.getMaterial()),
          "Encrypted key material should not equal encryption key material");

      if (i < 3) {
        assertFalse(Arrays.equals(ekv.getEncryptedKeyVersion().getMaterial(),
            orig.getEncryptedKeyVersion().getMaterial()),
            "Re-encrypted EEK should have different material");
      } else {
        assertTrue(Arrays.equals(ekv.getEncryptedKeyVersion().getMaterial(),
            orig.getEncryptedKeyVersion().getMaterial()),
            "Re-encrypted EEK should have same material");
      }

      // Decrypt the new EEK into an EK and check it
      final KeyVersion kv = kpExt.decryptEncryptedKey(ekv);
      assertEquals(KeyProviderCryptoExtension.EK, kv.getVersionName());

      // Decrypt it again and it should be the same
      KeyVersion kv1 = kpExt.decryptEncryptedKey(ekv);
      assertArrayEquals(kv.getMaterial(), kv1.getMaterial());

      // Verify decrypting the new EEK and orig EEK gives the same material.
      final KeyVersion origKv = kpExt.decryptEncryptedKey(orig);
      assertTrue(Arrays.equals(origKv.getMaterial(), kv.getMaterial()),
          "Returned EEK and original EEK should both decrypt to the same kv.");
    }
  }

  @Test
  public void testNonDefaultCryptoExtensionSelectionWithCachingKeyProvider()
      throws Exception {
    Configuration config = new Configuration();
    KeyProvider localKp = new DummyCryptoExtensionKeyProvider(config);
    localKp = new CachingKeyProvider(localKp, 30000, 30000);
    EncryptedKeyVersion localEkv = getEncryptedKeyVersion(config, localKp);
    assertEquals("dummyFakeKey@1", localEkv.getEncryptionKeyVersionName());
  }

  @Test
  public void testDefaultCryptoExtensionSelectionWithCachingKeyProvider()
    throws Exception {
    Configuration config = new Configuration();
    KeyProvider localKp =
            new UserProvider.Factory().
                    createProvider(new URI("user:///"), config);
    localKp = new CachingKeyProvider(localKp, 30000, 30000);
    EncryptedKeyVersion localEkv = getEncryptedKeyVersion(config, localKp);
    assertEquals(ENCRYPTION_KEY_NAME+"@0",
        localEkv.getEncryptionKeyVersionName());
  }

  @Test
  public void testNonDefaultCryptoExtensionSelectionOnKeyProviderExtension()
    throws Exception {
    Configuration config = new Configuration();
    KeyProvider localKp = new UserProvider.Factory().
            createProvider(new URI("user:///"), config);
    localKp = new DummyCachingCryptoExtensionKeyProvider(localKp, 30000, 30000);
    EncryptedKeyVersion localEkv = getEncryptedKeyVersion(config, localKp);
    assertEquals("dummyCachingFakeKey@1",
        localEkv.getEncryptionKeyVersionName());
  }

  private EncryptedKeyVersion getEncryptedKeyVersion(Configuration config,
                                                     KeyProvider localKp)
          throws IOException, GeneralSecurityException {
    KeyProvider.Options localOptions = new KeyProvider.Options(config);
    localOptions.setCipher(CIPHER);
    localOptions.setBitLength(128);
    KeyVersion localEncryptionKey =
            localKp.createKey(ENCRYPTION_KEY_NAME,
                    SecureRandom.getSeed(16), localOptions);
    KeyProviderCryptoExtension localKpExt =
            KeyProviderCryptoExtension.
                    createKeyProviderCryptoExtension(localKp);
    return localKpExt.generateEncryptedKey(localEncryptionKey.getName());
  }

  /**
   * Dummy class to test that this key provider is chosen to
   * provide CryptoExtension services over the DefaultCryptoExtension.
   */
  public class DummyCryptoExtensionKeyProvider extends KeyProvider
          implements KeyProviderCryptoExtension.CryptoExtension {

    private KeyProvider kp;
    private KeyVersion kv;
    private EncryptedKeyVersion ekv;

    public DummyCryptoExtensionKeyProvider(Configuration conf) {
      super(conf);
      conf = new Configuration();
      try {
        this.kp = new UserProvider.Factory().createProvider(
                new URI("user:///"), conf);
        this.kv = new KeyVersion(ENCRYPTION_KEY_NAME,
                "dummyFakeKey@1", new byte[16]);
        this.ekv = new EncryptedKeyVersion(ENCRYPTION_KEY_NAME,
                "dummyFakeKey@1", new byte[16], kv);
      } catch (URISyntaxException e) {
        fail(e.getMessage());
      } catch (IOException e) {
        fail(e.getMessage());
      }
    }

    @Override
    public void warmUpEncryptedKeys(String... keyNames) throws IOException {

    }

    @Override
    public void drain(String keyName) {

    }

    @Override
    public EncryptedKeyVersion generateEncryptedKey(String encryptionKeyName)
            throws IOException, GeneralSecurityException {
      return this.ekv;
    }

    @Override
    public EncryptedKeyVersion reencryptEncryptedKey(EncryptedKeyVersion ekv)
        throws IOException, GeneralSecurityException {
      return ekv;
    }

    @Override
    public void reencryptEncryptedKeys(List<EncryptedKeyVersion> ekvs)
        throws IOException, GeneralSecurityException {
    }

    @Override
    public KeyVersion decryptEncryptedKey(
            EncryptedKeyVersion encryptedKeyVersion)
            throws IOException, GeneralSecurityException {
      return kv;
    }

    @Override
    public KeyVersion getKeyVersion(String versionName)
            throws IOException {
      return this.kp.getKeyVersion(versionName);
    }

    @Override
    public List<String> getKeys() throws IOException {
      return this.kp.getKeys();
    }

    @Override
    public List<KeyVersion> getKeyVersions(String name)
            throws IOException {
      return this.kp.getKeyVersions(name);
    }

    @Override
    public Metadata getMetadata(String name)
            throws IOException {
      return this.kp.getMetadata(name);
    }

    @Override
    public KeyVersion createKey(String name, byte[] material,
                                Options localOptions) throws IOException {
      return this.kp.createKey(name, material, localOptions);
    }

    @Override
    public void deleteKey(String name) throws IOException {
      this.kp.deleteKey(name);
    }

    @Override
    public KeyVersion rollNewVersion(String name,
                                     byte[] material) throws IOException {
      return this.kp.rollNewVersion(name, material);
    }

    @Override
    public void flush() throws IOException {
      this.kp.flush();
    }
  }

  /**
   * Dummy class to verify that CachingKeyProvider is used to
   * provide CryptoExtension services if the CachingKeyProvider itself
   * implements CryptoExtension.
   */
  public class DummyCachingCryptoExtensionKeyProvider
          extends CachingKeyProvider
          implements KeyProviderCryptoExtension.CryptoExtension {
    private KeyProvider kp;
    private KeyVersion kv;
    private EncryptedKeyVersion ekv;

    public DummyCachingCryptoExtensionKeyProvider(KeyProvider keyProvider,
                                                  long keyTimeoutMillis,
                                                  long currKeyTimeoutMillis) {
      super(keyProvider, keyTimeoutMillis, currKeyTimeoutMillis);
      conf = new Configuration();
      try {
        this.kp = new UserProvider.Factory().createProvider(
                new URI("user:///"), conf);
        this.kv = new KeyVersion(ENCRYPTION_KEY_NAME,
                "dummyCachingFakeKey@1", new byte[16]);
        this.ekv = new EncryptedKeyVersion(ENCRYPTION_KEY_NAME,
                "dummyCachingFakeKey@1", new byte[16], kv);
      } catch (URISyntaxException e) {
        fail(e.getMessage());
      } catch (IOException e) {
        fail(e.getMessage());
      }
    }

    @Override
    public void warmUpEncryptedKeys(String... keyNames) throws IOException {

    }

    @Override
    public void drain(String keyName) {

    }

    @Override
    public EncryptedKeyVersion generateEncryptedKey(String encryptionKeyName)
            throws IOException, GeneralSecurityException {
      return this.ekv;
    }

    @Override
    public KeyVersion decryptEncryptedKey(
            EncryptedKeyVersion encryptedKeyVersion)
            throws IOException, GeneralSecurityException {
      return kv;
    }

    @Override
    public EncryptedKeyVersion reencryptEncryptedKey(EncryptedKeyVersion ekv)
        throws IOException, GeneralSecurityException {
      return ekv;
    }

    @Override
    public void reencryptEncryptedKeys(List<EncryptedKeyVersion> ekvs)
        throws IOException, GeneralSecurityException {
    }
  }
}
