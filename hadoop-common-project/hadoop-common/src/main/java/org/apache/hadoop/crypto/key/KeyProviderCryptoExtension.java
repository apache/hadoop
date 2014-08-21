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
import java.security.GeneralSecurityException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A KeyProvider with Cytographic Extensions specifically for generating
 * Encrypted Keys as well as decrypting them
 *
 */
@InterfaceAudience.Private
public class KeyProviderCryptoExtension extends
    KeyProviderExtension<KeyProviderCryptoExtension.CryptoExtension> {

  public static final String EEK = "EEK";
  public static final String EK = "EK";

  /**
   * This is a holder class whose instance contains the keyVersionName, iv
   * used to generate the encrypted Key and the encrypted KeyVersion
   */
  public static class EncryptedKeyVersion {
    private String keyName;
    private String keyVersionName;
    private byte[] iv;
    private KeyVersion encryptedKey;

    protected EncryptedKeyVersion(String keyName, String keyVersionName,
        byte[] iv, KeyVersion encryptedKey) {
      this.keyName = keyName;
      this.keyVersionName = keyVersionName;
      this.iv = iv;
      this.encryptedKey = encryptedKey;
    }

    public String getKeyName() {
      return keyName;
    }

    public String getKeyVersionName() {
      return keyVersionName;
    }

    public byte[] getIv() {
      return iv;
    }

    public KeyVersion getEncryptedKey() {
      return encryptedKey;
    }

  }

  /**
   * CryptoExtension is a type of Extension that exposes methods to generate
   * EncryptedKeys and to decrypt the same.
   */
  public interface CryptoExtension extends KeyProviderExtension.Extension {

    /**
     * Calls to this method allows the underlying KeyProvider to warm-up any
     * implementation specific caches used to store the Encrypted Keys.
     * @param keyNames Array of Key Names
     */
    public void warmUpEncryptedKeys(String... keyNames)
        throws IOException;

    /**
     * Generates a key material and encrypts it using the given key version name
     * and initialization vector. The generated key material is of the same
     * length as the <code>KeyVersion</code> material of the latest key version
     * of the key and is encrypted using the same cipher.
     * <p/>
     * NOTE: The generated key is not stored by the <code>KeyProvider</code>
     *
     * @param encryptionKeyName
     *          The latest KeyVersion of this key's material will be encrypted.
     * @return EncryptedKeyVersion with the generated key material, the version
     *         name is 'EEK' (for Encrypted Encryption Key)
     * @throws IOException
     *           thrown if the key material could not be generated
     * @throws GeneralSecurityException
     *           thrown if the key material could not be encrypted because of a
     *           cryptographic issue.
     */
    public EncryptedKeyVersion generateEncryptedKey(
        String encryptionKeyName) throws IOException,
        GeneralSecurityException;

    /**
     * Decrypts an encrypted byte[] key material using the given a key version
     * name and initialization vector.
     *
     * @param encryptedKeyVersion
     *          contains keyVersionName and IV to decrypt the encrypted key
     *          material
     * @return a KeyVersion with the decrypted key material, the version name is
     *         'EK' (For Encryption Key)
     * @throws IOException
     *           thrown if the key material could not be decrypted
     * @throws GeneralSecurityException
     *           thrown if the key material could not be decrypted because of a
     *           cryptographic issue.
     */
    public KeyVersion decryptEncryptedKey(
        EncryptedKeyVersion encryptedKeyVersion) throws IOException,
        GeneralSecurityException;
  }

  private static class DefaultCryptoExtension implements CryptoExtension {

    private final KeyProvider keyProvider;

    private DefaultCryptoExtension(KeyProvider keyProvider) {
      this.keyProvider = keyProvider;
    }

    // the IV used to encrypt a EK typically will be the same IV used to
    // encrypt data with the EK. To avoid any chance of weakening the
    // encryption because the same IV is used, we simply XOR the IV thus we
    // are not using the same IV for 2 different encryptions (even if they
    // are done using different keys)
    private byte[] flipIV(byte[] iv) {
      byte[] rIv = new byte[iv.length];
      for (int i = 0; i < iv.length; i++) {
        rIv[i] = (byte) (iv[i] ^ 0xff);
      }
      return rIv;
    }

    @Override
    public EncryptedKeyVersion generateEncryptedKey(String encryptionKeyName)
        throws IOException, GeneralSecurityException {
      KeyVersion keyVer = keyProvider.getCurrentKey(encryptionKeyName);
      Preconditions.checkNotNull(keyVer, "No KeyVersion exists for key '%s' ",
          encryptionKeyName);
      byte[] newKey = new byte[keyVer.getMaterial().length];
      SecureRandom.getInstance("SHA1PRNG").nextBytes(newKey);
      Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
      byte[] iv = SecureRandom.getSeed(cipher.getBlockSize());
      cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(keyVer.getMaterial(),
          "AES"), new IvParameterSpec(flipIV(iv)));
      byte[] ek = cipher.doFinal(newKey);
      return new EncryptedKeyVersion(encryptionKeyName,
          keyVer.getVersionName(), iv,
          new KeyVersion(keyVer.getName(), EEK, ek));
    }

    @Override
    public KeyVersion decryptEncryptedKey(
        EncryptedKeyVersion encryptedKeyVersion) throws IOException,
        GeneralSecurityException {
      KeyVersion keyVer =
          keyProvider.getKeyVersion(encryptedKeyVersion.getKeyVersionName());
      Preconditions.checkNotNull(keyVer, "KeyVersion name '%s' does not exist",
          encryptedKeyVersion.getKeyVersionName());
      KeyVersion keyVersion = encryptedKeyVersion.getEncryptedKey();
      Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
      cipher.init(Cipher.DECRYPT_MODE,
          new SecretKeySpec(keyVersion.getMaterial(), "AES"),
          new IvParameterSpec(flipIV(encryptedKeyVersion.getIv())));
      byte[] ek =
          cipher.doFinal(encryptedKeyVersion.getEncryptedKey().getMaterial());
      return new KeyVersion(keyVer.getName(), EK, ek);
    }

    @Override
    public void warmUpEncryptedKeys(String... keyNames)
        throws IOException {
      // NO-OP since the default version does not cache any keys
    }

  }

  /**
   * This constructor is to be used by sub classes that provide
   * delegating/proxying functionality to the {@link KeyProviderCryptoExtension}
   * @param keyProvider
   * @param extension
   */
  protected KeyProviderCryptoExtension(KeyProvider keyProvider,
      CryptoExtension extension) {
    super(keyProvider, extension);
  }

  /**
   * Notifies the Underlying CryptoExtension implementation to warm up any
   * implementation specific caches for the specified KeyVersions
   * @param keyNames Arrays of key Names
   */
  public void warmUpEncryptedKeys(String... keyNames)
      throws IOException {
    getExtension().warmUpEncryptedKeys(keyNames);
  }

  /**
   * Generates a key material and encrypts it using the given key version name
   * and initialization vector. The generated key material is of the same
   * length as the <code>KeyVersion</code> material and is encrypted using the
   * same cipher.
   * <p/>
   * NOTE: The generated key is not stored by the <code>KeyProvider</code>
   *
   * @param encryptionKeyName The latest KeyVersion of this key's material will
   * be encrypted.
   * @return EncryptedKeyVersion with the generated key material, the version
   * name is 'EEK' (for Encrypted Encryption Key)
   * @throws IOException thrown if the key material could not be generated
   * @throws GeneralSecurityException thrown if the key material could not be
   * encrypted because of a cryptographic issue.
   */
  public EncryptedKeyVersion generateEncryptedKey(String encryptionKeyName)
      throws IOException,
                                           GeneralSecurityException {
    return getExtension().generateEncryptedKey(encryptionKeyName);
  }

  /**
   * Decrypts an encrypted byte[] key material using the given a key version
   * name and initialization vector.
   *
   * @param encryptedKey contains keyVersionName and IV to decrypt the encrypted
   * key material
   * @return a KeyVersion with the decrypted key material, the version name is
   * 'EK' (For Encryption Key)
   * @throws IOException thrown if the key material could not be decrypted
   * @throws GeneralSecurityException thrown if the key material could not be
   * decrypted because of a cryptographic issue.
   */
  public KeyVersion decryptEncryptedKey(EncryptedKeyVersion encryptedKey)
      throws IOException, GeneralSecurityException {
    return getExtension().decryptEncryptedKey(encryptedKey);
  }

  /**
   * Creates a <code>KeyProviderCryptoExtension</code> using a given
   * {@link KeyProvider}.
   * <p/>
   * If the given <code>KeyProvider</code> implements the
   * {@link CryptoExtension} interface the <code>KeyProvider</code> itself
   * will provide the extension functionality, otherwise a default extension
   * implementation will be used.
   *
   * @param keyProvider <code>KeyProvider</code> to use to create the
   * <code>KeyProviderCryptoExtension</code> extension.
   * @return a <code>KeyProviderCryptoExtension</code> instance using the
   * given <code>KeyProvider</code>.
   */
  public static KeyProviderCryptoExtension createKeyProviderCryptoExtension(
      KeyProvider keyProvider) {
    CryptoExtension cryptoExtension = (keyProvider instanceof CryptoExtension)
                         ? (CryptoExtension) keyProvider
                         : new DefaultCryptoExtension(keyProvider);
    return new KeyProviderCryptoExtension(keyProvider, cryptoExtension);
  }

}
