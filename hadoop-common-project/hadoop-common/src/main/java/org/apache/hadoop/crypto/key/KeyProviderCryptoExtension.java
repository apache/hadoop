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
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.List;
import java.util.ListIterator;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import com.google.common.base.Preconditions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.Decryptor;
import org.apache.hadoop.crypto.Encryptor;

/**
 * A KeyProvider with Cryptographic Extensions specifically for generating
 * and decrypting encrypted encryption keys.
 * 
 */
@InterfaceAudience.Private
public class KeyProviderCryptoExtension extends
    KeyProviderExtension<KeyProviderCryptoExtension.CryptoExtension> {

  /**
   * Designates an encrypted encryption key, or EEK.
   */
  public static final String EEK = "EEK";
  /**
   * Designates a decrypted encrypted encryption key, that is, an encryption key
   * (EK).
   */
  public static final String EK = "EK";

  /**
   * An encrypted encryption key (EEK) and related information. An EEK must be
   * decrypted using the key's encryption key before it can be used.
   */
  public static class EncryptedKeyVersion {
    private String encryptionKeyName;
    private String encryptionKeyVersionName;
    private byte[] encryptedKeyIv;
    private KeyVersion encryptedKeyVersion;

    /**
     * Create a new EncryptedKeyVersion.
     *
     * @param keyName                  Name of the encryption key used to
     *                                 encrypt the encrypted key.
     * @param encryptionKeyVersionName Version name of the encryption key used
     *                                 to encrypt the encrypted key.
     * @param encryptedKeyIv           Initialization vector of the encrypted
     *                                 key. The IV of the encryption key used to
     *                                 encrypt the encrypted key is derived from
     *                                 this IV.
     * @param encryptedKeyVersion      The encrypted encryption key version.
     */
    protected EncryptedKeyVersion(String keyName,
        String encryptionKeyVersionName, byte[] encryptedKeyIv,
        KeyVersion encryptedKeyVersion) {
      this.encryptionKeyName = keyName == null ? null : keyName.intern();
      this.encryptionKeyVersionName = encryptionKeyVersionName == null ?
          null : encryptionKeyVersionName.intern();
      this.encryptedKeyIv = encryptedKeyIv;
      this.encryptedKeyVersion = encryptedKeyVersion;
    }

    /**
     * Factory method to create a new EncryptedKeyVersion that can then be
     * passed into {@link #decryptEncryptedKey}. Note that the fields of the
     * returned EncryptedKeyVersion will only partially be populated; it is not
     * necessarily suitable for operations besides decryption.
     *
     * @param keyName Key name of the encryption key use to encrypt the
     *                encrypted key.
     * @param encryptionKeyVersionName Version name of the encryption key used
     *                                 to encrypt the encrypted key.
     * @param encryptedKeyIv           Initialization vector of the encrypted
     *                                 key. The IV of the encryption key used to
     *                                 encrypt the encrypted key is derived from
     *                                 this IV.
     * @param encryptedKeyMaterial     Key material of the encrypted key.
     * @return EncryptedKeyVersion suitable for decryption.
     */
    public static EncryptedKeyVersion createForDecryption(String keyName,
        String encryptionKeyVersionName, byte[] encryptedKeyIv,
        byte[] encryptedKeyMaterial) {
      KeyVersion encryptedKeyVersion = new KeyVersion(null, EEK,
          encryptedKeyMaterial);
      return new EncryptedKeyVersion(keyName, encryptionKeyVersionName,
          encryptedKeyIv, encryptedKeyVersion);
    }

    /**
     * @return Name of the encryption key used to encrypt the encrypted key.
     */
    public String getEncryptionKeyName() {
      return encryptionKeyName;
    }

    /**
     * @return Version name of the encryption key used to encrypt the encrypted
     * key.
     */
    public String getEncryptionKeyVersionName() {
      return encryptionKeyVersionName;
    }

    /**
     * @return Initialization vector of the encrypted key. The IV of the
     * encryption key used to encrypt the encrypted key is derived from this
     * IV.
     */
    public byte[] getEncryptedKeyIv() {
      return encryptedKeyIv;
    }

    /**
     * @return The encrypted encryption key version.
     */
    public KeyVersion getEncryptedKeyVersion() {
      return encryptedKeyVersion;
    }

    /**
     * Derive the initialization vector (IV) for the encryption key from the IV
     * of the encrypted key. This derived IV is used with the encryption key to
     * decrypt the encrypted key.
     * <p/>
     * The alternative to this is using the same IV for both the encryption key
     * and the encrypted key. Even a simple symmetric transformation like this
     * improves security by avoiding IV re-use. IVs will also be fairly unique
     * among different EEKs.
     *
     * @param encryptedKeyIV of the encrypted key (i.e. {@link
     * #getEncryptedKeyIv()})
     * @return IV for the encryption key
     */
    protected static byte[] deriveIV(byte[] encryptedKeyIV) {
      byte[] rIv = new byte[encryptedKeyIV.length];
      // Do a simple XOR transformation to flip all the bits
      for (int i = 0; i < encryptedKeyIV.length; i++) {
        rIv[i] = (byte) (encryptedKeyIV[i] ^ 0xff);
      }
      return rIv;
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
     * Drains the Queue for the provided key.
     *
     * @param keyName the key to drain the Queue for
     */
    public void drain(String keyName);

    /**
     * Generates a key material and encrypts it using the given key name.
     * The generated key material is of the same
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
     * Decrypts an encrypted byte[] key material using the given key version
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

    /**
     * Re-encrypts an encrypted key version, using its initialization vector
     * and key material, but with the latest key version name of its key name
     * in the key provider.
     * <p>
     * If the latest key version name in the provider is the
     * same as the one encrypted the passed-in encrypted key version, the same
     * encrypted key version is returned.
     * <p>
     * NOTE: The generated key is not stored by the <code>KeyProvider</code>
     *
     * @param  ekv The EncryptedKeyVersion containing keyVersionName and IV.
     * @return     The re-encrypted EncryptedKeyVersion.
     * @throws IOException If the key material could not be re-encrypted.
     * @throws GeneralSecurityException If the key material could not be
     *                            re-encrypted because of a cryptographic issue.
     */
    EncryptedKeyVersion reencryptEncryptedKey(EncryptedKeyVersion ekv)
        throws IOException, GeneralSecurityException;

    /**
     * Batched version of {@link #reencryptEncryptedKey(EncryptedKeyVersion)}.
     * <p>
     * For each encrypted key version, re-encrypts an encrypted key version,
     * using its initialization vector and key material, but with the latest
     * key version name of its key name. If the latest key version name in the
     * provider is the same as the one encrypted the passed-in encrypted key
     * version, the same encrypted key version is returned.
     * <p>
     * NOTE: The generated key is not stored by the <code>KeyProvider</code>
     *
     * @param  ekvs List containing the EncryptedKeyVersion's
     * @throws IOException If any EncryptedKeyVersion could not be re-encrypted
     * @throws GeneralSecurityException If any EncryptedKeyVersion could not be
     *                            re-encrypted because of a cryptographic issue.
     */
    void reencryptEncryptedKeys(List<EncryptedKeyVersion> ekvs)
        throws IOException, GeneralSecurityException;
  }

  private static class DefaultCryptoExtension implements CryptoExtension {

    private final KeyProvider keyProvider;
    private static final ThreadLocal<SecureRandom> RANDOM = 
        new ThreadLocal<SecureRandom>() {
      @Override
      protected SecureRandom initialValue() {
        return new SecureRandom();
      }
    };

    private DefaultCryptoExtension(KeyProvider keyProvider) {
      this.keyProvider = keyProvider;
    }

    @Override
    public EncryptedKeyVersion generateEncryptedKey(String encryptionKeyName)
        throws IOException, GeneralSecurityException {
      // Fetch the encryption key
      KeyVersion encryptionKey = keyProvider.getCurrentKey(encryptionKeyName);
      Preconditions.checkNotNull(encryptionKey,
          "No KeyVersion exists for key '%s' ", encryptionKeyName);
      // Generate random bytes for new key and IV

      CryptoCodec cc = CryptoCodec.getInstance(keyProvider.getConf());
      try {
        final byte[] newKey = new byte[encryptionKey.getMaterial().length];
        cc.generateSecureRandom(newKey);
        final byte[] iv = new byte[cc.getCipherSuite().getAlgorithmBlockSize()];
        cc.generateSecureRandom(iv);
        Encryptor encryptor = cc.createEncryptor();
        return generateEncryptedKey(encryptor, encryptionKey, newKey, iv);
      } finally {
        cc.close();
      }
    }

    private EncryptedKeyVersion generateEncryptedKey(final Encryptor encryptor,
        final KeyVersion encryptionKey, final byte[] key, final byte[] iv)
        throws IOException, GeneralSecurityException {
      // Encryption key IV is derived from new key's IV
      final byte[] encryptionIV = EncryptedKeyVersion.deriveIV(iv);
      encryptor.init(encryptionKey.getMaterial(), encryptionIV);
      final int keyLen = key.length;
      ByteBuffer bbIn = ByteBuffer.allocateDirect(keyLen);
      ByteBuffer bbOut = ByteBuffer.allocateDirect(keyLen);
      bbIn.put(key);
      bbIn.flip();
      encryptor.encrypt(bbIn, bbOut);
      bbOut.flip();
      byte[] encryptedKey = new byte[keyLen];
      bbOut.get(encryptedKey);
      return new EncryptedKeyVersion(encryptionKey.getName(),
          encryptionKey.getVersionName(), iv,
          new KeyVersion(encryptionKey.getName(), EEK, encryptedKey));
    }

    @Override
    public EncryptedKeyVersion reencryptEncryptedKey(EncryptedKeyVersion ekv)
        throws IOException, GeneralSecurityException {
      final String ekName = ekv.getEncryptionKeyName();
      final KeyVersion ekNow = keyProvider.getCurrentKey(ekName);
      Preconditions
          .checkNotNull(ekNow, "KeyVersion name '%s' does not exist", ekName);
      Preconditions.checkArgument(ekv.getEncryptedKeyVersion().getVersionName()
              .equals(KeyProviderCryptoExtension.EEK),
          "encryptedKey version name must be '%s', but found '%s'",
          KeyProviderCryptoExtension.EEK,
          ekv.getEncryptedKeyVersion().getVersionName());

      if (ekv.getEncryptedKeyVersion().equals(ekNow)) {
        // no-op if same key version
        return ekv;
      }

      final KeyVersion dek = decryptEncryptedKey(ekv);
      final CryptoCodec cc = CryptoCodec.getInstance(keyProvider.getConf());
      try {
        final Encryptor encryptor = cc.createEncryptor();
        return generateEncryptedKey(encryptor, ekNow, dek.getMaterial(),
            ekv.getEncryptedKeyIv());
      } finally {
        cc.close();
      }
    }

    @Override
    public void reencryptEncryptedKeys(List<EncryptedKeyVersion> ekvs)
        throws IOException, GeneralSecurityException {
      Preconditions.checkNotNull(ekvs, "Input list is null");
      KeyVersion ekNow = null;
      Decryptor decryptor = null;
      Encryptor encryptor = null;
      try (CryptoCodec cc = CryptoCodec.getInstance(keyProvider.getConf())) {
        decryptor = cc.createDecryptor();
        encryptor = cc.createEncryptor();
        ListIterator<EncryptedKeyVersion> iter = ekvs.listIterator();
        while (iter.hasNext()) {
          final EncryptedKeyVersion ekv = iter.next();
          Preconditions.checkNotNull(ekv, "EncryptedKeyVersion is null");
          final String ekName = ekv.getEncryptionKeyName();
          Preconditions.checkNotNull(ekName, "Key name is null");
          Preconditions.checkNotNull(ekv.getEncryptedKeyVersion(),
              "EncryptedKeyVersion is null");
          Preconditions.checkArgument(
              ekv.getEncryptedKeyVersion().getVersionName()
                  .equals(KeyProviderCryptoExtension.EEK),
              "encryptedKey version name must be '%s', but found '%s'",
              KeyProviderCryptoExtension.EEK,
              ekv.getEncryptedKeyVersion().getVersionName());

          if (ekNow == null) {
            ekNow = keyProvider.getCurrentKey(ekName);
            Preconditions
                .checkNotNull(ekNow, "Key name '%s' does not exist", ekName);
          } else {
            Preconditions.checkArgument(ekNow.getName().equals(ekName),
                "All keys must have the same key name. Expected '%s' "
                    + "but found '%s'", ekNow.getName(), ekName);
          }

          final String encryptionKeyVersionName =
              ekv.getEncryptionKeyVersionName();
          final KeyVersion encryptionKey =
              keyProvider.getKeyVersion(encryptionKeyVersionName);
          Preconditions.checkNotNull(encryptionKey,
              "KeyVersion name '%s' does not exist", encryptionKeyVersionName);
          if (encryptionKey.equals(ekNow)) {
            // no-op if same key version
            continue;
          }

          final KeyVersion ek =
              decryptEncryptedKey(decryptor, encryptionKey, ekv);
          iter.set(generateEncryptedKey(encryptor, ekNow, ek.getMaterial(),
              ekv.getEncryptedKeyIv()));
        }
      }
    }

    private KeyVersion decryptEncryptedKey(final Decryptor decryptor,
        final KeyVersion encryptionKey,
        final EncryptedKeyVersion encryptedKeyVersion)
        throws IOException, GeneralSecurityException {
      // Encryption key IV is determined from encrypted key's IV
      final byte[] encryptionIV =
          EncryptedKeyVersion.deriveIV(encryptedKeyVersion.getEncryptedKeyIv());

      decryptor.init(encryptionKey.getMaterial(), encryptionIV);
      final KeyVersion encryptedKV =
          encryptedKeyVersion.getEncryptedKeyVersion();
      int keyLen = encryptedKV.getMaterial().length;
      ByteBuffer bbIn = ByteBuffer.allocateDirect(keyLen);
      ByteBuffer bbOut = ByteBuffer.allocateDirect(keyLen);
      bbIn.put(encryptedKV.getMaterial());
      bbIn.flip();
      decryptor.decrypt(bbIn, bbOut);
      bbOut.flip();
      byte[] decryptedKey = new byte[keyLen];
      bbOut.get(decryptedKey);
      return new KeyVersion(encryptionKey.getName(), EK, decryptedKey);
    }

    @Override
    public KeyVersion decryptEncryptedKey(
        EncryptedKeyVersion encryptedKeyVersion)
        throws IOException, GeneralSecurityException {
      // Fetch the encryption key material
      final String encryptionKeyVersionName =
          encryptedKeyVersion.getEncryptionKeyVersionName();
      final KeyVersion encryptionKey =
          keyProvider.getKeyVersion(encryptionKeyVersionName);
      Preconditions
          .checkNotNull(encryptionKey, "KeyVersion name '%s' does not exist",
              encryptionKeyVersionName);
      Preconditions.checkArgument(
          encryptedKeyVersion.getEncryptedKeyVersion().getVersionName()
              .equals(KeyProviderCryptoExtension.EEK),
          "encryptedKey version name must be '%s', but found '%s'",
          KeyProviderCryptoExtension.EEK,
          encryptedKeyVersion.getEncryptedKeyVersion().getVersionName());

      try (CryptoCodec cc = CryptoCodec.getInstance(keyProvider.getConf())) {
        final Decryptor decryptor = cc.createDecryptor();
        return decryptEncryptedKey(decryptor, encryptionKey,
            encryptedKeyVersion);
      }
    }

    @Override
    public void warmUpEncryptedKeys(String... keyNames)
        throws IOException {
      // NO-OP since the default version does not cache any keys
    }

    @Override
    public void drain(String keyName) {
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
   * Re-encrypts an encrypted key version, using its initialization vector
   * and key material, but with the latest key version name of its key name
   * in the key provider.
   * <p>
   * If the latest key version name in the provider is the
   * same as the one encrypted the passed-in encrypted key version, the same
   * encrypted key version is returned.
   * <p>
   * NOTE: The generated key is not stored by the <code>KeyProvider</code>
   *
   * @param  ekv The EncryptedKeyVersion containing keyVersionName and IV.
   * @return     The re-encrypted EncryptedKeyVersion.
   * @throws IOException If the key material could not be re-encrypted
   * @throws GeneralSecurityException If the key material could not be
   *                            re-encrypted because of a cryptographic issue.
   */
  public EncryptedKeyVersion reencryptEncryptedKey(EncryptedKeyVersion ekv)
      throws IOException, GeneralSecurityException {
    return getExtension().reencryptEncryptedKey(ekv);
  }

  /**
   * Calls {@link CryptoExtension#drain(String)} for the given key name on the
   * underlying {@link CryptoExtension}.
   *
   * @param keyName
   */
  public void drain(String keyName) {
    getExtension().drain(keyName);
  }

  /**
   * Batched version of {@link #reencryptEncryptedKey(EncryptedKeyVersion)}.
   * <p>
   * For each encrypted key version, re-encrypts an encrypted key version,
   * using its initialization vector and key material, but with the latest
   * key version name of its key name. If the latest key version name in the
   * provider is the same as the one encrypted the passed-in encrypted key
   * version, the same encrypted key version is returned.
   * <p>
   * NOTE: The generated key is not stored by the <code>KeyProvider</code>
   *
   * @param  ekvs List containing the EncryptedKeyVersion's
   * @return      The re-encrypted EncryptedKeyVersion's, in the same order.
   * @throws IOException If any EncryptedKeyVersion could not be re-encrypted
   * @throws GeneralSecurityException If any EncryptedKeyVersion could not be
   *                            re-encrypted because of a cryptographic issue.
   */
  public void reencryptEncryptedKeys(List<EncryptedKeyVersion> ekvs)
      throws IOException, GeneralSecurityException {
    getExtension().reencryptEncryptedKeys(ekvs);
  }

  /**
   * Creates a <code>KeyProviderCryptoExtension</code> using a given
   * {@link KeyProvider}.
   * <p/>
   * If the given <code>KeyProvider</code> implements the
   * {@link CryptoExtension} interface the <code>KeyProvider</code> itself
   * will provide the extension functionality.
   * If the given <code>KeyProvider</code> implements the
   * {@link KeyProviderExtension} interface and the KeyProvider being
   * extended by the <code>KeyProvider</code> implements the
   * {@link CryptoExtension} interface, the KeyProvider being extended will
   * provide the extension functionality. Otherwise, a default extension
   * implementation will be used.
   *
   * @param keyProvider <code>KeyProvider</code> to use to create the
   * <code>KeyProviderCryptoExtension</code> extension.
   * @return a <code>KeyProviderCryptoExtension</code> instance using the
   * given <code>KeyProvider</code>.
   */
  public static KeyProviderCryptoExtension createKeyProviderCryptoExtension(
      KeyProvider keyProvider) {
    CryptoExtension cryptoExtension = null;
    if (keyProvider instanceof CryptoExtension) {
      cryptoExtension = (CryptoExtension) keyProvider;
    } else if (keyProvider instanceof KeyProviderExtension &&
            ((KeyProviderExtension)keyProvider).getKeyProvider() instanceof
                    KeyProviderCryptoExtension.CryptoExtension) {
      KeyProviderExtension keyProviderExtension =
              (KeyProviderExtension)keyProvider;
      cryptoExtension =
              (CryptoExtension)keyProviderExtension.getKeyProvider();
    } else {
      cryptoExtension = new DefaultCryptoExtension(keyProvider);
    }
    return new KeyProviderCryptoExtension(keyProvider, cryptoExtension);
  }

  @Override
  public void close() throws IOException {
    KeyProvider provider = getKeyProvider();
    if (provider != null && provider != this) {
      provider.close();
    }
  }

}
