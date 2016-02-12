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

package org.apache.hadoop.crypto.key.kms.server;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.kms.ValueQueue;
import org.apache.hadoop.crypto.key.kms.ValueQueue.SyncGenerationPolicy;

/**
 * A {@link KeyProviderCryptoExtension} that pre-generates and caches encrypted 
 * keys.
 */
@InterfaceAudience.Private
public class EagerKeyGeneratorKeyProviderCryptoExtension 
    extends KeyProviderCryptoExtension {

  private static final String KEY_CACHE_PREFIX =
      "hadoop.security.kms.encrypted.key.cache.";

  public static final String KMS_KEY_CACHE_SIZE =
      KEY_CACHE_PREFIX + "size";
  public static final int KMS_KEY_CACHE_SIZE_DEFAULT = 100;

  public static final String KMS_KEY_CACHE_LOW_WATERMARK =
      KEY_CACHE_PREFIX + "low.watermark";
  public static final float KMS_KEY_CACHE_LOW_WATERMARK_DEFAULT = 0.30f;

  public static final String KMS_KEY_CACHE_EXPIRY_MS =
      KEY_CACHE_PREFIX + "expiry";
  public static final int KMS_KEY_CACHE_EXPIRY_DEFAULT = 43200000;

  public static final String KMS_KEY_CACHE_NUM_REFILL_THREADS =
      KEY_CACHE_PREFIX + "num.fill.threads";
  public static final int KMS_KEY_CACHE_NUM_REFILL_THREADS_DEFAULT = 2;


  private static class CryptoExtension 
      implements KeyProviderCryptoExtension.CryptoExtension {

    private class EncryptedQueueRefiller implements
        ValueQueue.QueueRefiller<EncryptedKeyVersion> {

      @Override
      public void fillQueueForKey(String keyName,
          Queue<EncryptedKeyVersion> keyQueue, int numKeys) throws IOException {
        List<EncryptedKeyVersion> retEdeks =
            new LinkedList<EncryptedKeyVersion>();
        for (int i = 0; i < numKeys; i++) {
          try {
            retEdeks.add(keyProviderCryptoExtension.generateEncryptedKey(
                keyName));
          } catch (GeneralSecurityException e) {
            throw new IOException(e);
          }
        }
        keyQueue.addAll(retEdeks);
      }
    }

    private KeyProviderCryptoExtension keyProviderCryptoExtension;
    private final ValueQueue<EncryptedKeyVersion> encKeyVersionQueue;

    public CryptoExtension(Configuration conf, 
        KeyProviderCryptoExtension keyProviderCryptoExtension) {
      this.keyProviderCryptoExtension = keyProviderCryptoExtension;
      encKeyVersionQueue =
          new ValueQueue<KeyProviderCryptoExtension.EncryptedKeyVersion>(
              conf.getInt(KMS_KEY_CACHE_SIZE,
                  KMS_KEY_CACHE_SIZE_DEFAULT),
              conf.getFloat(KMS_KEY_CACHE_LOW_WATERMARK,
                  KMS_KEY_CACHE_LOW_WATERMARK_DEFAULT),
              conf.getInt(KMS_KEY_CACHE_EXPIRY_MS,
                  KMS_KEY_CACHE_EXPIRY_DEFAULT),
              conf.getInt(KMS_KEY_CACHE_NUM_REFILL_THREADS,
                  KMS_KEY_CACHE_NUM_REFILL_THREADS_DEFAULT),
              SyncGenerationPolicy.LOW_WATERMARK, new EncryptedQueueRefiller()
          );
    }

    @Override
    public void warmUpEncryptedKeys(String... keyNames) throws
                                                        IOException {
      try {
        encKeyVersionQueue.initializeQueuesForKeys(keyNames);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void drain(String keyName) {
      encKeyVersionQueue.drain(keyName);
    }

    @Override
    public EncryptedKeyVersion generateEncryptedKey(String encryptionKeyName)
        throws IOException, GeneralSecurityException {
      try {
        return encKeyVersionQueue.getNext(encryptionKeyName);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    }

    @Override
    public KeyVersion
    decryptEncryptedKey(EncryptedKeyVersion encryptedKeyVersion)
        throws IOException, GeneralSecurityException {
      return keyProviderCryptoExtension.decryptEncryptedKey(
          encryptedKeyVersion);
    }
  }

  /**
   * This class is a proxy for a <code>KeyProviderCryptoExtension</code> that
   * decorates the underlying <code>CryptoExtension</code> with one that eagerly
   * caches pre-generated Encrypted Keys using a <code>ValueQueue</code>
   * 
   * @param conf Configuration object to load parameters from
   * @param keyProviderCryptoExtension <code>KeyProviderCryptoExtension</code>
   * to delegate calls to.
   */
  public EagerKeyGeneratorKeyProviderCryptoExtension(Configuration conf,
      KeyProviderCryptoExtension keyProviderCryptoExtension) {
    super(keyProviderCryptoExtension, 
        new CryptoExtension(conf, keyProviderCryptoExtension));
  }

  /**
   * Roll a new version of the given key generating the material for it.
   * <p>
   * Due to the caching on the ValueQueue, even after a rollNewVersion call,
   * {@link #generateEncryptedKey(String)} may still return an old key - even
   * when we drain the queue here, the async thread may later fill in old keys.
   * This is acceptable since old version keys are still able to decrypt, and
   * client shall make no assumptions that it will get a new versioned key
   * after rollNewVersion.
   */
  @Override
  public KeyVersion rollNewVersion(String name)
      throws NoSuchAlgorithmException, IOException {
    KeyVersion keyVersion = super.rollNewVersion(name);
    getExtension().drain(name);
    return keyVersion;
  }

  @Override
  public KeyVersion rollNewVersion(String name, byte[] material)
      throws IOException {
    KeyVersion keyVersion = super.rollNewVersion(name, material);
    getExtension().drain(name);
    return keyVersion;
  }
}
