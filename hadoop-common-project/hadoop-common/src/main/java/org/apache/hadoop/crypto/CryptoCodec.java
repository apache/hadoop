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
package org.apache.hadoop.crypto;

import java.security.GeneralSecurityException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CODEC_CLASS_KEY;

/**
 * Crypto codec class, encapsulates encryptor/decryptor pair.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class CryptoCodec implements Configurable {
  
  public static CryptoCodec getInstance(Configuration conf) {
    final Class<? extends CryptoCodec> klass = conf.getClass(
        HADOOP_SECURITY_CRYPTO_CODEC_CLASS_KEY, JceAesCtrCryptoCodec.class, 
        CryptoCodec.class);
    return ReflectionUtils.newInstance(klass, conf);
  }

  /**
   * @return the CipherSuite for this codec.
   */
  public abstract CipherSuite getCipherSuite();

  /**
   * Create a {@link org.apache.hadoop.crypto.Encryptor}. 
   * @return Encryptor the encryptor
   */
  public abstract Encryptor createEncryptor() throws GeneralSecurityException;
  
  /**
   * Create a {@link org.apache.hadoop.crypto.Decryptor}.
   * @return Decryptor the decryptor
   */
  public abstract Decryptor createDecryptor() throws GeneralSecurityException;
  
  /**
   * This interface is only for Counter (CTR) mode. Generally the Encryptor
   * or Decryptor calculates the IV and maintain encryption context internally. 
   * For example a {@link javax.crypto.Cipher} will maintain its encryption 
   * context internally when we do encryption/decryption using the 
   * Cipher#update interface. 
   * <p/>
   * Encryption/Decryption is not always on the entire file. For example,
   * in Hadoop, a node may only decrypt a portion of a file (i.e. a split).
   * In these situations, the counter is derived from the file position.
   * <p/>
   * The IV can be calculated by combining the initial IV and the counter with 
   * a lossless operation (concatenation, addition, or XOR).
   * @see http://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Counter_.28CTR.29
   * 
   * @param initIV initial IV
   * @param counter counter for input stream position 
   * @param IV the IV for input stream position
   */
  public abstract void calculateIV(byte[] initIV, long counter, byte[] IV);
  
  /**
   * Generate a number of secure, random bytes suitable for cryptographic use.
   * This method needs to be thread-safe.
   *
   * @param bytes byte array to populate with random data
   */
  public abstract void generateSecureRandom(byte[] bytes);
}
