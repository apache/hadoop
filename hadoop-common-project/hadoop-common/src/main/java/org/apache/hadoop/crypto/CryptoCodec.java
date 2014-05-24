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
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class CryptoCodec implements Configurable {
  
  public static CryptoCodec getInstance(Configuration conf) {
    final Class<? extends CryptoCodec> klass = conf.getClass(
        HADOOP_SECURITY_CRYPTO_CODEC_CLASS_KEY, JCEAESCTRCryptoCodec.class, 
        CryptoCodec.class);
    return ReflectionUtils.newInstance(klass, conf);
  }
  
  /**
   * Get block size of a block cipher.
   * For different algorithms, the block size may be different.
   * @return int block size
   */
  public abstract int getAlgorithmBlockSize();

  /**
   * Get a {@link #org.apache.hadoop.crypto.Encryptor}. 
   * @return Encryptor
   */
  public abstract Encryptor getEncryptor() throws GeneralSecurityException;
  
  /**
   * Get a {@link #org.apache.hadoop.crypto.Decryptor}.
   * @return Decryptor
   */
  public abstract Decryptor getDecryptor() throws GeneralSecurityException;
  
  /**
   * This interface is only for Counter (CTR) mode. Typically calculating 
   * IV(Initialization Vector) is up to Encryptor or Decryptor, for 
   * example {@link #javax.crypto.Cipher} will maintain encryption context 
   * internally when do encryption/decryption continuously using its 
   * Cipher#update interface. 
   * <p/>
   * In Hadoop, multiple nodes may read splits of a file, so decrypting of 
   * file is not continuous, even for encrypting may be not continuous. For 
   * each part, we need to calculate the counter through file position.
   * <p/>
   * Typically IV for a file position is produced by combining initial IV and 
   * the counter using any lossless operation (concatenation, addition, or XOR).
   * @see http://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Counter_.28CTR.29
   * 
   * @param initIV initial IV
   * @param counter counter for input stream position 
   * @param IV the IV for input stream position
   */
  public abstract void calculateIV(byte[] initIV, long counter, byte[] IV);
}
