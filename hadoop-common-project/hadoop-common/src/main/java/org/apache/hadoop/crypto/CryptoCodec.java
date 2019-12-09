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

import java.io.Closeable;
import java.security.GeneralSecurityException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.util.PerformanceAdvisory;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_DEFAULT;

/**
 * Crypto codec class, encapsulates encryptor/decryptor pair.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class CryptoCodec implements Configurable, Closeable {
  public static Logger LOG = LoggerFactory.getLogger(CryptoCodec.class);
  
  /**
   * Get crypto codec for specified algorithm/mode/padding.
   * 
   * @param conf
   *          the configuration
   * @param cipherSuite
   *          algorithm/mode/padding
   * @return CryptoCodec the codec object. Null value will be returned if no
   *         crypto codec classes with cipher suite configured.
   */
  public static CryptoCodec getInstance(Configuration conf, 
      CipherSuite cipherSuite) {
    List<Class<? extends CryptoCodec>> klasses = getCodecClasses(
        conf, cipherSuite);
    if (klasses == null) {
      return null;
    }
    CryptoCodec codec = null;
    for (Class<? extends CryptoCodec> klass : klasses) {
      try {
        CryptoCodec c = ReflectionUtils.newInstance(klass, conf);
        if (c.getCipherSuite().getName().equals(cipherSuite.getName())) {
          if (codec == null) {
            PerformanceAdvisory.LOG.debug("Using crypto codec {}.",
                klass.getName());
            codec = c;
          }
        } else {
          PerformanceAdvisory.LOG.debug(
              "Crypto codec {} doesn't meet the cipher suite {}.",
              klass.getName(), cipherSuite.getName());
        }
      } catch (Exception e) {
        PerformanceAdvisory.LOG.debug("Crypto codec {} is not available.",
            klass.getName());
      }
    }
    
    return codec;
  }
  
  /**
   * Get crypto codec for algorithm/mode/padding in config value
   * hadoop.security.crypto.cipher.suite
   * 
   * @param conf
   *          the configuration
   * @return CryptoCodec the codec object Null value will be returned if no
   *         crypto codec classes with cipher suite configured.
   */
  public static CryptoCodec getInstance(Configuration conf) {
    String name = conf.get(HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY, 
        HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_DEFAULT);
    return getInstance(conf, CipherSuite.convert(name));
  }
  
  private static List<Class<? extends CryptoCodec>> getCodecClasses(
      Configuration conf, CipherSuite cipherSuite) {
    List<Class<? extends CryptoCodec>> result = Lists.newArrayList();
    String configName = HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX + 
        cipherSuite.getConfigSuffix();
    String codecString;
    if (configName.equals(CommonConfigurationKeysPublic
        .HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_AES_CTR_NOPADDING_KEY)) {
      codecString = conf.get(configName, CommonConfigurationKeysPublic
          .HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_AES_CTR_NOPADDING_DEFAULT);
    } else {
      codecString = conf.get(configName);
    }
    if (codecString == null) {
      PerformanceAdvisory.LOG.debug(
          "No crypto codec classes with cipher suite configured.");
      return null;
    }
    for (String c : Splitter.on(',').trimResults().omitEmptyStrings().
        split(codecString)) {
      try {
        Class<?> cls = conf.getClassByName(c);
        result.add(cls.asSubclass(CryptoCodec.class));
      } catch (ClassCastException e) {
        PerformanceAdvisory.LOG.debug("Class {} is not a CryptoCodec.", c);
      } catch (ClassNotFoundException e) {
        PerformanceAdvisory.LOG.debug("Crypto codec {} not found.", c);
      }
    }
    
    return result;
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
