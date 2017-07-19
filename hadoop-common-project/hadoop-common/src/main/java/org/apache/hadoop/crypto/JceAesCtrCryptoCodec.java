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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_DEFAULT;

/**
 * Implement the AES-CTR crypto codec using JCE provider.
 */
@InterfaceAudience.Private
public class JceAesCtrCryptoCodec extends AesCtrCryptoCodec {
  private static final Logger LOG =
      LoggerFactory.getLogger(JceAesCtrCryptoCodec.class.getName());
  
  private Configuration conf;
  private String provider;
  private SecureRandom random;

  public JceAesCtrCryptoCodec() {
  }
  
  @Override
  public Configuration getConf() {
    return conf;
  }
  
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    provider = conf.get(HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY);
    final String secureRandomAlg = conf.get(
        HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, 
        HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_DEFAULT);
    try {
      random = (provider != null) ? 
          SecureRandom.getInstance(secureRandomAlg, provider) : 
            SecureRandom.getInstance(secureRandomAlg);
    } catch (GeneralSecurityException e) {
      LOG.warn(e.getMessage());
      random = new SecureRandom();
    }
  }

  @Override
  public Encryptor createEncryptor() throws GeneralSecurityException {
    return new JceAesCtrCipher(Cipher.ENCRYPT_MODE, provider);
  }

  @Override
  public Decryptor createDecryptor() throws GeneralSecurityException {
    return new JceAesCtrCipher(Cipher.DECRYPT_MODE, provider);
  }
  
  @Override
  public void generateSecureRandom(byte[] bytes) {
    random.nextBytes(bytes);
  }  
  
  private static class JceAesCtrCipher implements Encryptor, Decryptor {
    private final Cipher cipher;
    private final int mode;
    private boolean contextReset = false;
    
    public JceAesCtrCipher(int mode, String provider) 
        throws GeneralSecurityException {
      this.mode = mode;
      if (provider == null || provider.isEmpty()) {
        cipher = Cipher.getInstance(SUITE.getName());
      } else {
        cipher = Cipher.getInstance(SUITE.getName(), provider);
      }
    }

    @Override
    public void init(byte[] key, byte[] iv) throws IOException {
      Preconditions.checkNotNull(key);
      Preconditions.checkNotNull(iv);
      contextReset = false;
      try {
        cipher.init(mode, new SecretKeySpec(key, "AES"), 
            new IvParameterSpec(iv));
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    /**
     * AES-CTR will consume all of the input data. It requires enough space in 
     * the destination buffer to encrypt entire input buffer.
     */
    @Override
    public void encrypt(ByteBuffer inBuffer, ByteBuffer outBuffer)
        throws IOException {
      process(inBuffer, outBuffer);
    }
    
    /**
     * AES-CTR will consume all of the input data. It requires enough space in
     * the destination buffer to decrypt entire input buffer.
     */
    @Override
    public void decrypt(ByteBuffer inBuffer, ByteBuffer outBuffer)
        throws IOException {
      process(inBuffer, outBuffer);
    }
    
    private void process(ByteBuffer inBuffer, ByteBuffer outBuffer)
        throws IOException {
      try {
        int inputSize = inBuffer.remaining();
        // Cipher#update will maintain crypto context.
        int n = cipher.update(inBuffer, outBuffer);
        if (n < inputSize) {
          /**
           * Typically code will not get here. Cipher#update will consume all 
           * input data and put result in outBuffer. 
           * Cipher#doFinal will reset the crypto context.
           */
          contextReset = true;
          cipher.doFinal(inBuffer, outBuffer);
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    
    @Override
    public boolean isContextReset() {
      return contextReset;
    }
  }
}
