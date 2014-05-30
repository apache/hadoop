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

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY;

/**
 * Implement the AES-CTR crypto codec using JCE provider.
 */
@InterfaceAudience.Private
public class JCEAESCTRCryptoCodec extends AESCTRCryptoCodec {
  private Configuration conf;
  private String provider;

  public JCEAESCTRCryptoCodec() {
  }
  
  @Override
  public Configuration getConf() {
    return conf;
  }
  
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    provider = conf.get(HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY);
  }

  @Override
  public Encryptor createEncryptor() throws GeneralSecurityException {
    return new JCEAESCTRCipher(Cipher.ENCRYPT_MODE, provider);
  }

  @Override
  public Decryptor createDecryptor() throws GeneralSecurityException {
    return new JCEAESCTRCipher(Cipher.DECRYPT_MODE, provider);
  }
  
  private static class JCEAESCTRCipher implements Encryptor, Decryptor {
    private final Cipher cipher;
    private final int mode;
    private boolean contextReset = false;
    
    public JCEAESCTRCipher(int mode, String provider) 
        throws GeneralSecurityException {
      this.mode = mode;
      if (provider == null || provider.isEmpty()) {
        cipher = Cipher.getInstance("AES/CTR/NoPadding");
      } else {
        cipher = Cipher.getInstance("AES/CTR/NoPadding", provider);
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
