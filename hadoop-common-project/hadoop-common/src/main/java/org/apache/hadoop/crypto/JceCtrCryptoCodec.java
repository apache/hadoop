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

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_DEFAULT;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class JceCtrCryptoCodec extends CryptoCodec{
  private Configuration conf;
  private String provider;
  private SecureRandom random;

  public String getProvider() {
    return provider;
  }

  public void calculateIV(byte[] initIV, long counter,
                            byte[] iv, int blockSize) {
    Preconditions.checkArgument(initIV.length == blockSize);
    Preconditions.checkArgument(iv.length == blockSize);

    int i = iv.length; // IV length
    int j = 0; // counter bytes index
    int sum = 0;
    while(i-- > 0) {
    // (sum >>> Byte.SIZE) is the carry for condition
      sum = (initIV[i] & 0xff) + (sum >>> Byte.SIZE);
      if (j++ < 8) { // Big-endian, and long is 8 bytes length
        sum += (byte) counter & 0xff;
        counter >>>= 8;
      }
      iv[i] = (byte) sum;
    }
  }

  public void close() throws IOException {
  }

  protected abstract Logger getLogger();

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    this.provider = CryptoUtils.getJceProvider(conf);

    final String secureRandomAlg =
          conf.get(
              HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY,
              HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_DEFAULT);

    try {
      random = (provider != null && !provider.isEmpty())
            ? SecureRandom.getInstance(secureRandomAlg, provider)
            : SecureRandom.getInstance(secureRandomAlg);
    } catch(GeneralSecurityException e) {
      getLogger().warn(e.getMessage());
      random = new SecureRandom();
    }
  }

  @Override
  public void generateSecureRandom(byte[] bytes) {
    random.nextBytes(bytes);
  }

  protected static class JceCtrCipher implements Encryptor, Decryptor {
    private final Cipher cipher;
    private final int mode;
    private String name;
    private boolean contextReset = false;

    public JceCtrCipher(int mode, String provider,
                        CipherSuite suite, String name)
            throws GeneralSecurityException {

      this.mode = mode;
      this.name = name;
      if(provider == null || provider.isEmpty()) {
        cipher = Cipher.getInstance(suite.getName());
      } else {
        cipher = Cipher.getInstance(suite.getName(), provider);
      }
    }

    public void init(byte[] key, byte[] iv) throws IOException {
      Preconditions.checkNotNull(key);
      Preconditions.checkNotNull(iv);
      contextReset = false;
      try {
        cipher.init(mode, new SecretKeySpec(key, name),
                      new IvParameterSpec(iv));
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    public void encrypt(ByteBuffer inBuffer, ByteBuffer outBuffer)
          throws IOException {
      process(inBuffer, outBuffer);
    }

    public void decrypt(ByteBuffer inBuffer, ByteBuffer outBuffer)
          throws IOException {
      process(inBuffer, outBuffer);
    }

    public void process(ByteBuffer inBuffer, ByteBuffer outBuffer)
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

    public boolean isContextReset() {
      return contextReset;
    }
  }
}
