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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.random.OpensslSecureRandom;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Random;
import org.slf4j.Logger;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_SECURE_RANDOM_IMPL_KEY;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class OpensslCtrCryptoCodec extends CryptoCodec{

  private Configuration conf;
  private Random random;
  private String engineId;

  public String getEngineId() {
    return engineId;
  }

  public void setEngineId(String engineId) {
    this.engineId = engineId;
  }

  public Random getRandom() {
    return random;
  }

  public void setRandom(Random random) {
    this.random = random;
  }


  public void calculateIV(byte[] initIV, long counter,
                            byte[] iv, int blockSize) {
    Preconditions.checkArgument(initIV.length == blockSize);
    Preconditions.checkArgument(iv.length == blockSize);

    int i = iv.length; // IV length
    int j = 0; // counter bytes index
    int sum = 0;
    while(i-- > 0){
      // (sum >>> Byte.SIZE) is the carry for condition
      sum = (initIV[i] & 0xff) + (sum >>> Byte.SIZE);
      if (j++ < 8) { // Big-endian, and long is 8 bytes length
        sum += (byte) counter & 0xff;
        counter >>>= 8;
      }
      iv[i] = (byte) sum;
    }
  }

  protected abstract Logger getLogger();

  public void setConf(Configuration conf) {
    this.conf = conf;
    final Class<? extends Random> klass = conf.getClass(
            HADOOP_SECURITY_SECURE_RANDOM_IMPL_KEY,
            OpensslSecureRandom.class,
            Random.class);
    try {
      random = ReflectionUtils.newInstance(klass, conf);
      getLogger().debug("Using " + klass.getName() +
                " as random number generator.");
    } catch (Exception e) {
      getLogger().info("Unable to use " + klass.getName() +
              ".  Falling back to " +
              "Java SecureRandom.", e);
      this.random = new SecureRandom();
    }
  }

  public Configuration getConf() {
    return conf;
  }

  @Override
  public void generateSecureRandom(byte[] bytes) {
    random.nextBytes(bytes);
  }

  @Override
  public void close() throws IOException {
    if (this.random instanceof Closeable) {
      Closeable r = (Closeable) this.random;
      IOUtils.cleanupWithLogger(getLogger(), r);
    }
  }

  protected static class OpensslCtrCipher implements Encryptor, Decryptor {
    private final OpensslCipher cipher;
    private final int mode;
    private boolean contextReset = false;

    public OpensslCtrCipher(int mode, CipherSuite suite, String engineId)
                throws GeneralSecurityException {
      this.mode = mode;
      cipher = OpensslCipher.getInstance(suite.getName(), engineId);
    }

    public OpensslCtrCipher(int mode, CipherSuite suite)
            throws GeneralSecurityException {
      this.mode = mode;
      cipher = OpensslCipher.getInstance(suite.getName());
    }

    @Override
    public void init(byte[] key, byte[] iv) throws IOException {
      Preconditions.checkNotNull(key);
      Preconditions.checkNotNull(iv);
      contextReset = false;
      cipher.init(mode, key, iv);
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
     *  AES-CTR will consume all of the input data. It requires enough space in
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
        // OpensslCipher#update will maintain crypto context.
        int n = cipher.update(inBuffer, outBuffer);
        if (n < inputSize) {
        /**
         * Typically code will not get here. OpensslCipher#update will
         * consume all input data and put result in outBuffer.
         * OpensslCipher#doFinal will reset the crypto context.
         */
          contextReset = true;
          cipher.doFinal(outBuffer);
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
