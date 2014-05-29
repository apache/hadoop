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

import com.google.common.base.Preconditions;

public class JCEAESCTRDecryptor implements Decryptor {
  private final Cipher cipher;
  private boolean contextReset = false;
  
  public JCEAESCTRDecryptor(String provider) throws GeneralSecurityException {
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
      cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"), 
          new IvParameterSpec(iv));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * AES-CTR will consume all of the input data. It requires enough space in
   * the destination buffer to decrypt entire input buffer.
   */
  @Override
  public void decrypt(ByteBuffer inBuffer, ByteBuffer outBuffer)
      throws IOException {
    try {
      int inputSize = inBuffer.remaining();
      // Cipher#update will maintain decryption context.
      int n = cipher.update(inBuffer, outBuffer);
      if (n < inputSize) {
        /**
         * Typically code will not get here. Cipher#update will decrypt all 
         * input data and put result in outBuffer. 
         * Cipher#doFinal will reset the decryption context.
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
