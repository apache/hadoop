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

import com.google.common.base.Preconditions;

import java.io.IOException;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class AesCtrCryptoCodec extends CryptoCodec {

  protected static final CipherSuite SUITE = CipherSuite.AES_CTR_NOPADDING;

  /**
   * For AES, the algorithm block is fixed size of 128 bits.
   * @see http://en.wikipedia.org/wiki/Advanced_Encryption_Standard
   */
  private static final int AES_BLOCK_SIZE = SUITE.getAlgorithmBlockSize();

  @Override
  public CipherSuite getCipherSuite() {
    return SUITE;
  }
  
  /**
   * The IV is produced by adding the initial IV to the counter. IV length 
   * should be the same as {@link #AES_BLOCK_SIZE}
   */
  @Override
  public void calculateIV(byte[] initIV, long counter, byte[] IV) {
    Preconditions.checkArgument(initIV.length == AES_BLOCK_SIZE);
    Preconditions.checkArgument(IV.length == AES_BLOCK_SIZE);

    int i = IV.length; // IV length
    int j = 0; // counter bytes index
    int sum = 0;
    while (i-- > 0) {
      // (sum >>> Byte.SIZE) is the carry for addition
      sum = (initIV[i] & 0xff) + (sum >>> Byte.SIZE);
      if (j++ < 8) { // Big-endian, and long is 8 bytes length
        sum += (byte) counter & 0xff;
        counter >>>= 8;
      }
      IV[i] = (byte) sum;
    }
  }

  @Override
  public void close() throws IOException {
  }
}
