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

@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class AesCtrCryptoCodec extends CryptoCodec {

  protected static final CipherSuite SUITE = CipherSuite.AES_CTR_NOPADDING;

  /**
   * For AES, the algorithm block is fixed size of 128 bits.
   * @see http://en.wikipedia.org/wiki/Advanced_Encryption_Standard
   */
  private static final int AES_BLOCK_SIZE = SUITE.getAlgorithmBlockSize();
  private static final int CTR_OFFSET = 8;

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
    
    System.arraycopy(initIV, 0, IV, 0, CTR_OFFSET);
    long l = 0;
    for (int i = 0; i < 8; i++) {
      l = ((l << 8) | (initIV[CTR_OFFSET + i] & 0xff));
    }
    l += counter;
    IV[CTR_OFFSET + 0] = (byte) (l >>> 56);
    IV[CTR_OFFSET + 1] = (byte) (l >>> 48);
    IV[CTR_OFFSET + 2] = (byte) (l >>> 40);
    IV[CTR_OFFSET + 3] = (byte) (l >>> 32);
    IV[CTR_OFFSET + 4] = (byte) (l >>> 24);
    IV[CTR_OFFSET + 5] = (byte) (l >>> 16);
    IV[CTR_OFFSET + 6] = (byte) (l >>> 8);
    IV[CTR_OFFSET + 7] = (byte) (l);
  }
}
