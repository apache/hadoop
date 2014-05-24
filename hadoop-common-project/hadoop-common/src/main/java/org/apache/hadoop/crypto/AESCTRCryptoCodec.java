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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Preconditions;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class AESCTRCryptoCodec extends CryptoCodec {
  /**
   * For AES, the algorithm block is fixed size of 128 bits.
   * @see http://en.wikipedia.org/wiki/Advanced_Encryption_Standard
   */
  private static final int AES_BLOCK_SIZE = 16;

  @Override
  public int getAlgorithmBlockSize() {
    return AES_BLOCK_SIZE;
  }
  
  /**
   * IV is produced by combining initial IV and the counter using addition.
   * IV length should be the same as {@link #AES_BLOCK_SIZE}
   */
  @Override
  public void calculateIV(byte[] initIV, long counter, byte[] IV) {
    Preconditions.checkArgument(initIV.length == AES_BLOCK_SIZE);
    Preconditions.checkArgument(IV.length == AES_BLOCK_SIZE);
    
    ByteBuffer buf = ByteBuffer.wrap(IV);
    buf.put(initIV);
    buf.order(ByteOrder.BIG_ENDIAN);
    counter += buf.getLong(AES_BLOCK_SIZE - 8);
    buf.putLong(AES_BLOCK_SIZE - 8, counter);
  }
}
