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
package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.conf.Configured;

/**
 * A common class of basic facilities to be shared by encoder and decoder
 *
 * It implements the {@link ErasureCoder} interface.
 */
public abstract class AbstractErasureCoder
    extends Configured implements ErasureCoder {

  private int numDataUnits;
  private int numParityUnits;
  private int chunkSize;

  @Override
  public void initialize(int numDataUnits, int numParityUnits,
                         int chunkSize) {
    this.numDataUnits = numDataUnits;
    this.numParityUnits = numParityUnits;
    this.chunkSize = chunkSize;
  }

  @Override
  public int getNumDataUnits() {
    return numDataUnits;
  }

  @Override
  public int getNumParityUnits() {
    return numParityUnits;
  }

  @Override
  public int getChunkSize() {
    return chunkSize;
  }

  @Override
  public boolean preferNativeBuffer() {
    return false;
  }

  @Override
  public void release() {
    // Nothing to do by default
  }
}
