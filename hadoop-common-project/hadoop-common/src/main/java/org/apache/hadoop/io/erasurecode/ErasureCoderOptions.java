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
package org.apache.hadoop.io.erasurecode;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Erasure coder configuration that maintains schema info and coder options.
 */
@InterfaceAudience.Private
public final class ErasureCoderOptions {

  private final int numDataUnits;
  private final int numParityUnits;
  private final int numAllUnits;
  private final boolean allowChangeInputs;
  private final boolean allowVerboseDump;

  public ErasureCoderOptions(int numDataUnits, int numParityUnits) {
    this(numDataUnits, numParityUnits, false, false);
  }

  public ErasureCoderOptions(int numDataUnits, int numParityUnits,
                        boolean allowChangeInputs, boolean allowVerboseDump) {
    this.numDataUnits = numDataUnits;
    this.numParityUnits = numParityUnits;
    this.numAllUnits = numDataUnits + numParityUnits;
    this.allowChangeInputs = allowChangeInputs;
    this.allowVerboseDump = allowVerboseDump;
  }

  /**
   * The number of data input units for the coding. A unit can be a byte,
   * chunk or buffer or even a block.
   * @return count of data input units
   */
  public int getNumDataUnits() {
    return numDataUnits;
  }

  /**
   * The number of parity output units for the coding. A unit can be a byte,
   * chunk, buffer or even a block.
   * @return count of parity output units
   */
  public int getNumParityUnits() {
    return numParityUnits;
  }

  /**
   * The number of all the involved units in the coding.
   * @return count of all the data units and parity units
   */
  public int getNumAllUnits() {
    return numAllUnits;
  }

  /**
   * Allow changing input buffer content (not positions). Maybe better
   * performance if not allowed.
   * @return true if allowing input content to be changed, false otherwise
   */
  public boolean allowChangeInputs() {
    return allowChangeInputs;
  }

  /**
   * Allow dump verbose debug info or not.
   * @return true if verbose debug info is desired, false otherwise
   */
  public boolean allowVerboseDump() {
    return allowVerboseDump;
  }
}
