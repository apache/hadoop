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
package org.apache.hadoop.io.erasurecode.rawcoder.util;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Some utilities for Reed-Solomon coding.
 */
@InterfaceAudience.Private
public class RSUtil {

  // We always use the byte system (with symbol size 8, field size 256,
  // primitive polynomial 285, and primitive root 2).
  public static GaloisField GF = GaloisField.getInstance();
  public static final int PRIMITIVE_ROOT = 2;

  public static int[] getPrimitivePower(int numDataUnits, int numParityUnits) {
    int[] primitivePower = new int[numDataUnits + numParityUnits];
    // compute powers of the primitive root
    for (int i = 0; i < numDataUnits + numParityUnits; i++) {
      primitivePower[i] = GF.power(PRIMITIVE_ROOT, i);
    }
    return primitivePower;
  }

}
