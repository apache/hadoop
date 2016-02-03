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

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;

import java.util.Arrays;

/**
 * Helpful utilities for implementing some raw erasure coders.
 */
@InterfaceAudience.Private
public final class CoderUtil {

  private CoderUtil() {
    // No called
  }


  /**
   * Get indexes into inputs array for items marked as null, either erased or
   * not to read.
   * @return indexes into inputs array
   */
  public static <T> int[] getErasedOrNotToReadIndexes(T[] inputs) {
    int[] invalidIndexes = new int[inputs.length];
    int idx = 0;
    for (int i = 0; i < inputs.length; i++) {
      if (inputs[i] == null) {
        invalidIndexes[idx++] = i;
      }
    }

    return Arrays.copyOf(invalidIndexes, idx);
  }

  /**
   * Find the valid input from all the inputs.
   * @param inputs input buffers to look for valid input
   * @return the first valid input
   */
  public static <T> T findFirstValidInput(T[] inputs) {
    for (T input : inputs) {
      if (input != null) {
        return input;
      }
    }

    throw new HadoopIllegalArgumentException(
        "Invalid inputs are found, all being null");
  }

  /**
   * Picking up indexes of valid inputs.
   * @param inputs actually decoding input buffers
   * @param validIndexes an array to be filled and returned
   * @param <T>
   */
  public static <T> void makeValidIndexes(T[] inputs, int[] validIndexes) {
    int idx = 0;
    for (int i = 0; i < inputs.length && idx < validIndexes.length; i++) {
      if (inputs[i] != null) {
        validIndexes[idx++] = i;
      }
    }
  }
}
