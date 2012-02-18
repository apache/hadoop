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

package org.apache.hadoop.raid;

public interface ErasureCode {
  /**
   * Encodes the given message.
   * @param message The data of the message. The data is present in the least
   *                significant bits of each int. The number of data bits is
   *                symbolSize(). The number of elements of message is
   *                stripeSize().
   * @param parity  (out) The information is present in the least
   *                significant bits of each int. The number of parity bits is
   *                symbolSize(). The number of elements in the code is
   *                paritySize().
   */
  public void encode(int[] message, int[] parity);

  /**
   * Generates missing portions of data.
   * @param data The message and parity. The parity should be placed in the
   *             first part of the array. In each integer, the relevant portion
   *             is present in the least significant bits of each int.
   *             The number of elements in data is stripeSize() + paritySize().
   * @param erasedLocations The indexes in data which are not available.
   * @param erasedValues    (out)The decoded values corresponding to erasedLocations.
   */
  public void decode(int[] data, int[] erasedLocations, int[] erasedValues);

  /**
   * The number of elements in the message.
   */
  public int stripeSize();

  /**
   * The number of elements in the code.
   */
  public int paritySize();

  /**
   * Number of bits for each symbol.
   */
  public int symbolSize();
}
