/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.utils;

import org.apache.hadoop.hdds.HddsUtils;

/**
 * This class uses system current time milliseconds to generate unique id.
 */
public final class UniqueId {
    /*
     * When we represent time in milliseconds using 'long' data type,
     * the LSB bits are used. Currently we are only using 44 bits (LSB),
     * 20 bits (MSB) are not used.
     * We will exhaust this 44 bits only when we are in year 2525,
     * until then we can safely use this 20 bits (MSB) for offset to generate
     * unique id within millisecond.
     *
     * Year        : Mon Dec 31 18:49:04 IST 2525
     * TimeInMillis: 17545641544247
     * Binary Representation:
     *   MSB (20 bits): 0000 0000 0000 0000 0000
     *   LSB (44 bits): 1111 1111 0101 0010 1001 1011 1011 0100 1010 0011 0111
     *
     * We have 20 bits to run counter, we should exclude the first bit (MSB)
     * as we don't want to deal with negative values.
     * To be on safer side we will use 'short' data type which is of length
     * 16 bits and will give us 65,536 values for offset.
     *
     */

  private static volatile short offset = 0;

  /**
   * Private constructor so that no one can instantiate this class.
   */
  private UniqueId() {}

  /**
   * Calculate and returns next unique id based on System#currentTimeMillis.
   *
   * @return unique long value
   */
  public static synchronized long next() {
    long utcTime = HddsUtils.getUtcTime();
    if ((utcTime & 0xFFFF000000000000L) == 0) {
      return utcTime << Short.SIZE | (offset++ & 0x0000FFFF);
    }
    throw new RuntimeException("Got invalid UTC time," +
        " cannot generate unique Id. UTC Time: " + utcTime);
  }
}