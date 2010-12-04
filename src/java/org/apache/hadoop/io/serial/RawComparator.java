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

package org.apache.hadoop.io.serial;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A compare function that compares two sets of bytes.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface RawComparator {

  /**
   * Compare the two serialized keys. This must be stable, so:
   * compare(b1,s1,l1,b2,s2,l2) = -compare(b2,s2,l2,b1,s1,l2) for all buffers.
   * @param b1 the left data buffer to compare
   * @param s1 the first index in b1 to compare
   * @param l1 the number of bytes in b1 to compare
   * @param b2 the right data buffer to compare
   * @param s2 the first index in b2 to compare
   * @param l2 the number of bytes in b2 to compare
   * @return negative if b1 is less than b2, 0 if they are equal, positive if
   *         b1 is greater than b2.
   */
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2);

}
