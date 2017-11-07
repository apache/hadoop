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

package org.apache.hadoop.fs;

import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Static methods to implement policies for {@link StreamCapabilities}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class StreamCapabilitiesPolicy {
  /**
   * Implement the policy for {@link CanUnbuffer#unbuffer()}.
   *
   * @param in the input stream
   */
  public static void unbuffer(InputStream in) {
    try {
      if (in instanceof StreamCapabilities
          && ((StreamCapabilities) in).hasCapability(
          StreamCapabilities.UNBUFFER)) {
        ((CanUnbuffer) in).unbuffer();
      }
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("this stream " +
          in.getClass().getName() +
          " claims to unbuffer but forgets to implement CanUnbuffer");
    }
  }
}

