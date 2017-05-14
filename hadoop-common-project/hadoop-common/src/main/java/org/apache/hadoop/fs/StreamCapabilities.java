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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Interface to query streams for supported capabilities.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface StreamCapabilities {
  /**
   * Capabilities that a stream can support and be queried for.
   */
  enum StreamCapability {
    /**
     * Stream hflush capability to flush out the data in client's buffer.
     * Streams with this capability implement {@link Syncable} and support
     * {@link Syncable#hflush()}.
     */
    HFLUSH("hflush"),

    /**
     * Stream hsync capability to flush out the data in client's buffer and
     * the disk device. Streams with this capability implement {@link Syncable}
     * and support {@link Syncable#hsync()}.
     */
    HSYNC("hsync");

    private final String capability;

    StreamCapability(String value) {
      this.capability = value;
    }

    public final String getValue() {
      return capability;
    }
  }

  /**
   * Query the stream for a specific capability.
   *
   * @param capability string to query the stream support for.
   * @return True if the stream supports capability.
   */
  boolean hasCapability(String capability);
}

