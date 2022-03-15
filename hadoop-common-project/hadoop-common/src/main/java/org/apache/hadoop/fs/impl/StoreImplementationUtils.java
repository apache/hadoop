/*
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
 * limitations under the License
 */

package org.apache.hadoop.fs.impl;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StreamCapabilities;

import static org.apache.hadoop.fs.StreamCapabilities.HFLUSH;
import static org.apache.hadoop.fs.StreamCapabilities.HSYNC;

/**
 * Utility classes to help implementing filesystems and streams.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class StoreImplementationUtils {

  private StoreImplementationUtils() {
  }

  /**
   * Check the probe capability being for {@link StreamCapabilities#HSYNC}
   * or {@link StreamCapabilities#HFLUSH}
   * {@code Syncable.hsync()} and {@code Syncable.hflush()} functionality.
   * @param capability capability string.
   * @return true if either refers to one of the Syncable operations.
   */
  public static boolean isProbeForSyncable(String capability) {
    return capability.equalsIgnoreCase(HSYNC) ||
        capability.equalsIgnoreCase(HFLUSH);
  }

  /**
   * Probe for an object having a capability; returns true
   * if the stream implements {@link StreamCapabilities} and its
   * {@code hasCapabilities()} method returns true for the capability.
   * This is a package private method intended to provided a common
   * implementation for input and output streams.
   * {@link StreamCapabilities#hasCapability(String)} call is for public use.
   * @param object object to probe.
   * @param capability capability to probe for
   * @return true if the object implements stream capabilities and
   * declares that it supports the capability.
   */
  static boolean objectHasCapability(Object object, String capability) {
    if (object instanceof StreamCapabilities) {
      return ((StreamCapabilities) object).hasCapability(capability);
    }
    return false;
  }

  /**
   * Probe for an output stream having a capability; returns true
   * if the stream implements {@link StreamCapabilities} and its
   * {@code hasCapabilities()} method returns true for the capability.
   * @param out output stream
   * @param capability capability to probe for
   * @return true if the stream declares that it supports the capability.
   */
  public static boolean hasCapability(OutputStream out, String capability) {
    return objectHasCapability(out, capability);
  }

  /**
   * Probe for an input stream having a capability; returns true
   * if the stream implements {@link StreamCapabilities} and its
   * {@code hasCapabilities()} method returns true for the capability.
   * @param in input stream
   * @param capability capability to probe for
   * @return true if the stream declares that it supports the capability.
   */
  public static boolean hasCapability(InputStream in, String capability) {
    return objectHasCapability(in, capability);
  }

}
