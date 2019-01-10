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
 * limitations under the License.
 */

package org.apache.hadoop.fs.impl;


import com.google.common.base.Preconditions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class PathCapabilitiesSupport {

  /**
   * Validate the arguments to {@code PathCapabilities.hadCapability()}.
   * @param path path to query the capability of.
   * @param capability non-null, non-empty string to query the path for support.
   * @throws IllegalArgumentException if a an argument is invalid.
   */
  public static void validatehasPathCapabilityArgs(
      final Path path, final String capability) {
    Preconditions.checkArgument(capability != null && !capability.isEmpty(),
        "null/empty capability");
    Preconditions.checkArgument(path != null, "null path");
  }
}
