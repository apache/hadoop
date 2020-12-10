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

import java.util.Locale;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathCapabilities;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkArgument;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class PathCapabilitiesSupport {

  /**
   * Validate the arguments to
   * {@link PathCapabilities#hasPathCapability(Path, String)}.
   * @param path path to query the capability of.
   * @param capability non-null, non-empty string to query the path for support.
   * @return the string to use in a switch statement.
   * @throws IllegalArgumentException if a an argument is invalid.
   */
  public static String validatePathCapabilityArgs(
      final Path path, final String capability) {
    checkArgument(path != null, "null path");
    checkArgument(capability != null, "capability parameter is null");
    checkArgument(!capability.isEmpty(),
        "capability parameter is empty string");
    return capability.toLowerCase(Locale.ENGLISH);
  }
}
