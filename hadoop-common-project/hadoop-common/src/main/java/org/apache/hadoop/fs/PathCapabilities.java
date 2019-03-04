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

package org.apache.hadoop.fs;

import java.io.IOException;

/**
 * The Path counterpoint to {@link StreamCapabilities}; a query to see if,
 * a FileSystem/FileContext instance has a specific capability under the given
 * path.
 */
public interface PathCapabilities {

  /**
   * Probe for a filesystem instance offering a specific capability under the
   * given path.
   * If the function returns {@code true}, the filesystem is explicitly
   * declaring that the capability is available.
   * If the function returns {@code false}, it can mean one of:
   * <ul>
   *   <li>The capability is not known.</li>
   *   <li>The capability is known but it is not supported.</li>
   *   <li>The capability is known but the filesystem does not know if it
   *   is supported under the supplied path it.</li>
   * </ul>
   * The core guarantee which a caller can rely on is: if the predicate
   * returns true, then the specific operation/behavior can be expected to be
   * supported.
   * @param path path to query the capability of.
   * @param capability non-null, non-empty string to query the path for support.
   * @return true if the capability is supported under that part of the FS.
   * @throws IOException this should not be raised, except on problems
   * resolving paths or relaying the call.
   * @throws IllegalArgumentException invalid arguments
   */
  boolean hasPathCapability(Path path, String capability)
      throws IOException;
}
