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

package org.apache.hadoop.fs.s3a.impl;

import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_DELETE;
import static org.apache.hadoop.fs.s3a.Constants.DIRECTORY_MARKER_POLICY_KEEP;

/**
 * Interface for Directory Marker policies to implement.
 */

public interface DirectoryPolicy {



  /**
   * Should a directory marker be retained?
   * @param path path a file/directory is being created with.
   * @return true if the marker MAY be kept, false if it MUST be deleted.
   */
  boolean keepDirectoryMarkers(Path path);

  /**
   * Get the marker policy.
   * @return policy.
   */
  MarkerPolicy getMarkerPolicy();

  /**
   * Describe the policy for marker tools and logs.
   * @return description of the current policy.
   */
  String describe();

  /**
   * Does a specific path have the relevant option.
   * This is to be forwarded from the S3AFileSystem.hasPathCapability
   * But only for those capabilities related to markers*
   * @param path path
   * @param capability capability
   * @return true if the capability is supported, false if not
   * @throws IllegalArgumentException if the capability is unknown.
   */
  boolean hasPathCapability(Path path, String capability);

  /**
   * Supported retention policies.
   */
  enum MarkerPolicy {

    /**
     * Delete markers.
     * <p></p>
     * This is the classic S3A policy,
     */
    Delete(DIRECTORY_MARKER_POLICY_DELETE),

    /**
     * Keep markers.
     * <p></p>
     * This is <i>Not backwards compatible</i>.
     */
    Keep(DIRECTORY_MARKER_POLICY_KEEP),

    /**
     * Keep markers in authoritative paths only.
     * <p></p>
     * This is <i>Not backwards compatible</i> within the
     * auth paths, but is outside these.
     */
    Authoritative(DIRECTORY_MARKER_POLICY_AUTHORITATIVE);

    /**
     * The name of the option as allowed in configuration files
     * and marker-aware tooling.
     */
    private final String optionName;

    MarkerPolicy(final String optionName) {
      this.optionName = optionName;
    }

    /**
     * Get the option name.
     * @return name of the option
     */
    public String getOptionName() {
      return optionName;
    }
  }
}
