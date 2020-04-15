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

public interface DirectoryPolicy {

  /**
   * Should a directory marker be retained?
   * @param path path a file/directory is being created with.
   * @return true if the marker MAY be kept, false if it MUST be deleted.
   */
  boolean keepDirectoryMarkers(Path path);

  /**
   * Supported retention policies.
   */
  enum MarkerPolicy {
    /**
     * Keep markers.
     * This is <i>Not backwards compatible</i>.
     */
    Keep,

    /**
     * Delete markers.
     * This is what has been done since S3A was released. */
    Delete,

    /**
     * Keep markers in authoritative paths only.
     * This is <i>Not backwards compatible</i> within the
     * auth paths, but is outside these.
     */
    Authoritative
  }
}
