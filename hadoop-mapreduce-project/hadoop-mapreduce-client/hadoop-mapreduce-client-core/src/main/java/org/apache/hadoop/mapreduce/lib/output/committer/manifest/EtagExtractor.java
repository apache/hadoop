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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;

/**
 * Interface for anything which can take a filesystem-client-specific subclass
 * of a FileStatus and return an etag.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface EtagExtractor {

  /**
   * Extract an etag from a status if the conditions are met.
   * If the conditions are not met, return null or ""; they will
   * both be treated as "no etags available"
   * <pre>
   *   1. The status is of a type which the implementation recognizes
   *   as containing an etag.
   *   2. After casting the etag field can be retrieved
   *   3. and that value is non-null/non-empty.
   * </pre>
   * @param status status, which may be of any subclass of FileStatus.
   * @return either a valid etag, or null or "".
   */
  String getEtag(FileStatus status);
}
