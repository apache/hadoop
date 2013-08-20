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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Supports zero-copy reads.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface SupportsZeroCopy {
  /**
   * Get a zero-copy cursor to use for zero-copy reads.
   *
   * @throws IOException
   *     If there was an error creating the ZeroCopyCursor
   * @throws UnsupportedOperationException
   *     If this stream does not support zero-copy reads.
   *     This is used, for example, when one stream wraps another
   *     which may or may not support ZCR.
   */
  public ZeroCopyCursor createZeroCopyCursor()
      throws IOException, ZeroCopyUnavailableException;
}