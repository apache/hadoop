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

package org.apache.hadoop.yarn.sharedcache;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

@Public
@Evolving
/**
 * An interface to calculate a checksum for a resource in the shared cache. The
 * checksum implementation should be thread safe.
 */
public interface SharedCacheChecksum {

  /**
   * Calculate the checksum of the passed input stream.
   *
   * @param in <code>InputStream</code> to be checksumed
   * @return the message digest of the input stream
   * @throws IOException
   */
  public String computeChecksum(InputStream in) throws IOException;
}
