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

package org.apache.hadoop.hdfs.server.datanode.checker;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


/**
 * A Checkable is an object whose health can be probed by invoking its
 * {@link #check} method.
 *
 * e.g. a {@link Checkable} instance may represent a single hardware
 * resource.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface Checkable<K, V> {

  /**
   * Query the health of this object. This method may hang
   * indefinitely depending on the status of the target resource.
   *
   * @param context for the probe operation. May be null depending
   *                on the implementation.
   *
   * @return result of the check operation.
   *
   * @throws Exception encountered during the check operation. An
   *                   exception indicates that the check failed.
   */
  V check(K context) throws Exception;
}
