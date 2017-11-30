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

import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Opaque, serializable reference to an entity in the FileSystem. May contain
 * metadata sufficient to resolve or verify subsequent accesses indepedent of
 * other modifications to the FileSystem.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface PathHandle extends Serializable {

  /**
   * @return Serialized from in bytes.
   */
  default byte[] toByteArray() {
    ByteBuffer bb = bytes();
    byte[] ret = new byte[bb.remaining()];
    bb.get(ret);
    return ret;
  }

  ByteBuffer bytes();

  @Override
  boolean equals(Object other);

}
