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
package org.apache.hadoop.hdfs;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

/**
 * An immutable key which identifies a block.
 */
@InterfaceAudience.Private
final public class ExtendedBlockId {
  /**
   * The block ID for this block.
   */
  private final long blockId;

  /**
   * The block pool ID for this block.
   */
  private final String bpId;

  public static ExtendedBlockId fromExtendedBlock(ExtendedBlock block) {
    return new ExtendedBlockId(block.getBlockId(), block.getBlockPoolId());
  }

  public ExtendedBlockId(long blockId, String bpId) {
    this.blockId = blockId;
    this.bpId = bpId;
  }

  public long getBlockId() {
    return this.blockId;
  }

  public String getBlockPoolId() {
    return this.bpId;
  }

  @Override
  public boolean equals(Object o) {
    if ((o == null) || (o.getClass() != this.getClass())) {
      return false;
    }
    ExtendedBlockId other = (ExtendedBlockId)o;
    return new EqualsBuilder().
        append(blockId, other.blockId).
        append(bpId, other.bpId).
        isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().
        append(this.blockId).
        append(this.bpId).
        toHashCode();
  }

  @Override
  public String toString() {
    return String.valueOf(blockId) + "_" + bpId;
  }
}
