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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Encapsulates various options related to how fine-grained data checksums are
 * combined into block-level checksums.
 */
@InterfaceAudience.Private
public class BlockChecksumOptions {
  private final BlockChecksumType blockChecksumType;
  private final long stripeLength;

  public BlockChecksumOptions(
      BlockChecksumType blockChecksumType, long stripeLength) {
    this.blockChecksumType = blockChecksumType;
    this.stripeLength = stripeLength;
  }

  public BlockChecksumOptions(BlockChecksumType blockChecksumType) {
    this(blockChecksumType, 0);
  }

  public BlockChecksumType getBlockChecksumType() {
    return blockChecksumType;
  }

  public long getStripeLength() {
    return stripeLength;
  }

  @Override
  public String toString() {
    return String.format("blockChecksumType=%s, stripedLength=%d",
        blockChecksumType, stripeLength);
  }
}
