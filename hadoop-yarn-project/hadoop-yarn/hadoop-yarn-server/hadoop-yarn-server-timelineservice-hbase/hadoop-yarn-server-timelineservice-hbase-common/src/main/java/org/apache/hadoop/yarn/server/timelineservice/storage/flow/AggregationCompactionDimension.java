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
package org.apache.hadoop.yarn.server.timelineservice.storage.flow;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Identifies the compaction dimensions for the data in the {@link FlowRunTable}
 * .
 */
public enum AggregationCompactionDimension {

  /**
   * the application id.
   */
  APPLICATION_ID((byte) 101);

  private byte tagType;
  private byte[] inBytes;

  private AggregationCompactionDimension(byte tagType) {
    this.tagType = tagType;
    this.inBytes = Bytes.toBytes(this.name());
  }

  public Attribute getAttribute(String attributeValue) {
    return new Attribute(this.name(), Bytes.toBytes(attributeValue));
  }

  public byte getTagType() {
    return tagType;
  }

  public byte[] getInBytes() {
    return this.inBytes.clone();
  }

  public static AggregationCompactionDimension
      getAggregationCompactionDimension(String aggCompactDimStr) {
    for (AggregationCompactionDimension aggDim : AggregationCompactionDimension
        .values()) {
      if (aggDim.name().equals(aggCompactDimStr)) {
        return aggDim;
      }
    }
    return null;
  }
}
