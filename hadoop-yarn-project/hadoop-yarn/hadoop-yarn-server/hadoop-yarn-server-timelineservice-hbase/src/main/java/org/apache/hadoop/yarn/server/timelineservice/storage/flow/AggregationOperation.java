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
 * Identifies the attributes to be set for puts into the {@link FlowRunTable}.
 * The numbers used for tagType are prime numbers.
 */
public enum AggregationOperation {

  /**
   * When the flow was started.
   */
  GLOBAL_MIN((byte) 71),

  /**
   * When it ended.
   */
  GLOBAL_MAX((byte) 73),

  /**
   * The metrics of the flow.
   */
  SUM((byte) 79),

  /**
   * application running.
   */
  SUM_FINAL((byte) 83),

  /**
   * Min value as per the latest timestamp
   * seen for a given app.
   */
  LATEST_MIN((byte) 89),

  /**
   * Max value as per the latest timestamp
   * seen for a given app.
   */
  LATEST_MAX((byte) 97);

  private byte tagType;
  private byte[] inBytes;

  private AggregationOperation(byte tagType) {
    this.tagType = tagType;
    this.inBytes = Bytes.toBytes(this.name());
  }

  public Attribute getAttribute() {
    return new Attribute(this.name(), this.inBytes);
  }

  public byte getTagType() {
    return tagType;
  }

  public byte[] getInBytes() {
    return this.inBytes.clone();
  }

  /**
   * returns the AggregationOperation enum that represents that string.
   * @param aggOpStr Aggregation operation.
   * @return the AggregationOperation enum that represents that string
   */
  public static AggregationOperation getAggregationOperation(String aggOpStr) {
    for (AggregationOperation aggOp : AggregationOperation.values()) {
      if (aggOp.name().equals(aggOpStr)) {
        return aggOp;
      }
    }
    return null;
  }

}
