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

package org.apache.hadoop.hdds.client;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * The replication factor to be used while writing key into ozone.
 */
public enum ReplicationFactor {
  ONE(1),
  THREE(3);

  /**
   * Integer representation of replication.
   */
  private int value;

  /**
   * Initializes ReplicationFactor with value.
   * @param value replication value
   */
  ReplicationFactor(int value) {
    this.value = value;
  }

  /**
   * Returns enum value corresponding to the int value.
   * @param value replication value
   * @return ReplicationFactor
   */
  public static ReplicationFactor valueOf(int value) {
    if(value == 1) {
      return ONE;
    }
    if (value == 3) {
      return THREE;
    }
    throw new IllegalArgumentException("Unsupported value: " + value);
  }

  public static ReplicationFactor fromProto(
      HddsProtos.ReplicationFactor replicationFactor) {
    if (replicationFactor == null) {
      return null;
    }
    switch (replicationFactor) {
    case ONE:
      return ReplicationFactor.ONE;
    case THREE:
      return ReplicationFactor.THREE;
    default:
      throw new IllegalArgumentException(
          "Unsupported ProtoBuf replication factor: " + replicationFactor);
    }
  }

  /**
   * Returns integer representation of ReplicationFactor.
   * @return replication value
   */
  public int getValue() {
    return value;
  }
}
