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
 * The replication type to be used while writing key into ozone.
 */
public enum ReplicationType {
    RATIS,
    STAND_ALONE,
  CHAINED;

  public static ReplicationType fromProto(
      HddsProtos.ReplicationType replicationType) {
    if (replicationType == null) {
      return null;
    }
    switch (replicationType) {
    case RATIS:
      return ReplicationType.RATIS;
    case STAND_ALONE:
      return ReplicationType.STAND_ALONE;
    case CHAINED:
      return ReplicationType.CHAINED;
    default:
      throw new IllegalArgumentException(
          "Unsupported ProtoBuf replication type: " + replicationType);
    }
  }
}
