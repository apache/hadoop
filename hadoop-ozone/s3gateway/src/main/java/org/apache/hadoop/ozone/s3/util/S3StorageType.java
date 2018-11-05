/*
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

package org.apache.hadoop.ozone.s3.util;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;

/**
 * Maps S3 storage class values to Ozone replication values.
 */

public enum S3StorageType {

  REDUCED_REDUNDANCY(ReplicationType.STAND_ALONE, ReplicationFactor.ONE),
  STANDARD(ReplicationType.RATIS, ReplicationFactor.THREE);

  private final ReplicationType type;
  private final ReplicationFactor factor;

  S3StorageType(
      ReplicationType type,
      ReplicationFactor factor) {
    this.type = type;
    this.factor = factor;
  }

  public ReplicationFactor getFactor() {
    return factor;
  }

  public ReplicationType getType() {
    return type;
  }

  public static S3StorageType getDefault() {
    return STANDARD;
  }

}
