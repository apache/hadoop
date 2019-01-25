/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo.Builder;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test OmKeyInfo.
 */
public class TestOmKeyInfo {

  @Test
  public void protobufConversion() {
    OmKeyInfo key = new Builder()
        .setKeyName("key1")
        .setBucketName("bucket")
        .setVolumeName("vol1")
        .setCreationTime(123L)
        .setModificationTime(123L)
        .setDataSize(123L)
        .setReplicationFactor(ReplicationFactor.THREE)
        .setReplicationType(ReplicationType.RATIS)
        .addMetadata("key1", "value1")
        .addMetadata("key2", "value2")
        .build();

    OmKeyInfo keyAfterSerialization =
        OmKeyInfo.getFromProtobuf(key.getProtobuf());

    Assert.assertEquals(key, keyAfterSerialization);
  }
}