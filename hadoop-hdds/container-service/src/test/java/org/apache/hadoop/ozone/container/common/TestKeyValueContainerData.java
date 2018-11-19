/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is used to test the KeyValueContainerData.
 */
public class TestKeyValueContainerData {

  private static final long MAXSIZE = (long) StorageUnit.GB.toBytes(5);
  @Test
  public void testKeyValueData() {
    long containerId = 1L;
    ContainerProtos.ContainerType containerType = ContainerProtos
        .ContainerType.KeyValueContainer;
    String path = "/tmp";
    String containerDBType = "RocksDB";
    ContainerProtos.ContainerDataProto.State state =
        ContainerProtos.ContainerDataProto.State.CLOSED;
    AtomicLong val = new AtomicLong(0);
    UUID pipelineId = UUID.randomUUID();
    UUID datanodeId = UUID.randomUUID();

    KeyValueContainerData kvData = new KeyValueContainerData(containerId,
        MAXSIZE, pipelineId.toString(), datanodeId.toString());

    assertEquals(containerType, kvData.getContainerType());
    assertEquals(containerId, kvData.getContainerID());
    assertEquals(ContainerProtos.ContainerDataProto.State.OPEN, kvData
        .getState());
    assertEquals(0, kvData.getMetadata().size());
    assertEquals(0, kvData.getNumPendingDeletionBlocks());
    assertEquals(val.get(), kvData.getReadBytes());
    assertEquals(val.get(), kvData.getWriteBytes());
    assertEquals(val.get(), kvData.getReadCount());
    assertEquals(val.get(), kvData.getWriteCount());
    assertEquals(val.get(), kvData.getKeyCount());
    assertEquals(val.get(), kvData.getNumPendingDeletionBlocks());
    assertEquals(MAXSIZE, kvData.getMaxSize());

    kvData.setState(state);
    kvData.setContainerDBType(containerDBType);
    kvData.setChunksPath(path);
    kvData.setMetadataPath(path);
    kvData.incrReadBytes(10);
    kvData.incrWriteBytes(10);
    kvData.incrReadCount();
    kvData.incrWriteCount();
    kvData.incrKeyCount();
    kvData.incrPendingDeletionBlocks(1);

    assertEquals(state, kvData.getState());
    assertEquals(containerDBType, kvData.getContainerDBType());
    assertEquals(path, kvData.getChunksPath());
    assertEquals(path, kvData.getMetadataPath());

    assertEquals(10, kvData.getReadBytes());
    assertEquals(10, kvData.getWriteBytes());
    assertEquals(1, kvData.getReadCount());
    assertEquals(1, kvData.getWriteCount());
    assertEquals(1, kvData.getKeyCount());
    assertEquals(1, kvData.getNumPendingDeletionBlocks());
    assertEquals(pipelineId.toString(), kvData.getOriginPipelineId());
    assertEquals(datanodeId.toString(), kvData.getOriginNodeId());
  }

}
