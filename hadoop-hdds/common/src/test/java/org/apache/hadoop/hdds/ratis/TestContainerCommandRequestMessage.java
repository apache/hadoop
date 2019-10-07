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
package org.apache.hadoop.hdds.ratis;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.KeyValue;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutBlockRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutSmallFileRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.UUID;
import java.util.function.BiFunction;

/** Testing {@link ContainerCommandRequestMessage}. */
public class TestContainerCommandRequestMessage {
  static final Random RANDOM = new Random();

  static ByteString newData(int length, Random random) {
    final ByteString.Output out = ByteString.newOutput();
    for(int i = 0; i < length; i++) {
      out.write(random.nextInt());
    }
    return out.toByteString();
  }

  static ChecksumData checksum(ByteString data) {
    try {
      return new Checksum().computeChecksum(data.toByteArray());
    } catch (OzoneChecksumException e) {
      throw new IllegalStateException(e);
    }
  }

  static ContainerCommandRequestProto newPutSmallFile(
      BlockID blockID, ByteString data) {
    final BlockData.Builder blockData
        = BlockData.newBuilder()
        .setBlockID(blockID.getDatanodeBlockIDProtobuf());
    final PutBlockRequestProto.Builder putBlockRequest
        = PutBlockRequestProto.newBuilder()
        .setBlockData(blockData);
    final KeyValue keyValue = KeyValue.newBuilder()
        .setKey("OverWriteRequested")
        .setValue("true")
        .build();
    final ChunkInfo chunk = ChunkInfo.newBuilder()
        .setChunkName(blockID.getLocalID() + "_chunk")
        .setOffset(0)
        .setLen(data.size())
        .addMetadata(keyValue)
        .setChecksumData(checksum(data).getProtoBufMessage())
        .build();
    final PutSmallFileRequestProto putSmallFileRequest
        = PutSmallFileRequestProto.newBuilder()
        .setChunkInfo(chunk)
        .setBlock(putBlockRequest)
        .setData(data)
        .build();
    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(Type.PutSmallFile)
        .setContainerID(blockID.getContainerID())
        .setDatanodeUuid(UUID.randomUUID().toString())
        .setPutSmallFile(putSmallFileRequest)
        .build();
  }

  static ContainerCommandRequestProto newWriteChunk(
      BlockID blockID, ByteString data) {
    final ChunkInfo chunk = ChunkInfo.newBuilder()
        .setChunkName(blockID.getLocalID() + "_chunk_" + 1)
        .setOffset(0)
        .setLen(data.size())
        .setChecksumData(checksum(data).getProtoBufMessage())
        .build();

    final WriteChunkRequestProto.Builder writeChunkRequest
        = WriteChunkRequestProto.newBuilder()
        .setBlockID(blockID.getDatanodeBlockIDProtobuf())
        .setChunkData(chunk)
        .setData(data);
    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(Type.WriteChunk)
        .setContainerID(blockID.getContainerID())
        .setDatanodeUuid(UUID.randomUUID().toString())
        .setWriteChunk(writeChunkRequest)
        .build();
  }

  @Test
  public void testPutSmallFile() throws Exception {
    runTest(TestContainerCommandRequestMessage::newPutSmallFile);
  }

  @Test
  public void testWriteChunk() throws Exception {
    runTest(TestContainerCommandRequestMessage::newWriteChunk);
  }

  static void runTest(
      BiFunction<BlockID, ByteString, ContainerCommandRequestProto> method)
      throws Exception {
    for(int i = 0; i < 2; i++) {
      runTest(i, method);
    }
    for(int i = 2; i < 1 << 10;) {
      runTest(i + 1 + RANDOM.nextInt(i - 1), method);
      i <<= 1;
      runTest(i, method);
    }
  }

  static void runTest(int length,
      BiFunction<BlockID, ByteString, ContainerCommandRequestProto> method)
      throws Exception {
    System.out.println("length=" + length);
    final BlockID blockID = new BlockID(RANDOM.nextLong(), RANDOM.nextLong());
    final ByteString data = newData(length, RANDOM);

    final ContainerCommandRequestProto original = method.apply(blockID, data);
    final ContainerCommandRequestMessage message
        = ContainerCommandRequestMessage.toMessage(original, null);
    final ContainerCommandRequestProto computed
        = ContainerCommandRequestMessage.toProto(message.getContent(), null);
    Assert.assertEquals(original, computed);
  }
}
