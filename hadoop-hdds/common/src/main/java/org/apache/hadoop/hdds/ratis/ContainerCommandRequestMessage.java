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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutSmallFileRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.JavaUtils;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Implementing the {@link Message} interface
 * for {@link ContainerCommandRequestProto}.
 */
public final class ContainerCommandRequestMessage implements Message {
  public static ContainerCommandRequestMessage toMessage(
      ContainerCommandRequestProto request, String traceId) {
    final ContainerCommandRequestProto.Builder b
        = ContainerCommandRequestProto.newBuilder(request);
    if (traceId != null) {
      b.setTraceID(traceId);
    }

    ByteString data = ByteString.EMPTY;
    if (request.getCmdType() == Type.WriteChunk) {
      final WriteChunkRequestProto w = request.getWriteChunk();
      data = w.getData();
      b.setWriteChunk(w.toBuilder().clearData());
    } else if (request.getCmdType() == Type.PutSmallFile) {
      final PutSmallFileRequestProto p = request.getPutSmallFile();
      data = p.getData();
      b.setPutSmallFile(p.toBuilder().setData(ByteString.EMPTY));
    }
    return new ContainerCommandRequestMessage(b.build(), data);
  }

  public static ContainerCommandRequestProto toProto(
      ByteString bytes, RaftGroupId groupId)
      throws InvalidProtocolBufferException {
    final int i = 4 + bytes.asReadOnlyByteBuffer().getInt();
    final ContainerCommandRequestProto header
        = ContainerCommandRequestProto.parseFrom(bytes.substring(4, i));
    // TODO: setting pipeline id can be avoided if the client is sending it.
    //       In such case, just have to validate the pipeline id.
    final ContainerCommandRequestProto.Builder b = header.toBuilder();
    if (groupId != null) {
      b.setPipelineID(groupId.getUuid().toString());
    }
    final ByteString data = bytes.substring(i);
    if (header.getCmdType() == Type.WriteChunk) {
      b.setWriteChunk(b.getWriteChunkBuilder().setData(data));
    } else if (header.getCmdType() == Type.PutSmallFile) {
      b.setPutSmallFile(b.getPutSmallFileBuilder().setData(data));
    }
    return b.build();
  }

  private final ContainerCommandRequestProto header;
  private final ByteString data;
  private final Supplier<ByteString> contentSupplier
      = JavaUtils.memoize(this::buildContent);

  private ContainerCommandRequestMessage(
      ContainerCommandRequestProto header, ByteString data) {
    this.header = Objects.requireNonNull(header, "header == null");
    this.data = Objects.requireNonNull(data, "data == null");
  }

  private ByteString buildContent() {
    final ByteString headerBytes = header.toByteString();
    return RatisHelper.int2ByteString(headerBytes.size())
        .concat(headerBytes)
        .concat(data);
  }

  @Override
  public ByteString getContent() {
    return contentSupplier.get();
  }

  @Override
  public String toString() {
    return header + ", data.size=" + data.size();
  }
}
