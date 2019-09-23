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

package org.apache.hadoop.ozone.container.replication;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .CopyContainerRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .CopyContainerResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto
    .IntraDatanodeProtocolServiceGrpc;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to make containers available for replication.
 */
public class GrpcReplicationService extends
    IntraDatanodeProtocolServiceGrpc.IntraDatanodeProtocolServiceImplBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcReplicationService.class);

  private final ContainerReplicationSource containerReplicationSource;

  public GrpcReplicationService(
      ContainerReplicationSource containerReplicationSource) {
    this.containerReplicationSource = containerReplicationSource;
  }

  @Override
  public void download(CopyContainerRequestProto request,
      StreamObserver<CopyContainerResponseProto> responseObserver) {
    LOG.info("Streaming container data ({}) to other datanode",
        request.getContainerID());
    try {
      GrpcOutputStream outputStream =
          new GrpcOutputStream(responseObserver, request.getContainerID());
      containerReplicationSource
          .copyData(request.getContainerID(), outputStream);
    } catch (IOException e) {
      LOG.error("Can't stream the container data", e);
      responseObserver.onError(e);
    }
  }

  private static class GrpcOutputStream extends OutputStream
      implements Closeable {

    private static final int BUFFER_SIZE_IN_BYTES = 1024 * 1024;

    private final StreamObserver<CopyContainerResponseProto> responseObserver;

    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    private long containerId;

    private int readOffset = 0;

    private int writtenBytes;

    GrpcOutputStream(
        StreamObserver<CopyContainerResponseProto> responseObserver,
        long containerId) {
      this.responseObserver = responseObserver;
      this.containerId = containerId;
    }

    @Override
    public void write(int b) throws IOException {
      try {
        buffer.write(b);
        if (buffer.size() > BUFFER_SIZE_IN_BYTES) {
          flushBuffer(false);
        }
      } catch (Exception ex) {
        responseObserver.onError(ex);
      }
    }

    private void flushBuffer(boolean eof) {
      if (buffer.size() > 0) {
        CopyContainerResponseProto response =
            CopyContainerResponseProto.newBuilder()
                .setContainerID(containerId)
                .setData(ByteString.copyFrom(buffer.toByteArray()))
                .setEof(eof)
                .setReadOffset(readOffset)
                .setLen(buffer.size())
                .build();
        responseObserver.onNext(response);
        readOffset += buffer.size();
        writtenBytes += buffer.size();
        buffer.reset();
      }
    }

    @Override
    public void close() throws IOException {
      flushBuffer(true);
      LOG.info("{} bytes written to the rpc stream from container {}",
          writtenBytes, containerId);
      responseObserver.onCompleted();
    }
  }
}
