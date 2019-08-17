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

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .CopyContainerRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .CopyContainerResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto
    .IntraDatanodeProtocolServiceGrpc;
import org.apache.hadoop.hdds.protocol.datanode.proto
    .IntraDatanodeProtocolServiceGrpc.IntraDatanodeProtocolServiceStub;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client to read container data from Grpc.
 */
public class GrpcReplicationClient {

  private static final Logger LOG =
      LoggerFactory.getLogger(GrpcReplicationClient.class);

  private final ManagedChannel channel;

  private final IntraDatanodeProtocolServiceStub client;

  private final Path workingDirectory;

  public GrpcReplicationClient(String host,
      int port, Path workingDir) {

    channel = NettyChannelBuilder.forAddress(host, port)
        .usePlaintext()
        .maxInboundMessageSize(OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE)
        .build();
    client = IntraDatanodeProtocolServiceGrpc.newStub(channel);
    this.workingDirectory = workingDir;

  }

  public CompletableFuture<Path> download(long containerId) {
    CopyContainerRequestProto request =
        CopyContainerRequestProto.newBuilder()
            .setContainerID(containerId)
            .setLen(-1)
            .setReadOffset(0)
            .build();

    CompletableFuture<Path> response = new CompletableFuture<>();

    Path destinationPath =
        getWorkingDirectory().resolve("container-" + containerId + ".tar.gz");

    client.download(request,
        new StreamDownloader(containerId, response, destinationPath));
    return response;
  }

  private Path getWorkingDirectory() {
    return workingDirectory;
  }

  public void shutdown() {
    channel.shutdown();
  }

  /**
   * Grpc stream observer to ComletableFuture adapter.
   */
  public static class StreamDownloader
      implements StreamObserver<CopyContainerResponseProto> {

    private final CompletableFuture<Path> response;

    private final long containerId;

    private BufferedOutputStream stream;

    private Path outputPath;

    public StreamDownloader(long containerId, CompletableFuture<Path> response,
        Path outputPath) {
      this.response = response;
      this.containerId = containerId;
      this.outputPath = outputPath;
      try {
        Preconditions.checkNotNull(outputPath, "Output path cannot be null");
        Path parentPath = Preconditions.checkNotNull(outputPath.getParent());
        Files.createDirectories(parentPath);
        stream =
            new BufferedOutputStream(new FileOutputStream(outputPath.toFile()));
      } catch (IOException e) {
        throw new RuntimeException("OutputPath can't be used: " + outputPath,
            e);
      }

    }

    @Override
    public void onNext(CopyContainerResponseProto chunk) {
      try {
        stream.write(chunk.getData().toByteArray());
      } catch (IOException e) {
        response.completeExceptionally(e);
      }
    }

    @Override
    public void onError(Throwable throwable) {
      try {
        stream.close();
        LOG.error("Container download was unsuccessfull", throwable);
        try {
          Files.delete(outputPath);
        } catch (IOException ex) {
          LOG.error(
              "Error happened during the download but can't delete the "
                  + "temporary destination.", ex);
        }
        response.completeExceptionally(throwable);
      } catch (IOException e) {
        response.completeExceptionally(e);
      }
    }

    @Override
    public void onCompleted() {
      try {
        stream.close();
        LOG.info("Container is downloaded to {}", outputPath);
        response.complete(outputPath);
      } catch (IOException e) {
        response.completeExceptionally(e);
      }

    }
  }

}
