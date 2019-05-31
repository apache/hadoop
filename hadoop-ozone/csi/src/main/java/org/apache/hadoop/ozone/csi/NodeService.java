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
package org.apache.hadoop.ozone.csi;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.ozone.csi.CsiServer.CsiConfig;

import csi.v1.Csi.NodeGetCapabilitiesRequest;
import csi.v1.Csi.NodeGetCapabilitiesResponse;
import csi.v1.Csi.NodeGetInfoRequest;
import csi.v1.Csi.NodeGetInfoResponse;
import csi.v1.Csi.NodePublishVolumeRequest;
import csi.v1.Csi.NodePublishVolumeResponse;
import csi.v1.Csi.NodeUnpublishVolumeRequest;
import csi.v1.Csi.NodeUnpublishVolumeResponse;
import csi.v1.NodeGrpc.NodeImplBase;
import io.grpc.stub.StreamObserver;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the CSI node service.
 */
public class NodeService extends NodeImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(NodeService.class);

  private String s3Endpoint;

  public NodeService(CsiConfig configuration) {
    this.s3Endpoint = configuration.getS3gAddress();

  }

  @Override
  public void nodePublishVolume(NodePublishVolumeRequest request,
      StreamObserver<NodePublishVolumeResponse> responseObserver) {

    try {
      Files.createDirectories(Paths.get(request.getTargetPath()));
      String mountCommand =
          String.format("goofys --endpoint %s %s %s",
              s3Endpoint,
              request.getVolumeId(),
              request.getTargetPath());
      LOG.info("Executing {}", mountCommand);

      executeCommand(mountCommand);

      responseObserver.onNext(NodePublishVolumeResponse.newBuilder()
          .build());
      responseObserver.onCompleted();

    } catch (Exception e) {
      responseObserver.onError(e);
    }

  }

  private void executeCommand(String mountCommand)
      throws IOException, InterruptedException {
    Process exec = Runtime.getRuntime().exec(mountCommand);
    exec.waitFor(10, TimeUnit.SECONDS);

    LOG.info("Command is executed with  stdout: {}, stderr: {}",
        IOUtils.toString(exec.getInputStream(), "UTF-8"),
        IOUtils.toString(exec.getErrorStream(), "UTF-8"));
    if (exec.exitValue() != 0) {
      throw new RuntimeException(String
          .format("Return code of the command %s was %d", mountCommand,
              exec.exitValue()));
    }
  }

  @Override
  public void nodeUnpublishVolume(NodeUnpublishVolumeRequest request,
      StreamObserver<NodeUnpublishVolumeResponse> responseObserver) {
    String umountCommand =
        String.format("fusermount -u %s", request.getTargetPath());
    LOG.info("Executing {}", umountCommand);

    try {
      executeCommand(umountCommand);

      responseObserver.onNext(NodeUnpublishVolumeResponse.newBuilder()
          .build());
      responseObserver.onCompleted();

    } catch (Exception e) {
      responseObserver.onError(e);
    }

  }

  @Override
  public void nodeGetCapabilities(NodeGetCapabilitiesRequest request,
      StreamObserver<NodeGetCapabilitiesResponse> responseObserver) {
    NodeGetCapabilitiesResponse response =
        NodeGetCapabilitiesResponse.newBuilder()
            .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void nodeGetInfo(NodeGetInfoRequest request,
      StreamObserver<NodeGetInfoResponse> responseObserver) {
    NodeGetInfoResponse response = null;
    try {
      response = NodeGetInfoResponse.newBuilder()
          .setNodeId(InetAddress.getLocalHost().getHostName())
          .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (UnknownHostException e) {
      responseObserver.onError(e);
    }

  }
}
