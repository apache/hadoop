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
package csi.v0;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.69.0)",
    comments = "Source: csi.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class ControllerGrpc {

  private ControllerGrpc() {}

  public static final java.lang.String SERVICE_NAME = "csi.v0.Controller";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<csi.v0.Csi.CreateVolumeRequest,
      csi.v0.Csi.CreateVolumeResponse> getCreateVolumeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateVolume",
      requestType = csi.v0.Csi.CreateVolumeRequest.class,
      responseType = csi.v0.Csi.CreateVolumeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<csi.v0.Csi.CreateVolumeRequest,
      csi.v0.Csi.CreateVolumeResponse> getCreateVolumeMethod() {
    io.grpc.MethodDescriptor<csi.v0.Csi.CreateVolumeRequest, csi.v0.Csi.CreateVolumeResponse> getCreateVolumeMethod;
    if ((getCreateVolumeMethod = ControllerGrpc.getCreateVolumeMethod) == null) {
      synchronized (ControllerGrpc.class) {
        if ((getCreateVolumeMethod = ControllerGrpc.getCreateVolumeMethod) == null) {
          ControllerGrpc.getCreateVolumeMethod = getCreateVolumeMethod =
              io.grpc.MethodDescriptor.<csi.v0.Csi.CreateVolumeRequest, csi.v0.Csi.CreateVolumeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateVolume"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.CreateVolumeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.CreateVolumeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ControllerMethodDescriptorSupplier("CreateVolume"))
              .build();
        }
      }
    }
    return getCreateVolumeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<csi.v0.Csi.DeleteVolumeRequest,
      csi.v0.Csi.DeleteVolumeResponse> getDeleteVolumeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteVolume",
      requestType = csi.v0.Csi.DeleteVolumeRequest.class,
      responseType = csi.v0.Csi.DeleteVolumeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<csi.v0.Csi.DeleteVolumeRequest,
      csi.v0.Csi.DeleteVolumeResponse> getDeleteVolumeMethod() {
    io.grpc.MethodDescriptor<csi.v0.Csi.DeleteVolumeRequest, csi.v0.Csi.DeleteVolumeResponse> getDeleteVolumeMethod;
    if ((getDeleteVolumeMethod = ControllerGrpc.getDeleteVolumeMethod) == null) {
      synchronized (ControllerGrpc.class) {
        if ((getDeleteVolumeMethod = ControllerGrpc.getDeleteVolumeMethod) == null) {
          ControllerGrpc.getDeleteVolumeMethod = getDeleteVolumeMethod =
              io.grpc.MethodDescriptor.<csi.v0.Csi.DeleteVolumeRequest, csi.v0.Csi.DeleteVolumeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteVolume"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.DeleteVolumeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.DeleteVolumeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ControllerMethodDescriptorSupplier("DeleteVolume"))
              .build();
        }
      }
    }
    return getDeleteVolumeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<csi.v0.Csi.ControllerPublishVolumeRequest,
      csi.v0.Csi.ControllerPublishVolumeResponse> getControllerPublishVolumeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ControllerPublishVolume",
      requestType = csi.v0.Csi.ControllerPublishVolumeRequest.class,
      responseType = csi.v0.Csi.ControllerPublishVolumeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<csi.v0.Csi.ControllerPublishVolumeRequest,
      csi.v0.Csi.ControllerPublishVolumeResponse> getControllerPublishVolumeMethod() {
    io.grpc.MethodDescriptor<csi.v0.Csi.ControllerPublishVolumeRequest, csi.v0.Csi.ControllerPublishVolumeResponse> getControllerPublishVolumeMethod;
    if ((getControllerPublishVolumeMethod = ControllerGrpc.getControllerPublishVolumeMethod) == null) {
      synchronized (ControllerGrpc.class) {
        if ((getControllerPublishVolumeMethod = ControllerGrpc.getControllerPublishVolumeMethod) == null) {
          ControllerGrpc.getControllerPublishVolumeMethod = getControllerPublishVolumeMethod =
              io.grpc.MethodDescriptor.<csi.v0.Csi.ControllerPublishVolumeRequest, csi.v0.Csi.ControllerPublishVolumeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ControllerPublishVolume"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.ControllerPublishVolumeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.ControllerPublishVolumeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ControllerMethodDescriptorSupplier("ControllerPublishVolume"))
              .build();
        }
      }
    }
    return getControllerPublishVolumeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<csi.v0.Csi.ControllerUnpublishVolumeRequest,
      csi.v0.Csi.ControllerUnpublishVolumeResponse> getControllerUnpublishVolumeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ControllerUnpublishVolume",
      requestType = csi.v0.Csi.ControllerUnpublishVolumeRequest.class,
      responseType = csi.v0.Csi.ControllerUnpublishVolumeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<csi.v0.Csi.ControllerUnpublishVolumeRequest,
      csi.v0.Csi.ControllerUnpublishVolumeResponse> getControllerUnpublishVolumeMethod() {
    io.grpc.MethodDescriptor<csi.v0.Csi.ControllerUnpublishVolumeRequest, csi.v0.Csi.ControllerUnpublishVolumeResponse> getControllerUnpublishVolumeMethod;
    if ((getControllerUnpublishVolumeMethod = ControllerGrpc.getControllerUnpublishVolumeMethod) == null) {
      synchronized (ControllerGrpc.class) {
        if ((getControllerUnpublishVolumeMethod = ControllerGrpc.getControllerUnpublishVolumeMethod) == null) {
          ControllerGrpc.getControllerUnpublishVolumeMethod = getControllerUnpublishVolumeMethod =
              io.grpc.MethodDescriptor.<csi.v0.Csi.ControllerUnpublishVolumeRequest, csi.v0.Csi.ControllerUnpublishVolumeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ControllerUnpublishVolume"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.ControllerUnpublishVolumeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.ControllerUnpublishVolumeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ControllerMethodDescriptorSupplier("ControllerUnpublishVolume"))
              .build();
        }
      }
    }
    return getControllerUnpublishVolumeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<csi.v0.Csi.ValidateVolumeCapabilitiesRequest,
      csi.v0.Csi.ValidateVolumeCapabilitiesResponse> getValidateVolumeCapabilitiesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ValidateVolumeCapabilities",
      requestType = csi.v0.Csi.ValidateVolumeCapabilitiesRequest.class,
      responseType = csi.v0.Csi.ValidateVolumeCapabilitiesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<csi.v0.Csi.ValidateVolumeCapabilitiesRequest,
      csi.v0.Csi.ValidateVolumeCapabilitiesResponse> getValidateVolumeCapabilitiesMethod() {
    io.grpc.MethodDescriptor<csi.v0.Csi.ValidateVolumeCapabilitiesRequest, csi.v0.Csi.ValidateVolumeCapabilitiesResponse> getValidateVolumeCapabilitiesMethod;
    if ((getValidateVolumeCapabilitiesMethod = ControllerGrpc.getValidateVolumeCapabilitiesMethod) == null) {
      synchronized (ControllerGrpc.class) {
        if ((getValidateVolumeCapabilitiesMethod = ControllerGrpc.getValidateVolumeCapabilitiesMethod) == null) {
          ControllerGrpc.getValidateVolumeCapabilitiesMethod = getValidateVolumeCapabilitiesMethod =
              io.grpc.MethodDescriptor.<csi.v0.Csi.ValidateVolumeCapabilitiesRequest, csi.v0.Csi.ValidateVolumeCapabilitiesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ValidateVolumeCapabilities"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.ValidateVolumeCapabilitiesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.ValidateVolumeCapabilitiesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ControllerMethodDescriptorSupplier("ValidateVolumeCapabilities"))
              .build();
        }
      }
    }
    return getValidateVolumeCapabilitiesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<csi.v0.Csi.ListVolumesRequest,
      csi.v0.Csi.ListVolumesResponse> getListVolumesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListVolumes",
      requestType = csi.v0.Csi.ListVolumesRequest.class,
      responseType = csi.v0.Csi.ListVolumesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<csi.v0.Csi.ListVolumesRequest,
      csi.v0.Csi.ListVolumesResponse> getListVolumesMethod() {
    io.grpc.MethodDescriptor<csi.v0.Csi.ListVolumesRequest, csi.v0.Csi.ListVolumesResponse> getListVolumesMethod;
    if ((getListVolumesMethod = ControllerGrpc.getListVolumesMethod) == null) {
      synchronized (ControllerGrpc.class) {
        if ((getListVolumesMethod = ControllerGrpc.getListVolumesMethod) == null) {
          ControllerGrpc.getListVolumesMethod = getListVolumesMethod =
              io.grpc.MethodDescriptor.<csi.v0.Csi.ListVolumesRequest, csi.v0.Csi.ListVolumesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListVolumes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.ListVolumesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.ListVolumesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ControllerMethodDescriptorSupplier("ListVolumes"))
              .build();
        }
      }
    }
    return getListVolumesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<csi.v0.Csi.GetCapacityRequest,
      csi.v0.Csi.GetCapacityResponse> getGetCapacityMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetCapacity",
      requestType = csi.v0.Csi.GetCapacityRequest.class,
      responseType = csi.v0.Csi.GetCapacityResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<csi.v0.Csi.GetCapacityRequest,
      csi.v0.Csi.GetCapacityResponse> getGetCapacityMethod() {
    io.grpc.MethodDescriptor<csi.v0.Csi.GetCapacityRequest, csi.v0.Csi.GetCapacityResponse> getGetCapacityMethod;
    if ((getGetCapacityMethod = ControllerGrpc.getGetCapacityMethod) == null) {
      synchronized (ControllerGrpc.class) {
        if ((getGetCapacityMethod = ControllerGrpc.getGetCapacityMethod) == null) {
          ControllerGrpc.getGetCapacityMethod = getGetCapacityMethod =
              io.grpc.MethodDescriptor.<csi.v0.Csi.GetCapacityRequest, csi.v0.Csi.GetCapacityResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetCapacity"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.GetCapacityRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.GetCapacityResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ControllerMethodDescriptorSupplier("GetCapacity"))
              .build();
        }
      }
    }
    return getGetCapacityMethod;
  }

  private static volatile io.grpc.MethodDescriptor<csi.v0.Csi.ControllerGetCapabilitiesRequest,
      csi.v0.Csi.ControllerGetCapabilitiesResponse> getControllerGetCapabilitiesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ControllerGetCapabilities",
      requestType = csi.v0.Csi.ControllerGetCapabilitiesRequest.class,
      responseType = csi.v0.Csi.ControllerGetCapabilitiesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<csi.v0.Csi.ControllerGetCapabilitiesRequest,
      csi.v0.Csi.ControllerGetCapabilitiesResponse> getControllerGetCapabilitiesMethod() {
    io.grpc.MethodDescriptor<csi.v0.Csi.ControllerGetCapabilitiesRequest, csi.v0.Csi.ControllerGetCapabilitiesResponse> getControllerGetCapabilitiesMethod;
    if ((getControllerGetCapabilitiesMethod = ControllerGrpc.getControllerGetCapabilitiesMethod) == null) {
      synchronized (ControllerGrpc.class) {
        if ((getControllerGetCapabilitiesMethod = ControllerGrpc.getControllerGetCapabilitiesMethod) == null) {
          ControllerGrpc.getControllerGetCapabilitiesMethod = getControllerGetCapabilitiesMethod =
              io.grpc.MethodDescriptor.<csi.v0.Csi.ControllerGetCapabilitiesRequest, csi.v0.Csi.ControllerGetCapabilitiesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ControllerGetCapabilities"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.ControllerGetCapabilitiesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.ControllerGetCapabilitiesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ControllerMethodDescriptorSupplier("ControllerGetCapabilities"))
              .build();
        }
      }
    }
    return getControllerGetCapabilitiesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<csi.v0.Csi.CreateSnapshotRequest,
      csi.v0.Csi.CreateSnapshotResponse> getCreateSnapshotMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateSnapshot",
      requestType = csi.v0.Csi.CreateSnapshotRequest.class,
      responseType = csi.v0.Csi.CreateSnapshotResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<csi.v0.Csi.CreateSnapshotRequest,
      csi.v0.Csi.CreateSnapshotResponse> getCreateSnapshotMethod() {
    io.grpc.MethodDescriptor<csi.v0.Csi.CreateSnapshotRequest, csi.v0.Csi.CreateSnapshotResponse> getCreateSnapshotMethod;
    if ((getCreateSnapshotMethod = ControllerGrpc.getCreateSnapshotMethod) == null) {
      synchronized (ControllerGrpc.class) {
        if ((getCreateSnapshotMethod = ControllerGrpc.getCreateSnapshotMethod) == null) {
          ControllerGrpc.getCreateSnapshotMethod = getCreateSnapshotMethod =
              io.grpc.MethodDescriptor.<csi.v0.Csi.CreateSnapshotRequest, csi.v0.Csi.CreateSnapshotResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateSnapshot"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.CreateSnapshotRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.CreateSnapshotResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ControllerMethodDescriptorSupplier("CreateSnapshot"))
              .build();
        }
      }
    }
    return getCreateSnapshotMethod;
  }

  private static volatile io.grpc.MethodDescriptor<csi.v0.Csi.DeleteSnapshotRequest,
      csi.v0.Csi.DeleteSnapshotResponse> getDeleteSnapshotMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteSnapshot",
      requestType = csi.v0.Csi.DeleteSnapshotRequest.class,
      responseType = csi.v0.Csi.DeleteSnapshotResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<csi.v0.Csi.DeleteSnapshotRequest,
      csi.v0.Csi.DeleteSnapshotResponse> getDeleteSnapshotMethod() {
    io.grpc.MethodDescriptor<csi.v0.Csi.DeleteSnapshotRequest, csi.v0.Csi.DeleteSnapshotResponse> getDeleteSnapshotMethod;
    if ((getDeleteSnapshotMethod = ControllerGrpc.getDeleteSnapshotMethod) == null) {
      synchronized (ControllerGrpc.class) {
        if ((getDeleteSnapshotMethod = ControllerGrpc.getDeleteSnapshotMethod) == null) {
          ControllerGrpc.getDeleteSnapshotMethod = getDeleteSnapshotMethod =
              io.grpc.MethodDescriptor.<csi.v0.Csi.DeleteSnapshotRequest, csi.v0.Csi.DeleteSnapshotResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteSnapshot"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.DeleteSnapshotRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.DeleteSnapshotResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ControllerMethodDescriptorSupplier("DeleteSnapshot"))
              .build();
        }
      }
    }
    return getDeleteSnapshotMethod;
  }

  private static volatile io.grpc.MethodDescriptor<csi.v0.Csi.ListSnapshotsRequest,
      csi.v0.Csi.ListSnapshotsResponse> getListSnapshotsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListSnapshots",
      requestType = csi.v0.Csi.ListSnapshotsRequest.class,
      responseType = csi.v0.Csi.ListSnapshotsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<csi.v0.Csi.ListSnapshotsRequest,
      csi.v0.Csi.ListSnapshotsResponse> getListSnapshotsMethod() {
    io.grpc.MethodDescriptor<csi.v0.Csi.ListSnapshotsRequest, csi.v0.Csi.ListSnapshotsResponse> getListSnapshotsMethod;
    if ((getListSnapshotsMethod = ControllerGrpc.getListSnapshotsMethod) == null) {
      synchronized (ControllerGrpc.class) {
        if ((getListSnapshotsMethod = ControllerGrpc.getListSnapshotsMethod) == null) {
          ControllerGrpc.getListSnapshotsMethod = getListSnapshotsMethod =
              io.grpc.MethodDescriptor.<csi.v0.Csi.ListSnapshotsRequest, csi.v0.Csi.ListSnapshotsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListSnapshots"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.ListSnapshotsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.ListSnapshotsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ControllerMethodDescriptorSupplier("ListSnapshots"))
              .build();
        }
      }
    }
    return getListSnapshotsMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ControllerStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ControllerStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ControllerStub>() {
        @java.lang.Override
        public ControllerStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ControllerStub(channel, callOptions);
        }
      };
    return ControllerStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ControllerBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ControllerBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ControllerBlockingStub>() {
        @java.lang.Override
        public ControllerBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ControllerBlockingStub(channel, callOptions);
        }
      };
    return ControllerBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ControllerFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ControllerFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ControllerFutureStub>() {
        @java.lang.Override
        public ControllerFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ControllerFutureStub(channel, callOptions);
        }
      };
    return ControllerFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void createVolume(csi.v0.Csi.CreateVolumeRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.CreateVolumeResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateVolumeMethod(), responseObserver);
    }

    /**
     */
    default void deleteVolume(csi.v0.Csi.DeleteVolumeRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.DeleteVolumeResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteVolumeMethod(), responseObserver);
    }

    /**
     */
    default void controllerPublishVolume(csi.v0.Csi.ControllerPublishVolumeRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.ControllerPublishVolumeResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getControllerPublishVolumeMethod(), responseObserver);
    }

    /**
     */
    default void controllerUnpublishVolume(csi.v0.Csi.ControllerUnpublishVolumeRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.ControllerUnpublishVolumeResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getControllerUnpublishVolumeMethod(), responseObserver);
    }

    /**
     */
    default void validateVolumeCapabilities(csi.v0.Csi.ValidateVolumeCapabilitiesRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.ValidateVolumeCapabilitiesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getValidateVolumeCapabilitiesMethod(), responseObserver);
    }

    /**
     */
    default void listVolumes(csi.v0.Csi.ListVolumesRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.ListVolumesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListVolumesMethod(), responseObserver);
    }

    /**
     */
    default void getCapacity(csi.v0.Csi.GetCapacityRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.GetCapacityResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetCapacityMethod(), responseObserver);
    }

    /**
     */
    default void controllerGetCapabilities(csi.v0.Csi.ControllerGetCapabilitiesRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.ControllerGetCapabilitiesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getControllerGetCapabilitiesMethod(), responseObserver);
    }

    /**
     */
    default void createSnapshot(csi.v0.Csi.CreateSnapshotRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.CreateSnapshotResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateSnapshotMethod(), responseObserver);
    }

    /**
     */
    default void deleteSnapshot(csi.v0.Csi.DeleteSnapshotRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.DeleteSnapshotResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteSnapshotMethod(), responseObserver);
    }

    /**
     */
    default void listSnapshots(csi.v0.Csi.ListSnapshotsRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.ListSnapshotsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListSnapshotsMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service Controller.
   */
  public static abstract class ControllerImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return ControllerGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service Controller.
   */
  public static final class ControllerStub
      extends io.grpc.stub.AbstractAsyncStub<ControllerStub> {
    private ControllerStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ControllerStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ControllerStub(channel, callOptions);
    }

    /**
     */
    public void createVolume(csi.v0.Csi.CreateVolumeRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.CreateVolumeResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateVolumeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteVolume(csi.v0.Csi.DeleteVolumeRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.DeleteVolumeResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteVolumeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void controllerPublishVolume(csi.v0.Csi.ControllerPublishVolumeRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.ControllerPublishVolumeResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getControllerPublishVolumeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void controllerUnpublishVolume(csi.v0.Csi.ControllerUnpublishVolumeRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.ControllerUnpublishVolumeResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getControllerUnpublishVolumeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void validateVolumeCapabilities(csi.v0.Csi.ValidateVolumeCapabilitiesRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.ValidateVolumeCapabilitiesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getValidateVolumeCapabilitiesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listVolumes(csi.v0.Csi.ListVolumesRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.ListVolumesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListVolumesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getCapacity(csi.v0.Csi.GetCapacityRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.GetCapacityResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetCapacityMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void controllerGetCapabilities(csi.v0.Csi.ControllerGetCapabilitiesRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.ControllerGetCapabilitiesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getControllerGetCapabilitiesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createSnapshot(csi.v0.Csi.CreateSnapshotRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.CreateSnapshotResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateSnapshotMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteSnapshot(csi.v0.Csi.DeleteSnapshotRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.DeleteSnapshotResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteSnapshotMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listSnapshots(csi.v0.Csi.ListSnapshotsRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.ListSnapshotsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListSnapshotsMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service Controller.
   */
  public static final class ControllerBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<ControllerBlockingStub> {
    private ControllerBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ControllerBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ControllerBlockingStub(channel, callOptions);
    }

    /**
     */
    public csi.v0.Csi.CreateVolumeResponse createVolume(csi.v0.Csi.CreateVolumeRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateVolumeMethod(), getCallOptions(), request);
    }

    /**
     */
    public csi.v0.Csi.DeleteVolumeResponse deleteVolume(csi.v0.Csi.DeleteVolumeRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteVolumeMethod(), getCallOptions(), request);
    }

    /**
     */
    public csi.v0.Csi.ControllerPublishVolumeResponse controllerPublishVolume(csi.v0.Csi.ControllerPublishVolumeRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getControllerPublishVolumeMethod(), getCallOptions(), request);
    }

    /**
     */
    public csi.v0.Csi.ControllerUnpublishVolumeResponse controllerUnpublishVolume(csi.v0.Csi.ControllerUnpublishVolumeRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getControllerUnpublishVolumeMethod(), getCallOptions(), request);
    }

    /**
     */
    public csi.v0.Csi.ValidateVolumeCapabilitiesResponse validateVolumeCapabilities(csi.v0.Csi.ValidateVolumeCapabilitiesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getValidateVolumeCapabilitiesMethod(), getCallOptions(), request);
    }

    /**
     */
    public csi.v0.Csi.ListVolumesResponse listVolumes(csi.v0.Csi.ListVolumesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListVolumesMethod(), getCallOptions(), request);
    }

    /**
     */
    public csi.v0.Csi.GetCapacityResponse getCapacity(csi.v0.Csi.GetCapacityRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetCapacityMethod(), getCallOptions(), request);
    }

    /**
     */
    public csi.v0.Csi.ControllerGetCapabilitiesResponse controllerGetCapabilities(csi.v0.Csi.ControllerGetCapabilitiesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getControllerGetCapabilitiesMethod(), getCallOptions(), request);
    }

    /**
     */
    public csi.v0.Csi.CreateSnapshotResponse createSnapshot(csi.v0.Csi.CreateSnapshotRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateSnapshotMethod(), getCallOptions(), request);
    }

    /**
     */
    public csi.v0.Csi.DeleteSnapshotResponse deleteSnapshot(csi.v0.Csi.DeleteSnapshotRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteSnapshotMethod(), getCallOptions(), request);
    }

    /**
     */
    public csi.v0.Csi.ListSnapshotsResponse listSnapshots(csi.v0.Csi.ListSnapshotsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListSnapshotsMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service Controller.
   */
  public static final class ControllerFutureStub
      extends io.grpc.stub.AbstractFutureStub<ControllerFutureStub> {
    private ControllerFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ControllerFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ControllerFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<csi.v0.Csi.CreateVolumeResponse> createVolume(
        csi.v0.Csi.CreateVolumeRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateVolumeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<csi.v0.Csi.DeleteVolumeResponse> deleteVolume(
        csi.v0.Csi.DeleteVolumeRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteVolumeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<csi.v0.Csi.ControllerPublishVolumeResponse> controllerPublishVolume(
        csi.v0.Csi.ControllerPublishVolumeRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getControllerPublishVolumeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<csi.v0.Csi.ControllerUnpublishVolumeResponse> controllerUnpublishVolume(
        csi.v0.Csi.ControllerUnpublishVolumeRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getControllerUnpublishVolumeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<csi.v0.Csi.ValidateVolumeCapabilitiesResponse> validateVolumeCapabilities(
        csi.v0.Csi.ValidateVolumeCapabilitiesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getValidateVolumeCapabilitiesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<csi.v0.Csi.ListVolumesResponse> listVolumes(
        csi.v0.Csi.ListVolumesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListVolumesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<csi.v0.Csi.GetCapacityResponse> getCapacity(
        csi.v0.Csi.GetCapacityRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetCapacityMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<csi.v0.Csi.ControllerGetCapabilitiesResponse> controllerGetCapabilities(
        csi.v0.Csi.ControllerGetCapabilitiesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getControllerGetCapabilitiesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<csi.v0.Csi.CreateSnapshotResponse> createSnapshot(
        csi.v0.Csi.CreateSnapshotRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateSnapshotMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<csi.v0.Csi.DeleteSnapshotResponse> deleteSnapshot(
        csi.v0.Csi.DeleteSnapshotRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteSnapshotMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<csi.v0.Csi.ListSnapshotsResponse> listSnapshots(
        csi.v0.Csi.ListSnapshotsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListSnapshotsMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_VOLUME = 0;
  private static final int METHODID_DELETE_VOLUME = 1;
  private static final int METHODID_CONTROLLER_PUBLISH_VOLUME = 2;
  private static final int METHODID_CONTROLLER_UNPUBLISH_VOLUME = 3;
  private static final int METHODID_VALIDATE_VOLUME_CAPABILITIES = 4;
  private static final int METHODID_LIST_VOLUMES = 5;
  private static final int METHODID_GET_CAPACITY = 6;
  private static final int METHODID_CONTROLLER_GET_CAPABILITIES = 7;
  private static final int METHODID_CREATE_SNAPSHOT = 8;
  private static final int METHODID_DELETE_SNAPSHOT = 9;
  private static final int METHODID_LIST_SNAPSHOTS = 10;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CREATE_VOLUME:
          serviceImpl.createVolume((csi.v0.Csi.CreateVolumeRequest) request,
              (io.grpc.stub.StreamObserver<csi.v0.Csi.CreateVolumeResponse>) responseObserver);
          break;
        case METHODID_DELETE_VOLUME:
          serviceImpl.deleteVolume((csi.v0.Csi.DeleteVolumeRequest) request,
              (io.grpc.stub.StreamObserver<csi.v0.Csi.DeleteVolumeResponse>) responseObserver);
          break;
        case METHODID_CONTROLLER_PUBLISH_VOLUME:
          serviceImpl.controllerPublishVolume((csi.v0.Csi.ControllerPublishVolumeRequest) request,
              (io.grpc.stub.StreamObserver<csi.v0.Csi.ControllerPublishVolumeResponse>) responseObserver);
          break;
        case METHODID_CONTROLLER_UNPUBLISH_VOLUME:
          serviceImpl.controllerUnpublishVolume((csi.v0.Csi.ControllerUnpublishVolumeRequest) request,
              (io.grpc.stub.StreamObserver<csi.v0.Csi.ControllerUnpublishVolumeResponse>) responseObserver);
          break;
        case METHODID_VALIDATE_VOLUME_CAPABILITIES:
          serviceImpl.validateVolumeCapabilities((csi.v0.Csi.ValidateVolumeCapabilitiesRequest) request,
              (io.grpc.stub.StreamObserver<csi.v0.Csi.ValidateVolumeCapabilitiesResponse>) responseObserver);
          break;
        case METHODID_LIST_VOLUMES:
          serviceImpl.listVolumes((csi.v0.Csi.ListVolumesRequest) request,
              (io.grpc.stub.StreamObserver<csi.v0.Csi.ListVolumesResponse>) responseObserver);
          break;
        case METHODID_GET_CAPACITY:
          serviceImpl.getCapacity((csi.v0.Csi.GetCapacityRequest) request,
              (io.grpc.stub.StreamObserver<csi.v0.Csi.GetCapacityResponse>) responseObserver);
          break;
        case METHODID_CONTROLLER_GET_CAPABILITIES:
          serviceImpl.controllerGetCapabilities((csi.v0.Csi.ControllerGetCapabilitiesRequest) request,
              (io.grpc.stub.StreamObserver<csi.v0.Csi.ControllerGetCapabilitiesResponse>) responseObserver);
          break;
        case METHODID_CREATE_SNAPSHOT:
          serviceImpl.createSnapshot((csi.v0.Csi.CreateSnapshotRequest) request,
              (io.grpc.stub.StreamObserver<csi.v0.Csi.CreateSnapshotResponse>) responseObserver);
          break;
        case METHODID_DELETE_SNAPSHOT:
          serviceImpl.deleteSnapshot((csi.v0.Csi.DeleteSnapshotRequest) request,
              (io.grpc.stub.StreamObserver<csi.v0.Csi.DeleteSnapshotResponse>) responseObserver);
          break;
        case METHODID_LIST_SNAPSHOTS:
          serviceImpl.listSnapshots((csi.v0.Csi.ListSnapshotsRequest) request,
              (io.grpc.stub.StreamObserver<csi.v0.Csi.ListSnapshotsResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getCreateVolumeMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              csi.v0.Csi.CreateVolumeRequest,
              csi.v0.Csi.CreateVolumeResponse>(
                service, METHODID_CREATE_VOLUME)))
        .addMethod(
          getDeleteVolumeMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              csi.v0.Csi.DeleteVolumeRequest,
              csi.v0.Csi.DeleteVolumeResponse>(
                service, METHODID_DELETE_VOLUME)))
        .addMethod(
          getControllerPublishVolumeMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              csi.v0.Csi.ControllerPublishVolumeRequest,
              csi.v0.Csi.ControllerPublishVolumeResponse>(
                service, METHODID_CONTROLLER_PUBLISH_VOLUME)))
        .addMethod(
          getControllerUnpublishVolumeMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              csi.v0.Csi.ControllerUnpublishVolumeRequest,
              csi.v0.Csi.ControllerUnpublishVolumeResponse>(
                service, METHODID_CONTROLLER_UNPUBLISH_VOLUME)))
        .addMethod(
          getValidateVolumeCapabilitiesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              csi.v0.Csi.ValidateVolumeCapabilitiesRequest,
              csi.v0.Csi.ValidateVolumeCapabilitiesResponse>(
                service, METHODID_VALIDATE_VOLUME_CAPABILITIES)))
        .addMethod(
          getListVolumesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              csi.v0.Csi.ListVolumesRequest,
              csi.v0.Csi.ListVolumesResponse>(
                service, METHODID_LIST_VOLUMES)))
        .addMethod(
          getGetCapacityMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              csi.v0.Csi.GetCapacityRequest,
              csi.v0.Csi.GetCapacityResponse>(
                service, METHODID_GET_CAPACITY)))
        .addMethod(
          getControllerGetCapabilitiesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              csi.v0.Csi.ControllerGetCapabilitiesRequest,
              csi.v0.Csi.ControllerGetCapabilitiesResponse>(
                service, METHODID_CONTROLLER_GET_CAPABILITIES)))
        .addMethod(
          getCreateSnapshotMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              csi.v0.Csi.CreateSnapshotRequest,
              csi.v0.Csi.CreateSnapshotResponse>(
                service, METHODID_CREATE_SNAPSHOT)))
        .addMethod(
          getDeleteSnapshotMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              csi.v0.Csi.DeleteSnapshotRequest,
              csi.v0.Csi.DeleteSnapshotResponse>(
                service, METHODID_DELETE_SNAPSHOT)))
        .addMethod(
          getListSnapshotsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              csi.v0.Csi.ListSnapshotsRequest,
              csi.v0.Csi.ListSnapshotsResponse>(
                service, METHODID_LIST_SNAPSHOTS)))
        .build();
  }

  private static abstract class ControllerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ControllerBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return csi.v0.Csi.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Controller");
    }
  }

  private static final class ControllerFileDescriptorSupplier
      extends ControllerBaseDescriptorSupplier {
    ControllerFileDescriptorSupplier() {}
  }

  private static final class ControllerMethodDescriptorSupplier
      extends ControllerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    ControllerMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (ControllerGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ControllerFileDescriptorSupplier())
              .addMethod(getCreateVolumeMethod())
              .addMethod(getDeleteVolumeMethod())
              .addMethod(getControllerPublishVolumeMethod())
              .addMethod(getControllerUnpublishVolumeMethod())
              .addMethod(getValidateVolumeCapabilitiesMethod())
              .addMethod(getListVolumesMethod())
              .addMethod(getGetCapacityMethod())
              .addMethod(getControllerGetCapabilitiesMethod())
              .addMethod(getCreateSnapshotMethod())
              .addMethod(getDeleteSnapshotMethod())
              .addMethod(getListSnapshotsMethod())
              .build();
        }
      }
    }
    return result;
  }
}
