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
public final class IdentityGrpc {

  private IdentityGrpc() {}

  public static final java.lang.String SERVICE_NAME = "csi.v0.Identity";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<csi.v0.Csi.GetPluginInfoRequest,
      csi.v0.Csi.GetPluginInfoResponse> getGetPluginInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetPluginInfo",
      requestType = csi.v0.Csi.GetPluginInfoRequest.class,
      responseType = csi.v0.Csi.GetPluginInfoResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<csi.v0.Csi.GetPluginInfoRequest,
      csi.v0.Csi.GetPluginInfoResponse> getGetPluginInfoMethod() {
    io.grpc.MethodDescriptor<csi.v0.Csi.GetPluginInfoRequest, csi.v0.Csi.GetPluginInfoResponse> getGetPluginInfoMethod;
    if ((getGetPluginInfoMethod = IdentityGrpc.getGetPluginInfoMethod) == null) {
      synchronized (IdentityGrpc.class) {
        if ((getGetPluginInfoMethod = IdentityGrpc.getGetPluginInfoMethod) == null) {
          IdentityGrpc.getGetPluginInfoMethod = getGetPluginInfoMethod =
              io.grpc.MethodDescriptor.<csi.v0.Csi.GetPluginInfoRequest, csi.v0.Csi.GetPluginInfoResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetPluginInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.GetPluginInfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.GetPluginInfoResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityMethodDescriptorSupplier("GetPluginInfo"))
              .build();
        }
      }
    }
    return getGetPluginInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<csi.v0.Csi.GetPluginCapabilitiesRequest,
      csi.v0.Csi.GetPluginCapabilitiesResponse> getGetPluginCapabilitiesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetPluginCapabilities",
      requestType = csi.v0.Csi.GetPluginCapabilitiesRequest.class,
      responseType = csi.v0.Csi.GetPluginCapabilitiesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<csi.v0.Csi.GetPluginCapabilitiesRequest,
      csi.v0.Csi.GetPluginCapabilitiesResponse> getGetPluginCapabilitiesMethod() {
    io.grpc.MethodDescriptor<csi.v0.Csi.GetPluginCapabilitiesRequest, csi.v0.Csi.GetPluginCapabilitiesResponse> getGetPluginCapabilitiesMethod;
    if ((getGetPluginCapabilitiesMethod = IdentityGrpc.getGetPluginCapabilitiesMethod) == null) {
      synchronized (IdentityGrpc.class) {
        if ((getGetPluginCapabilitiesMethod = IdentityGrpc.getGetPluginCapabilitiesMethod) == null) {
          IdentityGrpc.getGetPluginCapabilitiesMethod = getGetPluginCapabilitiesMethod =
              io.grpc.MethodDescriptor.<csi.v0.Csi.GetPluginCapabilitiesRequest, csi.v0.Csi.GetPluginCapabilitiesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetPluginCapabilities"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.GetPluginCapabilitiesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.GetPluginCapabilitiesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityMethodDescriptorSupplier("GetPluginCapabilities"))
              .build();
        }
      }
    }
    return getGetPluginCapabilitiesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<csi.v0.Csi.ProbeRequest,
      csi.v0.Csi.ProbeResponse> getProbeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Probe",
      requestType = csi.v0.Csi.ProbeRequest.class,
      responseType = csi.v0.Csi.ProbeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<csi.v0.Csi.ProbeRequest,
      csi.v0.Csi.ProbeResponse> getProbeMethod() {
    io.grpc.MethodDescriptor<csi.v0.Csi.ProbeRequest, csi.v0.Csi.ProbeResponse> getProbeMethod;
    if ((getProbeMethod = IdentityGrpc.getProbeMethod) == null) {
      synchronized (IdentityGrpc.class) {
        if ((getProbeMethod = IdentityGrpc.getProbeMethod) == null) {
          IdentityGrpc.getProbeMethod = getProbeMethod =
              io.grpc.MethodDescriptor.<csi.v0.Csi.ProbeRequest, csi.v0.Csi.ProbeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Probe"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.ProbeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  csi.v0.Csi.ProbeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new IdentityMethodDescriptorSupplier("Probe"))
              .build();
        }
      }
    }
    return getProbeMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static IdentityStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<IdentityStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<IdentityStub>() {
        @java.lang.Override
        public IdentityStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new IdentityStub(channel, callOptions);
        }
      };
    return IdentityStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static IdentityBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<IdentityBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<IdentityBlockingStub>() {
        @java.lang.Override
        public IdentityBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new IdentityBlockingStub(channel, callOptions);
        }
      };
    return IdentityBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static IdentityFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<IdentityFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<IdentityFutureStub>() {
        @java.lang.Override
        public IdentityFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new IdentityFutureStub(channel, callOptions);
        }
      };
    return IdentityFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void getPluginInfo(csi.v0.Csi.GetPluginInfoRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.GetPluginInfoResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetPluginInfoMethod(), responseObserver);
    }

    /**
     */
    default void getPluginCapabilities(csi.v0.Csi.GetPluginCapabilitiesRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.GetPluginCapabilitiesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetPluginCapabilitiesMethod(), responseObserver);
    }

    /**
     */
    default void probe(csi.v0.Csi.ProbeRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.ProbeResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getProbeMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service Identity.
   */
  public static abstract class IdentityImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return IdentityGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service Identity.
   */
  public static final class IdentityStub
      extends io.grpc.stub.AbstractAsyncStub<IdentityStub> {
    private IdentityStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected IdentityStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new IdentityStub(channel, callOptions);
    }

    /**
     */
    public void getPluginInfo(csi.v0.Csi.GetPluginInfoRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.GetPluginInfoResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetPluginInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getPluginCapabilities(csi.v0.Csi.GetPluginCapabilitiesRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.GetPluginCapabilitiesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetPluginCapabilitiesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void probe(csi.v0.Csi.ProbeRequest request,
        io.grpc.stub.StreamObserver<csi.v0.Csi.ProbeResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getProbeMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service Identity.
   */
  public static final class IdentityBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<IdentityBlockingStub> {
    private IdentityBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected IdentityBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new IdentityBlockingStub(channel, callOptions);
    }

    /**
     */
    public csi.v0.Csi.GetPluginInfoResponse getPluginInfo(csi.v0.Csi.GetPluginInfoRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetPluginInfoMethod(), getCallOptions(), request);
    }

    /**
     */
    public csi.v0.Csi.GetPluginCapabilitiesResponse getPluginCapabilities(csi.v0.Csi.GetPluginCapabilitiesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetPluginCapabilitiesMethod(), getCallOptions(), request);
    }

    /**
     */
    public csi.v0.Csi.ProbeResponse probe(csi.v0.Csi.ProbeRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getProbeMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service Identity.
   */
  public static final class IdentityFutureStub
      extends io.grpc.stub.AbstractFutureStub<IdentityFutureStub> {
    private IdentityFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected IdentityFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new IdentityFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<csi.v0.Csi.GetPluginInfoResponse> getPluginInfo(
        csi.v0.Csi.GetPluginInfoRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetPluginInfoMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<csi.v0.Csi.GetPluginCapabilitiesResponse> getPluginCapabilities(
        csi.v0.Csi.GetPluginCapabilitiesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetPluginCapabilitiesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<csi.v0.Csi.ProbeResponse> probe(
        csi.v0.Csi.ProbeRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getProbeMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_PLUGIN_INFO = 0;
  private static final int METHODID_GET_PLUGIN_CAPABILITIES = 1;
  private static final int METHODID_PROBE = 2;

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
        case METHODID_GET_PLUGIN_INFO:
          serviceImpl.getPluginInfo((csi.v0.Csi.GetPluginInfoRequest) request,
              (io.grpc.stub.StreamObserver<csi.v0.Csi.GetPluginInfoResponse>) responseObserver);
          break;
        case METHODID_GET_PLUGIN_CAPABILITIES:
          serviceImpl.getPluginCapabilities((csi.v0.Csi.GetPluginCapabilitiesRequest) request,
              (io.grpc.stub.StreamObserver<csi.v0.Csi.GetPluginCapabilitiesResponse>) responseObserver);
          break;
        case METHODID_PROBE:
          serviceImpl.probe((csi.v0.Csi.ProbeRequest) request,
              (io.grpc.stub.StreamObserver<csi.v0.Csi.ProbeResponse>) responseObserver);
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
          getGetPluginInfoMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              csi.v0.Csi.GetPluginInfoRequest,
              csi.v0.Csi.GetPluginInfoResponse>(
                service, METHODID_GET_PLUGIN_INFO)))
        .addMethod(
          getGetPluginCapabilitiesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              csi.v0.Csi.GetPluginCapabilitiesRequest,
              csi.v0.Csi.GetPluginCapabilitiesResponse>(
                service, METHODID_GET_PLUGIN_CAPABILITIES)))
        .addMethod(
          getProbeMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              csi.v0.Csi.ProbeRequest,
              csi.v0.Csi.ProbeResponse>(
                service, METHODID_PROBE)))
        .build();
  }

  private static abstract class IdentityBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    IdentityBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return csi.v0.Csi.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Identity");
    }
  }

  private static final class IdentityFileDescriptorSupplier
      extends IdentityBaseDescriptorSupplier {
    IdentityFileDescriptorSupplier() {}
  }

  private static final class IdentityMethodDescriptorSupplier
      extends IdentityBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    IdentityMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (IdentityGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new IdentityFileDescriptorSupplier())
              .addMethod(getGetPluginInfoMethod())
              .addMethod(getGetPluginCapabilitiesMethod())
              .addMethod(getProbeMethod())
              .build();
        }
      }
    }
    return result;
  }
}
