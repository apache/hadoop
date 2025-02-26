/**
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

package org.apache.hadoop.ipc;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.ProcessingDetails.Timing;
import org.apache.hadoop.ipc.RPC.RpcInvoker;
import org.apache.hadoop.ipc.protobuf.ProtobufRpcEngine2Protos.RequestHeaderProto;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.protobuf.BlockingService;
import org.apache.hadoop.thirdparty.protobuf.Descriptors.MethodDescriptor;
import org.apache.hadoop.thirdparty.protobuf.Message;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.AsyncGet;
import org.apache.hadoop.tracing.Tracer;
import org.apache.hadoop.tracing.TraceScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RPC Engine for for protobuf based RPCs.
 */
@InterfaceStability.Evolving
public class ProtobufRpcEngine2 implements RpcEngine {
  public static final Logger LOG =
      LoggerFactory.getLogger(ProtobufRpcEngine2.class);
  private static final ThreadLocal<AsyncGet<Message, Exception>>
      ASYNC_RETURN_MESSAGE = new ThreadLocal<>();

  static { // Register the rpcRequest deserializer for ProtobufRpcEngine
    registerProtocolEngine();
  }

  static void registerProtocolEngine() {
    if (Server.getRpcInvoker(RPC.RpcKind.RPC_PROTOCOL_BUFFER) == null) {
      org.apache.hadoop.ipc.Server
          .registerProtocolEngine(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
              ProtobufRpcEngine2.RpcProtobufRequest.class,
              new Server.ProtoBufRpcInvoker());
    }
  }

  private static final ClientCache CLIENTS = new ClientCache();

  @Unstable
  public static AsyncGet<Message, Exception> getAsyncReturnMessage() {
    return ASYNC_RETURN_MESSAGE.get();
  }

  public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory, int rpcTimeout) throws IOException {
    return getProxy(protocol, clientVersion, addr, ticket, conf, factory,
        rpcTimeout, null);
  }

  @Override
  public <T> ProtocolProxy<T> getProxy(
      Class<T> protocol, long clientVersion,
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory, int rpcTimeout, RetryPolicy connectionRetryPolicy)
      throws IOException {
    return getProxy(protocol, clientVersion, addr, ticket, conf, factory,
      rpcTimeout, connectionRetryPolicy, null, null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
      ConnectionId connId, Configuration conf, SocketFactory factory,
      AlignmentContext alignmentContext) throws IOException {
    final Invoker invoker = new Invoker(protocol, connId, conf, factory, alignmentContext);
    return new ProtocolProxy<T>(protocol, (T) Proxy.newProxyInstance(
        protocol.getClassLoader(), new Class[] {protocol}, invoker), false);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory, int rpcTimeout, RetryPolicy connectionRetryPolicy,
      AtomicBoolean fallbackToSimpleAuth, AlignmentContext alignmentContext)
      throws IOException {

    final Invoker invoker = new Invoker(protocol, addr, ticket, conf, factory,
        rpcTimeout, connectionRetryPolicy, fallbackToSimpleAuth,
        alignmentContext);
    return new ProtocolProxy<T>(protocol, (T) Proxy.newProxyInstance(
        protocol.getClassLoader(), new Class[]{protocol}, invoker), false);
  }

  @Override
  public ProtocolProxy<ProtocolMetaInfoPB> getProtocolMetaInfoProxy(
      ConnectionId connId, Configuration conf, SocketFactory factory)
      throws IOException {
    Class<ProtocolMetaInfoPB> protocol = ProtocolMetaInfoPB.class;
    return new ProtocolProxy<ProtocolMetaInfoPB>(protocol,
        (ProtocolMetaInfoPB) Proxy.newProxyInstance(protocol.getClassLoader(),
            new Class[]{protocol}, new Invoker(protocol, connId, conf,
                factory, null)), false);
  }

  protected static class Invoker implements RpcInvocationHandler {
    private final Map<String, Message> returnTypes =
        new ConcurrentHashMap<String, Message>();
    private boolean isClosed = false;
    private final Client.ConnectionId remoteId;
    private final Client client;
    private final long clientProtocolVersion;
    private final String protocolName;
    private AtomicBoolean fallbackToSimpleAuth;
    private AlignmentContext alignmentContext;

    protected Invoker(Class<?> protocol, InetSocketAddress addr,
        UserGroupInformation ticket, Configuration conf, SocketFactory factory,
        int rpcTimeout, RetryPolicy connectionRetryPolicy,
        AtomicBoolean fallbackToSimpleAuth, AlignmentContext alignmentContext)
        throws IOException {
      this(protocol, Client.ConnectionId.getConnectionId(
          addr, protocol, ticket, rpcTimeout, connectionRetryPolicy, conf),
          conf, factory, alignmentContext);
      this.fallbackToSimpleAuth = fallbackToSimpleAuth;
    }

    /**
     * This constructor takes a connectionId, instead of creating a new one.
     *
     * @param protocol input protocol.
     * @param connId input connId.
     * @param conf input Configuration.
     * @param factory input factory.
     * @param alignmentContext Alignment context
     */
    protected Invoker(Class<?> protocol, Client.ConnectionId connId,
        Configuration conf, SocketFactory factory, AlignmentContext alignmentContext) {
      this.remoteId = connId;
      this.client = CLIENTS.getClient(conf, factory, RpcWritable.Buffer.class);
      this.protocolName = RPC.getProtocolName(protocol);
      this.clientProtocolVersion = RPC
          .getProtocolVersion(protocol);
      this.alignmentContext = alignmentContext;
    }

    private RequestHeaderProto constructRpcRequestHeader(Method method) {
      RequestHeaderProto.Builder builder = RequestHeaderProto
          .newBuilder();
      builder.setMethodName(method.getName());


      // For protobuf, {@code protocol} used when creating client side proxy is
      // the interface extending BlockingInterface, which has the annotations
      // such as ProtocolName etc.
      //
      // Using Method.getDeclaringClass(), as in WritableEngine to get at
      // the protocol interface will return BlockingInterface, from where
      // the annotation ProtocolName and Version cannot be
      // obtained.
      //
      // Hence we simply use the protocol class used to create the proxy.
      // For PB this may limit the use of mixins on client side.
      builder.setDeclaringClassProtocolName(protocolName);
      builder.setClientProtocolVersion(clientProtocolVersion);
      return builder.build();
    }

    /**
     * This is the client side invoker of RPC method. It only throws
     * ServiceException, since the invocation proxy expects only
     * ServiceException to be thrown by the method in case protobuf service.
     *
     * ServiceException has the following causes:
     * <ol>
     * <li>Exceptions encountered on the client side in this method are
     * set as cause in ServiceException as is.</li>
     * <li>Exceptions from the server are wrapped in RemoteException and are
     * set as cause in ServiceException</li>
     * </ol>
     *
     * Note that the client calling protobuf RPC methods, must handle
     * ServiceException by getting the cause from the ServiceException. If the
     * cause is RemoteException, then unwrap it to get the exception thrown by
     * the server.
     */
    @Override
    public Message invoke(Object proxy, final Method method, Object[] args)
        throws ServiceException {
      long startTime = 0;
      if (LOG.isDebugEnabled()) {
        startTime = Time.monotonicNow();
      }

      if (args.length != 2) { // RpcController + Message
        throw new ServiceException(
            "Too many or few parameters for request. Method: ["
            + method.getName() + "]" + ", Expected: 2, Actual: "
            + args.length);
      }
      if (args[1] == null) {
        throw new ServiceException("null param while calling Method: ["
            + method.getName() + "]");
      }

      // if Tracing is on then start a new span for this rpc.
      // guard it in the if statement to make sure there isn't
      // any extra string manipulation.
      Tracer tracer = Tracer.curThreadTracer();
      TraceScope traceScope = null;
      if (tracer != null) {
        traceScope = tracer.newScope(RpcClientUtil.methodToTraceString(method));
      }

      if (LOG.isTraceEnabled()) {
        LOG.trace(Thread.currentThread().getId() + ": Call -> " +
            remoteId + ": " + method.getName() +
            " {" + TextFormat.shortDebugString((Message) args[1]) + "}");
      }


      final Message theRequest = (Message) args[1];
      final RpcWritable.Buffer val;
      try {
        val = (RpcWritable.Buffer) client.call(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
            constructRpcRequest(method, theRequest), remoteId,
            fallbackToSimpleAuth, alignmentContext);

      } catch (Throwable e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(Thread.currentThread().getId() + ": Exception <- " +
              remoteId + ": " + method.getName() +
                " {" + e + "}");
        }
        if (traceScope != null) {
          traceScope.addTimelineAnnotation("Call got exception: " +
              e.toString());
        }
        throw new ServiceException(e);
      } finally {
        if (traceScope != null) {
          traceScope.close();
        }
      }

      if (LOG.isDebugEnabled()) {
        long callTime = Time.monotonicNow() - startTime;
        LOG.debug("Call: " + method.getName() + " took " + callTime + "ms");
      }

      if (Client.isAsynchronousMode()) {
        final AsyncGet<RpcWritable.Buffer, IOException> arr
            = Client.getAsyncRpcResponse();
        final AsyncGet<Message, Exception> asyncGet =
            new AsyncGet<Message, Exception>() {
              @Override
              public Message get(long timeout, TimeUnit unit) throws Exception {
                return getReturnMessage(method, arr.get(timeout, unit));
              }

              @Override
              public boolean isDone() {
            return arr.isDone();
          }
        };
        ASYNC_RETURN_MESSAGE.set(asyncGet);
        return null;
      } else {
        return getReturnMessage(method, val);
      }
    }

    protected Writable constructRpcRequest(Method method, Message theRequest) {
      RequestHeaderProto rpcRequestHeader = constructRpcRequestHeader(method);
      return new RpcProtobufRequest(rpcRequestHeader, theRequest);
    }

    private Message getReturnMessage(final Method method,
        final RpcWritable.Buffer buf) throws ServiceException {
      Message prototype = null;
      try {
        prototype = getReturnProtoType(method);
      } catch (Exception e) {
        throw new ServiceException(e);
      }
      Message returnMessage;
      try {
        returnMessage = buf.getValue(prototype.getDefaultInstanceForType());

        if (LOG.isTraceEnabled()) {
          LOG.trace(Thread.currentThread().getId() + ": Response <- " +
              remoteId + ": " + method.getName() +
                " {" + TextFormat.shortDebugString(returnMessage) + "}");
        }

      } catch (Throwable e) {
        throw new ServiceException(e);
      }
      return returnMessage;
    }

    @Override
    public void close() throws IOException {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }

    private Message getReturnProtoType(Method method) throws Exception {
      if (returnTypes.containsKey(method.getName())) {
        return returnTypes.get(method.getName());
      }

      Class<?> returnType = method.getReturnType();
      Method newInstMethod = returnType.getMethod("getDefaultInstance");
      newInstMethod.setAccessible(true);
      Message prototype = (Message) newInstMethod.invoke(null, (Object[]) null);
      returnTypes.put(method.getName(), prototype);
      return prototype;
    }

    @Override //RpcInvocationHandler
    public ConnectionId getConnectionId() {
      return remoteId;
    }

    protected long getClientProtocolVersion() {
      return clientProtocolVersion;
    }

    protected String getProtocolName() {
      return protocolName;
    }
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  static Client getClient(Configuration conf) {
    return CLIENTS.getClient(conf, SocketFactory.getDefault(),
        RpcWritable.Buffer.class);
  }



  @Override
  public RPC.Server getServer(Class<?> protocol, Object protocolImpl,
      String bindAddress, int port, int numHandlers, int numReaders,
      int queueSizePerHandler, boolean verbose, Configuration conf,
      SecretManager<? extends TokenIdentifier> secretManager,
      String portRangeConfig, AlignmentContext alignmentContext)
      throws IOException {
    return new Server(protocol, protocolImpl, conf, bindAddress, port,
        numHandlers, numReaders, queueSizePerHandler, verbose, secretManager,
        portRangeConfig, alignmentContext);
  }

  @VisibleForTesting
  public static void clearClientCache() {
    CLIENTS.clearCache();
  }

  public static class Server extends RPC.Server {

    static final ThreadLocal<ProtobufRpcEngineCallback2> CURRENT_CALLBACK =
        new ThreadLocal<>();

    static final ThreadLocal<CallInfo> CURRENT_CALL_INFO = new ThreadLocal<>();

    static class CallInfo {
      private final RPC.Server server;
      private final String methodName;

      CallInfo(RPC.Server server, String methodName) {
        this.server = server;
        this.methodName = methodName;
      }

      public RPC.Server getServer() {
        return server;
      }

      public String getMethodName() {
        return methodName;
      }
    }

    static class ProtobufRpcEngineCallbackImpl
        implements ProtobufRpcEngineCallback2 {

      private final RPC.Server server;
      private final Call call;
      private final String methodName;

      ProtobufRpcEngineCallbackImpl() {
        this.server = CURRENT_CALL_INFO.get().getServer();
        this.call = Server.getCurCall().get();
        this.methodName = CURRENT_CALL_INFO.get().getMethodName();
      }

      private void updateProcessingDetails(Call rpcCall, long deltaNanos) {
        ProcessingDetails details = rpcCall.getProcessingDetails();
        rpcCall.getProcessingDetails().set(Timing.PROCESSING, deltaNanos, TimeUnit.NANOSECONDS);
        deltaNanos -= details.get(Timing.LOCKWAIT, TimeUnit.NANOSECONDS);
        deltaNanos -= details.get(Timing.LOCKSHARED, TimeUnit.NANOSECONDS);
        deltaNanos -= details.get(Timing.LOCKEXCLUSIVE, TimeUnit.NANOSECONDS);
        details.set(Timing.LOCKFREE, deltaNanos, TimeUnit.NANOSECONDS);
      }

      @Override
      public void setResponse(Message message) {
        long deltaNanos = Time.monotonicNowNanos() - call.getStartHandleTimestampNanos();
        updateProcessingDetails(call, deltaNanos);
        call.setDeferredResponse(RpcWritable.wrap(message));
        server.updateDeferredMetrics(call, methodName, deltaNanos);
      }

      @Override
      public void error(Throwable t) {
        long deltaNanos = Time.monotonicNowNanos() - call.getStartHandleTimestampNanos();
        updateProcessingDetails(call, deltaNanos);
        call.setDeferredError(t);
        String detailedMetricsName = t.getClass().getSimpleName();
        server.updateDeferredMetrics(call, detailedMetricsName, deltaNanos);
      }
    }

    @InterfaceStability.Unstable
    public static ProtobufRpcEngineCallback2 registerForDeferredResponse2() {
      ProtobufRpcEngineCallback2 callback = new ProtobufRpcEngineCallbackImpl();
      CURRENT_CALLBACK.set(callback);
      return callback;
    }

    /**
     * Construct an RPC server.
     *
     * @param protocolClass the class of protocol
     * @param protocolImpl the protocolImpl whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param numReaders number of read threads
     * @param queueSizePerHandler the size of the queue contained
     *                            in each Handler
     * @param verbose whether each call should be logged
     * @param secretManager the server-side secret manager for each token type
     * @param portRangeConfig A config parameter that can be used to restrict
     * the range of ports used when port is 0 (an ephemeral port)
     * @param alignmentContext provides server state info on client responses
     * @throws IOException raised on errors performing I/O.
     */
    public Server(Class<?> protocolClass, Object protocolImpl,
        Configuration conf, String bindAddress, int port, int numHandlers,
        int numReaders, int queueSizePerHandler, boolean verbose,
        SecretManager<? extends TokenIdentifier> secretManager,
        String portRangeConfig, AlignmentContext alignmentContext)
        throws IOException {
      super(bindAddress, port, null, numHandlers,
          numReaders, queueSizePerHandler, conf,
          serverNameFromClass(protocolImpl.getClass()), secretManager,
          portRangeConfig);
      setAlignmentContext(alignmentContext);
      this.verbose = verbose;
      registerProtocolAndImpl(RPC.RpcKind.RPC_PROTOCOL_BUFFER, protocolClass,
          protocolImpl);
    }

    //Use the latest protobuf rpc invoker itself as that is backward compatible.
    private static final RpcInvoker RPC_INVOKER = new ProtoBufRpcInvoker();

    @Override
    protected RpcInvoker getServerRpcInvoker(RPC.RpcKind rpcKind) {
      if (rpcKind == RPC.RpcKind.RPC_PROTOCOL_BUFFER) {
        return RPC_INVOKER;
      }
      return super.getServerRpcInvoker(rpcKind);
    }

    /**
     * Protobuf invoker for {@link RpcInvoker}.
     */
    static class ProtoBufRpcInvoker implements RpcInvoker {
      private static ProtoClassProtoImpl getProtocolImpl(RPC.Server server,
          String protoName, long clientVersion) throws RpcServerException {
        ProtoNameVer pv = new ProtoNameVer(protoName, clientVersion);
        ProtoClassProtoImpl impl =
            server.getProtocolImplMap(RPC.RpcKind.RPC_PROTOCOL_BUFFER).get(pv);
        if (impl == null) { // no match for Protocol AND Version
          VerProtocolImpl highest = server.getHighestSupportedProtocol(
              RPC.RpcKind.RPC_PROTOCOL_BUFFER, protoName);
          if (highest == null) {
            throw new RpcNoSuchProtocolException(
                "Unknown protocol: " + protoName);
          }
          // protocol supported but not the version that client wants
          throw new RPC.VersionMismatch(protoName, clientVersion,
              highest.version);
        }
        return impl;
      }

      @Override
      /**
       * This is a server side method, which is invoked over RPC. On success
       * the return response has protobuf response payload. On failure, the
       * exception name and the stack trace are returned in the response.
       * See {@link HadoopRpcResponseProto}
       *
       * In this method there three types of exceptions possible and they are
       * returned in response as follows.
       * <ol>
       * <li> Exceptions encountered in this method that are returned
       * as {@link RpcServerException} </li>
       * <li> Exceptions thrown by the service is wrapped in ServiceException.
       * In that this method returns in response the exception thrown by the
       * service.</li>
       * <li> Other exceptions thrown by the service. They are returned as
       * it is.</li>
       * </ol>
       */
      public Writable call(RPC.Server server, String connectionProtocolName,
          Writable writableRequest, long receiveTime) throws Exception {
        RpcProtobufRequest request = (RpcProtobufRequest) writableRequest;
        RequestHeaderProto rpcRequest = request.getRequestHeader();
        String methodName = rpcRequest.getMethodName();

        /**
         * RPCs for a particular interface (ie protocol) are done using a
         * IPC connection that is setup using rpcProxy.
         * The rpcProxy's has a declared protocol name that is
         * sent form client to server at connection time.
         *
         * Each Rpc call also sends a protocol name
         * (called declaringClassprotocolName). This name is usually the same
         * as the connection protocol name except in some cases.
         * For example metaProtocols such ProtocolInfoProto which get info
         * about the protocol reuse the connection but need to indicate that
         * the actual protocol is different (i.e. the protocol is
         * ProtocolInfoProto) since they reuse the connection; in this case
         * the declaringClassProtocolName field is set to the ProtocolInfoProto.
         */

        String declaringClassProtoName =
            rpcRequest.getDeclaringClassProtocolName();
        long clientVersion = rpcRequest.getClientProtocolVersion();
        return call(server, connectionProtocolName, request, receiveTime,
            methodName, declaringClassProtoName, clientVersion);
      }

      @SuppressWarnings("deprecation")
      protected Writable call(RPC.Server server, String connectionProtocolName,
          RpcWritable.Buffer request, long receiveTime, String methodName,
          String declaringClassProtoName, long clientVersion) throws Exception {
        if (server.verbose) {
          LOG.info("Call: connectionProtocolName=" + connectionProtocolName +
              ", method=" + methodName);
        }

        ProtoClassProtoImpl protocolImpl = getProtocolImpl(server,
                              declaringClassProtoName, clientVersion);
        if (protocolImpl.isShadedPBImpl()) {
          return call(server, connectionProtocolName, request, methodName,
              protocolImpl);
        }
        //Legacy protobuf implementation. Handle using legacy (Non-shaded)
        // protobuf classes.
        return ProtobufRpcEngine.Server
            .processCall(server, connectionProtocolName, request, methodName,
                protocolImpl);
      }

      private RpcWritable call(RPC.Server server,
          String connectionProtocolName, RpcWritable.Buffer request,
          String methodName, ProtoClassProtoImpl protocolImpl)
          throws Exception {
        BlockingService service = (BlockingService) protocolImpl.protocolImpl;
        MethodDescriptor methodDescriptor = service.getDescriptorForType()
            .findMethodByName(methodName);
        if (methodDescriptor == null) {
          String msg = "Unknown method " + methodName + " called on "
                                + connectionProtocolName + " protocol.";
          LOG.warn(msg);
          throw new RpcNoSuchMethodException(msg);
        }
        Message prototype = service.getRequestPrototype(methodDescriptor);
        Message param = request.getValue(prototype);

        Message result;
        Call currentCall = Server.getCurCall().get();
        try {
          server.rpcDetailedMetrics.init(protocolImpl.protocolClass);
          CURRENT_CALL_INFO.set(new CallInfo(server, methodName));
          currentCall.setDetailedMetricsName(methodName);
          result = service.callBlockingMethod(methodDescriptor, null, param);
          // Check if this needs to be a deferred response,
          // by checking the ThreadLocal callback being set
          if (CURRENT_CALLBACK.get() != null) {
            currentCall.deferResponse();
            CURRENT_CALLBACK.set(null);
            return null;
          }
        } catch (ServiceException e) {
          Exception exception = (Exception) e.getCause();
          currentCall.setDetailedMetricsName(
              exception.getClass().getSimpleName());
          throw (Exception) e.getCause();
        } catch (Exception e) {
          currentCall.setDetailedMetricsName(e.getClass().getSimpleName());
          throw e;
        } finally {
          CURRENT_CALL_INFO.set(null);
        }
        return RpcWritable.wrap(result);
      }
    }
  }

  // htrace in the ipc layer creates the span name based on toString()
  // which uses the rpc header.  in the normal case we want to defer decoding
  // the rpc header until needed by the rpc engine.
  static class RpcProtobufRequest extends RpcWritable.Buffer {
    private volatile RequestHeaderProto requestHeader;
    private Message payload;

    RpcProtobufRequest() {
    }

    RpcProtobufRequest(RequestHeaderProto header, Message payload) {
      this.requestHeader = header;
      this.payload = payload;
    }

    RequestHeaderProto getRequestHeader() throws IOException {
      if (getByteBuffer() != null && requestHeader == null) {
        requestHeader = getValue(RequestHeaderProto.getDefaultInstance());
      }
      return requestHeader;
    }

    @Override
    public void writeTo(ResponseBuffer out) throws IOException {
      requestHeader.writeDelimitedTo(out);
      if (payload != null) {
        payload.writeDelimitedTo(out);
      }
    }

    // this is used by htrace to name the span.
    @Override
    public String toString() {
      try {
        RequestHeaderProto header = getRequestHeader();
        return header.getDeclaringClassProtocolName() + "." +
               header.getMethodName();
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }
}
