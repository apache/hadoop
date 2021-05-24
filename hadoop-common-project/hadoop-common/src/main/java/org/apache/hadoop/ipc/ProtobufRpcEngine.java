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

import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.util.concurrent.AsyncGet;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.AsyncGet;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;
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
 * This engine uses Protobuf 2.5.0. Recommended to upgrade to Protobuf 3.x
 * from hadoop-thirdparty and use ProtobufRpcEngine2.
 */
@Deprecated
@InterfaceStability.Evolving
public class ProtobufRpcEngine implements RpcEngine {
  public static final Logger LOG =
      LoggerFactory.getLogger(ProtobufRpcEngine.class);
  private static final ThreadLocal<AsyncGet<Message, Exception>>
      ASYNC_RETURN_MESSAGE = new ThreadLocal<>();

  static { // Register the rpcRequest deserializer for ProtobufRpcEngine
    //These will be used in server side, which is always ProtobufRpcEngine2
    ProtobufRpcEngine2.registerProtocolEngine();
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
  public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory, int rpcTimeout, RetryPolicy connectionRetryPolicy
      ) throws IOException {
    return getProxy(protocol, clientVersion, addr, ticket, conf, factory,
      rpcTimeout, connectionRetryPolicy, null, null);
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
            new Class[] { protocol }, new Invoker(protocol, connId, conf,
                factory)), false);
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
          conf, factory);
      this.fallbackToSimpleAuth = fallbackToSimpleAuth;
      this.alignmentContext = alignmentContext;
    }
    
    /**
     * This constructor takes a connectionId, instead of creating a new one.
     */
    protected Invoker(Class<?> protocol, Client.ConnectionId connId,
        Configuration conf, SocketFactory factory) {
      this.remoteId = connId;
      this.client = CLIENTS.getClient(conf, factory, RpcWritable.Buffer.class);
      this.protocolName = RPC.getProtocolName(protocol);
      this.clientProtocolVersion = RPC
          .getProtocolVersion(protocol);
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
        startTime = Time.now();
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
        if (traceScope != null) traceScope.close();
      }

      if (LOG.isDebugEnabled()) {
        long callTime = Time.now() - startTime;
        LOG.debug("Call: " + method.getName() + " took " + callTime + "ms");
      }
      
      if (Client.isAsynchronousMode()) {
        final AsyncGet<RpcWritable.Buffer, IOException> arr
            = Client.getAsyncRpcResponse();
        final AsyncGet<Message, Exception> asyncGet
            = new AsyncGet<Message, Exception>() {
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

  /**
   * Server implementation is always ProtobufRpcEngine2 based implementation,
   * supports backward compatibility for protobuf 2.5 based implementations,
   * which uses non-shaded protobuf classes.
   */
  public static class Server extends ProtobufRpcEngine2.Server {

    static final ThreadLocal<ProtobufRpcEngineCallback> currentCallback =
        new ThreadLocal<>();

    static class ProtobufRpcEngineCallbackImpl
        implements ProtobufRpcEngineCallback {

      private final RPC.Server server;
      private final Call call;
      private final String methodName;
      private final long setupTime;

      public ProtobufRpcEngineCallbackImpl() {
        this.server = CURRENT_CALL_INFO.get().getServer();
        this.call = Server.getCurCall().get();
        this.methodName = CURRENT_CALL_INFO.get().getMethodName();
        this.setupTime = Time.now();
      }

      @Override
      public void setResponse(Message message) {
        long processingTime = Time.now() - setupTime;
        call.setDeferredResponse(RpcWritable.wrap(message));
        server.updateDeferredMetrics(methodName, processingTime);
      }

      @Override
      public void error(Throwable t) {
        long processingTime = Time.now() - setupTime;
        String detailedMetricsName = t.getClass().getSimpleName();
        server.updateDeferredMetrics(detailedMetricsName, processingTime);
        call.setDeferredError(t);
      }
    }

    @InterfaceStability.Unstable
    public static ProtobufRpcEngineCallback registerForDeferredResponse() {
      ProtobufRpcEngineCallback callback = new ProtobufRpcEngineCallbackImpl();
      currentCallback.set(callback);
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
     * @param verbose whether each call should be logged
     * @param portRangeConfig A config parameter that can be used to restrict
     * the range of ports used when port is 0 (an ephemeral port)
     * @param alignmentContext provides server state info on client responses
     */
    public Server(Class<?> protocolClass, Object protocolImpl,
        Configuration conf, String bindAddress, int port, int numHandlers,
        int numReaders, int queueSizePerHandler, boolean verbose,
        SecretManager<? extends TokenIdentifier> secretManager, 
        String portRangeConfig, AlignmentContext alignmentContext)
        throws IOException {
      super(protocolClass, protocolImpl, conf, bindAddress, port, numHandlers,
          numReaders, queueSizePerHandler, verbose, secretManager,
          portRangeConfig, alignmentContext);
    }

    /**
     * This implementation is same as
     * ProtobufRpcEngine2.Server.ProtobufInvoker#call(..)
     * except this implementation uses non-shaded protobuf classes from legacy
     * protobuf version (default 2.5.0).
     */
    static RpcWritable processCall(RPC.Server server,
        String connectionProtocolName, RpcWritable.Buffer request,
        String methodName, ProtoClassProtoImpl protocolImpl) throws Exception {
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
        if (currentCallback.get() != null) {
          currentCall.deferResponse();
          currentCallback.set(null);
          return null;
        }
      } catch (ServiceException e) {
        Exception exception = (Exception) e.getCause();
        currentCall
            .setDetailedMetricsName(exception.getClass().getSimpleName());
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

  // htrace in the ipc layer creates the span name based on toString()
  // which uses the rpc header.  in the normal case we want to defer decoding
  // the rpc header until needed by the rpc engine.
  static class RpcProtobufRequest extends RpcWritable.Buffer {
    private volatile RequestHeaderProto requestHeader;
    private Message payload;

    public RpcProtobufRequest() {
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
