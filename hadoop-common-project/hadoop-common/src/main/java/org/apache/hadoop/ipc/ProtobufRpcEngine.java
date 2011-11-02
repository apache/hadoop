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

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.protobuf.HadoopRpcProtos.HadoopRpcExceptionProto;
import org.apache.hadoop.ipc.protobuf.HadoopRpcProtos.HadoopRpcRequestProto;
import org.apache.hadoop.ipc.protobuf.HadoopRpcProtos.HadoopRpcResponseProto;
import org.apache.hadoop.ipc.protobuf.HadoopRpcProtos.HadoopRpcResponseProto.ResponseStatus;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;

/**
 * RPC Engine for for protobuf based RPCs.
 */
@InterfaceStability.Evolving
public class ProtobufRpcEngine implements RpcEngine {
  private static final Log LOG = LogFactory.getLog(ProtobufRpcEngine.class);

  private static final ClientCache CLIENTS = new ClientCache();

  @Override
  @SuppressWarnings("unchecked")
  public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory, int rpcTimeout) throws IOException {

    return new ProtocolProxy<T>(protocol, (T) Proxy.newProxyInstance(protocol
        .getClassLoader(), new Class[] { protocol }, new Invoker(protocol,
        addr, ticket, conf, factory, rpcTimeout)), false);
  }

  private static class Invoker implements InvocationHandler, Closeable {
    private Map<String, Message> returnTypes = new ConcurrentHashMap<String, Message>();
    private boolean isClosed = false;
    private Client.ConnectionId remoteId;
    private Client client;

    public Invoker(Class<?> protocol, InetSocketAddress addr,
        UserGroupInformation ticket, Configuration conf, SocketFactory factory,
        int rpcTimeout) throws IOException {
      this.remoteId = Client.ConnectionId.getConnectionId(addr, protocol,
          ticket, rpcTimeout, conf);
      this.client = CLIENTS.getClient(conf, factory,
          RpcResponseWritable.class);
    }

    private HadoopRpcRequestProto constructRpcRequest(Method method,
        Object[] params) throws ServiceException {
      HadoopRpcRequestProto rpcRequest;
      HadoopRpcRequestProto.Builder builder = HadoopRpcRequestProto
          .newBuilder();
      builder.setMethodName(method.getName());

      if (params.length != 2) { // RpcController + Message
        throw new ServiceException("Too many parameters for request. Method: ["
            + method.getName() + "]" + ", Expected: 2, Actual: "
            + params.length);
      }
      if (params[1] == null) {
        throw new ServiceException("null param while calling Method: ["
            + method.getName() + "]");
      }

      Message param = (Message) params[1];
      builder.setRequest(param.toByteString());
      rpcRequest = builder.build();
      return rpcRequest;
    }

    /**
     * This is the client side invoker of RPC method. It only throws
     * ServiceException, since the invocation proxy expects only
     * ServiceException to be thrown by the method in case protobuf service.
     * 
     * ServiceException has the following causes:
     * <ol>
     * <li>Exceptions encountered in this methods are thrown as
     * RpcClientException, wrapped in RemoteException</li>
     * <li>Remote exceptions are thrown wrapped in RemoteException</li>
     * </ol>
     * 
     * Note that the client calling protobuf RPC methods, must handle
     * ServiceException by getting the cause from the ServiceException. If the
     * cause is RemoteException, then unwrap it to get the exception thrown by
     * the server.
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws ServiceException {
      long startTime = 0;
      if (LOG.isDebugEnabled()) {
        startTime = System.currentTimeMillis();
      }

      HadoopRpcRequestProto rpcRequest = constructRpcRequest(method, args);
      RpcResponseWritable val = null;
      try {
        val = (RpcResponseWritable) client.call(
            new RpcRequestWritable(rpcRequest), remoteId);
      } catch (Exception e) {
        RpcClientException ce = new RpcClientException("Client exception", e);
        throw new ServiceException(getRemoteException(ce));
      }

      HadoopRpcResponseProto response = val.message;
      if (LOG.isDebugEnabled()) {
        long callTime = System.currentTimeMillis() - startTime;
        LOG.debug("Call: " + method.getName() + " " + callTime);
      }

      // Wrap the received message
      ResponseStatus status = response.getStatus();
      if (status != ResponseStatus.SUCCESS) {
        RemoteException re =  new RemoteException(response.getException()
            .getExceptionName(), response.getException().getStackTrace());
        re.fillInStackTrace();
        throw new ServiceException(re);
      }

      Message prototype = null;
      try {
        prototype = getReturnProtoType(method);
      } catch (Exception e) {
        throw new ServiceException(e);
      }
      Message returnMessage;
      try {
        returnMessage = prototype.newBuilderForType()
            .mergeFrom(response.getResponse()).build();
      } catch (InvalidProtocolBufferException e) {
        RpcClientException ce = new RpcClientException("Client exception", e);
        throw new ServiceException(getRemoteException(ce));
      }
      return returnMessage;
    }

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
  }

  @Override
  public Object[] call(Method method, Object[][] params,
      InetSocketAddress[] addrs, UserGroupInformation ticket, Configuration conf) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writable Wrapper for Protocol Buffer Requests
   */
  private static class RpcRequestWritable implements Writable {
    HadoopRpcRequestProto message;

    @SuppressWarnings("unused")
    public RpcRequestWritable() {
    }

    RpcRequestWritable(HadoopRpcRequestProto message) {
      this.message = message;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(message.toByteArray().length);
      out.write(message.toByteArray());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int length = in.readInt();
      byte[] bytes = new byte[length];
      in.readFully(bytes);
      message = HadoopRpcRequestProto.parseFrom(bytes);
    }
  }

  /**
   * Writable Wrapper for Protocol Buffer Responses
   */
  private static class RpcResponseWritable implements Writable {
    HadoopRpcResponseProto message;

    @SuppressWarnings("unused")
    public RpcResponseWritable() {
    }

    public RpcResponseWritable(HadoopRpcResponseProto message) {
      this.message = message;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(message.toByteArray().length);
      out.write(message.toByteArray());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int length = in.readInt();
      byte[] bytes = new byte[length];
      in.readFully(bytes);
      message = HadoopRpcResponseProto.parseFrom(bytes);
    }
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  static Client getClient(Configuration conf) {
    return CLIENTS.getClient(conf, SocketFactory.getDefault(),
        RpcResponseWritable.class);
  }
  

  @Override
  public RPC.Server getServer(Class<?> protocol, Object instance,
      String bindAddress, int port, int numHandlers, int numReaders,
      int queueSizePerHandler, boolean verbose, Configuration conf,
      SecretManager<? extends TokenIdentifier> secretManager)
      throws IOException {
    return new Server(instance, conf, bindAddress, port, numHandlers,
        numReaders, queueSizePerHandler, verbose, secretManager);
  }
  
  private static RemoteException getRemoteException(Exception e) {
    return new RemoteException(e.getClass().getName(),
        StringUtils.stringifyException(e));
  }

  public static class Server extends RPC.Server {
    private BlockingService service;
    private boolean verbose;

    private static String classNameBase(String className) {
      String[] names = className.split("\\.", -1);
      if (names == null || names.length == 0) {
        return className;
      }
      return names[names.length - 1];
    }

    /**
     * Construct an RPC server.
     * 
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     */
    public Server(Object instance, Configuration conf, String bindAddress,
        int port, int numHandlers, int numReaders, int queueSizePerHandler,
        boolean verbose, SecretManager<? extends TokenIdentifier> secretManager)
        throws IOException {
      super(bindAddress, port, RpcRequestWritable.class, numHandlers,
          numReaders, queueSizePerHandler, conf, classNameBase(instance
              .getClass().getName()), secretManager);
      this.service = (BlockingService) instance;
      this.verbose = verbose;
    }

    /**
     * This is a server side method, which is invoked over RPC. On success
     * the return response has protobuf response payload. On failure, the
     * exception name and the stack trace are return in the resposne. See {@link HadoopRpcResponseProto}
     * 
     * In this method there three types of exceptions possible and they are
     * returned in response as follows.
     * <ol>
     * <li> Exceptions encountered in this method that are returned as {@link RpcServerException} </li>
     * <li> Exceptions thrown by the service is wrapped in ServiceException. In that
     * this method returns in response the exception thrown by the service.</li>
     * <li> Other exceptions thrown by the service. They are returned as
     * it is.</li>
     * </ol>
     */
    @Override
    public Writable call(String protocol, Writable writableRequest,
        long receiveTime) throws IOException {
      RpcRequestWritable request = (RpcRequestWritable) writableRequest;
      HadoopRpcRequestProto rpcRequest = request.message;
      String methodName = rpcRequest.getMethodName();
      if (verbose)
        LOG.info("Call: protocol=" + protocol + ", method=" + methodName);
      MethodDescriptor methodDescriptor = service.getDescriptorForType()
          .findMethodByName(methodName);
      if (methodDescriptor == null) {
        String msg = "Unknown method " + methodName + " called on " + protocol
            + " protocol.";
        LOG.warn(msg);
        return handleException(new RpcServerException(msg));
      }
      Message prototype = service.getRequestPrototype(methodDescriptor);
      Message param = prototype.newBuilderForType()
          .mergeFrom(rpcRequest.getRequest()).build();
      Message result;
      try {
        result = service.callBlockingMethod(methodDescriptor, null, param);
      } catch (ServiceException e) {
        Throwable cause = e.getCause();
        return handleException(cause != null ? cause : e);
      } catch (Exception e) {
        return handleException(e);
      }

      HadoopRpcResponseProto response = constructProtoSpecificRpcSuccessResponse(result);
      return new RpcResponseWritable(response);
    }

    private RpcResponseWritable handleException(Throwable e) {
      HadoopRpcExceptionProto exception = HadoopRpcExceptionProto.newBuilder()
          .setExceptionName(e.getClass().getName())
          .setStackTrace(StringUtils.stringifyException(e)).build();
      HadoopRpcResponseProto response = HadoopRpcResponseProto.newBuilder()
          .setStatus(ResponseStatus.ERRROR).setException(exception).build();
      return new RpcResponseWritable(response);
    }

    private HadoopRpcResponseProto constructProtoSpecificRpcSuccessResponse(
        Message message) {
      HadoopRpcResponseProto res = HadoopRpcResponseProto.newBuilder()
          .setResponse(message.toByteString())
          .setStatus(ResponseStatus.SUCCESS)
          .build();
      return res;
    }
  }
}
