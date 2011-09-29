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

package org.apache.hadoop.yarn.ipc;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcEngine;
import org.apache.hadoop.ipc.ClientCache;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.exceptions.impl.pb.YarnRemoteExceptionPBImpl;
import org.apache.hadoop.yarn.ipc.RpcProtos.ProtoSpecificRpcRequest;
import org.apache.hadoop.yarn.ipc.RpcProtos.ProtoSpecificRpcResponse;

import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;



@InterfaceStability.Evolving
public class ProtoOverHadoopRpcEngine implements RpcEngine {
  private static final Log LOG = LogFactory.getLog(RPC.class);
  
  private static final ClientCache CLIENTS=new ClientCache();
  
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
          ProtoSpecificResponseWritable.class);
    }

    private ProtoSpecificRpcRequest constructRpcRequest(Method method,
        Object[] params) throws ServiceException {
      ProtoSpecificRpcRequest rpcRequest;
      ProtoSpecificRpcRequest.Builder builder;

      builder = ProtoSpecificRpcRequest.newBuilder();
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
      builder.setRequestProto(param.toByteString());

      rpcRequest = builder.build();
      return rpcRequest;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      long startTime = 0;
      if (LOG.isDebugEnabled()) {
        startTime = System.currentTimeMillis();
      }

      ProtoSpecificRpcRequest rpcRequest = constructRpcRequest(method, args);
      ProtoSpecificResponseWritable val = null;
      try {
        val = (ProtoSpecificResponseWritable) client.call(
            new ProtoSpecificRequestWritable(rpcRequest), remoteId);
      } catch (Exception e) {
        throw new ServiceException(e);
      }
      
      ProtoSpecificRpcResponse response = val.message;
   
      if (LOG.isDebugEnabled()) {
        long callTime = System.currentTimeMillis() - startTime;
        LOG.debug("Call: " + method.getName() + " " + callTime);
      }
 
      if (response.hasIsError() && response.getIsError() == true) {
        YarnRemoteExceptionPBImpl exception = new YarnRemoteExceptionPBImpl(response.getException());
        exception.fillInStackTrace();
        ServiceException se = new ServiceException(exception);
        throw se;
      }
      
      Message prototype = null;
      try {
        prototype = getReturnProtoType(method);
      } catch (Exception e) {
        throw new ServiceException(e);
      }
      Message actualReturnMessage = prototype.newBuilderForType()
          .mergeFrom(response.getResponseProto()).build();
      return actualReturnMessage;
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
      } else {
        Class<?> returnType = method.getReturnType();

        Method newInstMethod = returnType.getMethod("getDefaultInstance");
        newInstMethod.setAccessible(true);
        Message prototype = (Message) newInstMethod.invoke(null,
            (Object[]) null);
        returnTypes.put(method.getName(), prototype);
        return prototype;
      }
    }
  }
  
  /**
   * Writable Wrapper for Protocol Buffer Requests
   */
  private static class ProtoSpecificRequestWritable implements Writable {
    ProtoSpecificRpcRequest message;

    @SuppressWarnings("unused")
    public ProtoSpecificRequestWritable() {
    }
    
    ProtoSpecificRequestWritable(ProtoSpecificRpcRequest message) {
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
      message = ProtoSpecificRpcRequest.parseFrom(bytes);
    }
  }
  
  /**
   * Writable Wrapper for Protocol Buffer Responses
   */
  public static class ProtoSpecificResponseWritable implements Writable {
    ProtoSpecificRpcResponse message;

    public ProtoSpecificResponseWritable() {
    }
    
    public ProtoSpecificResponseWritable(ProtoSpecificRpcResponse message) {
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
      message = ProtoSpecificRpcResponse.parseFrom(bytes);
    }
  }
  
  @Override
  public Object[] call(Method method, Object[][] params,
      InetSocketAddress[] addrs, UserGroupInformation ticket, Configuration conf)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  // for unit testing only
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  static Client getClient(Configuration conf) {
    return CLIENTS.getClient(conf, SocketFactory.getDefault(),
        ProtoSpecificResponseWritable.class);
  }

  public static class Server extends RPC.Server {

    private BlockingService service;
    private boolean verbose;
//
//    /**
//     * Construct an RPC server.
//     * 
//     * @param instance
//     *          the instance whose methods will be called
//     * @param conf
//     *          the configuration to use
//     * @param bindAddress
//     *          the address to bind on to listen for connection
//     * @param port
//     *          the port to listen for connections on
//     */
//    public Server(Object instance, Configuration conf, String bindAddress,
//        int port) throws IOException {
//      this(instance, conf, bindAddress, port, 1, false, null);
//    }

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
     * @param instance
     *          the instance whose methods will be called
     * @param conf
     *          the configuration to use
     * @param bindAddress
     *          the address to bind on to listen for connection
     * @param port
     *          the port to listen for connections on
     * @param numHandlers
     *          the number of method handler threads to run
     * @param verbose
     *          whether each call should be logged
     */
    public Server(Object instance, Configuration conf, String bindAddress,
        int port, int numHandlers, int numReaders, 
        int queueSizePerHandler, boolean verbose,
        SecretManager<? extends TokenIdentifier> secretManager)
        throws IOException {
      super(bindAddress, port, ProtoSpecificRequestWritable.class, numHandlers,
          numReaders, queueSizePerHandler, conf, classNameBase(instance.getClass().getName()), secretManager);
      this.service = (BlockingService) instance;
      this.verbose = verbose;
    }

    @Override
    public Writable call(String protocol, Writable writableRequest,
        long receiveTime) throws IOException {
      ProtoSpecificRequestWritable request = (ProtoSpecificRequestWritable) writableRequest;
      ProtoSpecificRpcRequest rpcRequest = request.message;
      String methodName = rpcRequest.getMethodName();
      System.out.println("Call: protocol=" + protocol + ", method="
          + methodName);
      if (verbose)
        log("Call: protocol=" + protocol + ", method="
            + methodName);
      MethodDescriptor methodDescriptor = service.getDescriptorForType()
          .findMethodByName(methodName);
      if (methodDescriptor == null) {
        String msg = "Unknown method " + methodName + " called on "
            + protocol + " protocol.";
        LOG.warn(msg);
        return handleException(new IOException(msg));
      }
      Message prototype = service.getRequestPrototype(methodDescriptor);
      Message param = prototype.newBuilderForType()
          .mergeFrom(rpcRequest.getRequestProto()).build();
      Message result;
      try {
        result = service.callBlockingMethod(methodDescriptor, null, param);
      } catch (ServiceException e) {
        e.printStackTrace();
        return handleException(e);
      } catch (Exception e) {
        return handleException(e);
      }

      ProtoSpecificRpcResponse response = constructProtoSpecificRpcSuccessResponse(result);
      return new ProtoSpecificResponseWritable(response);
    }

    private ProtoSpecificResponseWritable handleException(Throwable e) {
      ProtoSpecificRpcResponse.Builder builder = ProtoSpecificRpcResponse
          .newBuilder();
      builder.setIsError(true);
      if (e.getCause() instanceof YarnRemoteExceptionPBImpl) {
        builder.setException(((YarnRemoteExceptionPBImpl) e.getCause())
            .getProto());
      } else {
        builder.setException(new YarnRemoteExceptionPBImpl(e).getProto());
      }
      ProtoSpecificRpcResponse response = builder.build();
      return new ProtoSpecificResponseWritable(response);
    }

    private ProtoSpecificRpcResponse constructProtoSpecificRpcSuccessResponse(
        Message message) {
      ProtoSpecificRpcResponse res = ProtoSpecificRpcResponse.newBuilder()
          .setResponseProto(message.toByteString()).build();
      return res;
    }
  }

  private static void log(String value) {
    if (value != null && value.length() > 55)
      value = value.substring(0, 55) + "...";
    LOG.info(value);
  }

  @Override
  public RPC.Server getServer(Class<?> protocol, Object instance,
      String bindAddress, int port, int numHandlers,int numReaders, 
      int queueSizePerHandler, boolean verbose,
      Configuration conf, SecretManager<? extends TokenIdentifier> secretManager)
      throws IOException {
    return new Server(instance, conf, bindAddress, port, numHandlers, numReaders, queueSizePerHandler,
        verbose, secretManager);
  }
}
