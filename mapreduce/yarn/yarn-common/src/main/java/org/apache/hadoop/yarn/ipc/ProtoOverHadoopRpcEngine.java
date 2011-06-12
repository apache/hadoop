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
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcEngine;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.ipc.WritableRpcEngine;
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


@InterfaceStability.Evolving
public class ProtoOverHadoopRpcEngine implements RpcEngine {
  private static final Log LOG = LogFactory.getLog(RPC.class);
  
  private static final RpcEngine ENGINE = new WritableRpcEngine();

  /** Tunnel a Proto RPC request and response through Hadoop's RPC. */
  private static interface TunnelProtocol extends VersionedProtocol {
    /** WritableRpcEngine requires a versionID */
    public static final long versionID = 1L;

    /** All Proto methods and responses go through this. */
    ProtoSpecificResponseWritable call(ProtoSpecificRequestWritable request) throws IOException;
  }


  @Override
  @SuppressWarnings("unchecked")
  public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory, int rpcTimeout) throws IOException {

    return new ProtocolProxy<T>(protocol, (T) Proxy.newProxyInstance(protocol
        .getClassLoader(), new Class[] { protocol }, new Invoker(protocol,
        addr, ticket, conf, factory, rpcTimeout)), false);
  }

  @Override
  public void stopProxy(Object proxy) {
    try {
      ((Invoker) Proxy.getInvocationHandler(proxy)).close();
    } catch (IOException e) {
      LOG.warn("Error while stopping " + proxy, e);
    }
  }

  private class Invoker implements InvocationHandler, Closeable {
    private TunnelProtocol tunnel;
    private Map<String, Message> returnTypes = new ConcurrentHashMap<String, Message>();

    public Invoker(Class<?> protocol, InetSocketAddress addr,
        UserGroupInformation ticket, Configuration conf, SocketFactory factory,
        int rpcTimeout) throws IOException {
      this.tunnel = ENGINE.getProxy(TunnelProtocol.class,
          TunnelProtocol.versionID, addr, ticket, conf, factory, rpcTimeout)
          .getProxy();
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      ProtoSpecificRpcRequest rpcRequest = constructRpcRequest(method, args);
      ProtoSpecificResponseWritable val = null;
      try {
        val = tunnel.call(new ProtoSpecificRequestWritable(rpcRequest));
      } catch (Exception e) {
        throw new ServiceException(e);
      }
      
      ProtoSpecificRpcResponse response = val.message;

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
//        YarnRemoteExceptionPBImpl exception = new YarnRemoteExceptionPBImpl("Could not get prototype PB return type for method: [" + method.getName() + "]", e);
      }
      Message actualReturnMessage = prototype.newBuilderForType().mergeFrom(response.getResponseProto()).build();
      return actualReturnMessage;
    }

    public void close() throws IOException {
      ENGINE.stopProxy(tunnel);
    }
    
    private Message getReturnProtoType(Method method) throws Exception {
      if (returnTypes.containsKey(method.getName())) {
        return returnTypes.get(method.getName());
      } else {
        Class<?> returnType = method.getReturnType();

        Method newInstMethod = returnType.getMethod("getDefaultInstance", null);
        newInstMethod.setAccessible(true);
        Message prototype = (Message) newInstMethod.invoke(null, null);
        returnTypes.put(method.getName(), prototype);
        return prototype;
      }
    }
  }

  private class TunnelResponder implements TunnelProtocol {
    BlockingService service;
    
    public TunnelResponder(Class<?> iface, Object impl) {
      this.service = (BlockingService)impl;
    }

    public long getProtocolVersion(String protocol, long version)
        throws IOException {
      return TunnelProtocol.versionID;
    }
    
    @Override
    public ProtocolSignature getProtocolSignature(
        String protocol, long version, int clientMethodsHashCode)
      throws IOException {
      return new ProtocolSignature(TunnelProtocol.versionID, null);
    }

    public ProtoSpecificResponseWritable call(final ProtoSpecificRequestWritable request)
        throws IOException {
      ProtoSpecificRpcRequest rpcRequest = request.message;
      String methodName = rpcRequest.getMethodName();
      MethodDescriptor methodDescriptor = service.getDescriptorForType().findMethodByName(methodName);
      
      Message prototype = service.getRequestPrototype(methodDescriptor);
      Message param = prototype.newBuilderForType().mergeFrom(rpcRequest.getRequestProto()).build();
      
      Message result;
      try {
        result = service.callBlockingMethod(methodDescriptor, null, param);
      } catch (ServiceException e) {
        return handleException(e);
      } catch (Exception e) {
        return handleException(e);
      }
      
      ProtoSpecificRpcResponse response = constructProtoSpecificRpcSuccessResponse(result);
      return new ProtoSpecificResponseWritable(response);
    }
    
    private ProtoSpecificResponseWritable handleException (Throwable e) {
      ProtoSpecificRpcResponse.Builder builder = ProtoSpecificRpcResponse.newBuilder();
      builder.setIsError(true);
      if (e.getCause() instanceof YarnRemoteExceptionPBImpl) {
        builder.setException(((YarnRemoteExceptionPBImpl)e.getCause()).getProto());
      } else {
        builder.setException(new YarnRemoteExceptionPBImpl(e).getProto());
      }
      ProtoSpecificRpcResponse response = builder.build();
      return new ProtoSpecificResponseWritable(response);
    }
  }

  @Override
  public Object[] call(Method method, Object[][] params,
      InetSocketAddress[] addrs, UserGroupInformation ticket, Configuration conf)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  
  @Override
  public RPC.Server getServer(Class<?> protocol, Object instance,
      String bindAddress, int port, int numHandlers, boolean verbose,
      Configuration conf, SecretManager<? extends TokenIdentifier> secretManager)
      throws IOException {

    return ENGINE
        .getServer(TunnelProtocol.class,
            new TunnelResponder(protocol, instance), bindAddress, port,
            numHandlers, verbose, conf, secretManager);
  }

  
  private Class<?>[] getRequestParameterTypes(Message[] messages) {
    Class<?> [] paramTypes = new Class<?>[messages.length];
    for (int i = 0 ; i < messages.length ; i++) {
      paramTypes[i] = messages[i].getClass();
    }
    return paramTypes;
  }

  private ProtoSpecificRpcRequest constructRpcRequest(Method method,
      Object[] params) throws ServiceException {
    ProtoSpecificRpcRequest rpcRequest;
    ProtoSpecificRpcRequest.Builder builder;

    builder = ProtoSpecificRpcRequest.newBuilder();
    builder.setMethodName(method.getName());

    if (params.length != 2) { //RpcController + Message
      throw new ServiceException("Too many parameters for request. Method: [" + method.getName() + "]" + ", Expected: 2, Actual: " + params.length);
    }
    if (params[1] == null) {
      throw new ServiceException("null param while calling Method: [" + method.getName() +"]");
    }

    Message param = (Message) params[1];
    builder.setRequestProto(param.toByteString());

    rpcRequest = builder.build();
    return rpcRequest;
  }

  private ProtoSpecificRpcResponse constructProtoSpecificRpcSuccessResponse(Message message) {
    ProtoSpecificRpcResponse res = ProtoSpecificRpcResponse.newBuilder().setResponseProto(message.toByteString()).build();
    return res;
  }

  
  /**
   * Writable Wrapper for Protocol Buffer Responses
   */
  private static class ProtoSpecificResponseWritable implements Writable {
    ProtoSpecificRpcResponse message;

    public ProtoSpecificResponseWritable() {
    }
    
    ProtoSpecificResponseWritable(ProtoSpecificRpcResponse message) {
      this.message = message;
    }

    @Override
    public void write(DataOutput out) throws IOException {
//      System.err.println("XXX: writing length: " + message.toByteArray().length);
      out.writeInt(message.toByteArray().length);
      out.write(message.toByteArray());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int length = in.readInt();
//      System.err.println("YYY: Reading length: " + length);
      byte[] bytes = new byte[length];
      in.readFully(bytes);
      message = ProtoSpecificRpcResponse.parseFrom(bytes);
    }
  }
  
  /**
   * Writable Wrapper for Protocol Buffer Requests
   */
  private static class ProtoSpecificRequestWritable implements Writable {
    ProtoSpecificRpcRequest message;

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
}
