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

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.lang.reflect.Method;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

import java.net.InetSocketAddress;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.Closeable;
import java.util.Map;
import java.util.HashMap;

import javax.net.SocketFactory;

import org.apache.commons.logging.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.RpcPayloadHeader.RpcKind;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.*;

/** An RpcEngine implementation for Writable data. */
@InterfaceStability.Evolving
public class WritableRpcEngine implements RpcEngine {
  private static final Log LOG = LogFactory.getLog(RPC.class);
  
 
  /**
   * Get all superInterfaces that extend VersionedProtocol
   * @param childInterfaces
   * @return the super interfaces that extend VersionedProtocol
   */
  private static Class<?>[] getSuperInterfaces(Class<?>[] childInterfaces) {
    List<Class<?>> allInterfaces = new ArrayList<Class<?>>();

    for (Class<?> childInterface : childInterfaces) {
      if (VersionedProtocol.class.isAssignableFrom(childInterface)) {
          allInterfaces.add(childInterface);
          allInterfaces.addAll(
              Arrays.asList(
                  getSuperInterfaces(childInterface.getInterfaces())));
      } else {
        LOG.warn("Interface " + childInterface +
              " ignored because it does not extend VersionedProtocol");
      }
    }
    return (Class<?>[]) allInterfaces.toArray(new Class[allInterfaces.size()]);
  }
  
  /**
   * Get all interfaces that the given protocol implements or extends
   * which are assignable from VersionedProtocol.
   */
  private static Class<?>[] getProtocolInterfaces(Class<?> protocol) {
    Class<?>[] interfaces  = protocol.getInterfaces();
    return getSuperInterfaces(interfaces);
  }

  
  //writableRpcVersion should be updated if there is a change
  //in format of the rpc messages.
  
  // 2L - added declared class to Invocation
  public static final long writableRpcVersion = 2L; 

  
  /** A method invocation, including the method name and its parameters.*/
  private static class Invocation implements Writable, Configurable {
    private String methodName;
    private Class<?>[] parameterClasses;
    private Object[] parameters;
    private Configuration conf;
    private long clientVersion;
    private int clientMethodsHash;
    private String declaringClassProtocolName;
    
    //This could be different from static writableRpcVersion when received
    //at server, if client is using a different version.
    private long rpcVersion;

    @SuppressWarnings("unused") // called when deserializing an invocation
    public Invocation() {}

    public Invocation(Method method, Object[] parameters) {
      this.methodName = method.getName();
      this.parameterClasses = method.getParameterTypes();
      this.parameters = parameters;
      rpcVersion = writableRpcVersion;
      if (method.getDeclaringClass().equals(VersionedProtocol.class)) {
        //VersionedProtocol is exempted from version check.
        clientVersion = 0;
        clientMethodsHash = 0;
      } else {
        try {
          Field versionField = method.getDeclaringClass().getField("versionID");
          versionField.setAccessible(true);
          this.clientVersion = versionField.getLong(method.getDeclaringClass());
        } catch (NoSuchFieldException ex) {
          throw new RuntimeException(ex);
        } catch (IllegalAccessException ex) {
          throw new RuntimeException(ex);
        }
        this.clientMethodsHash = ProtocolSignature.getFingerprint(method
            .getDeclaringClass().getMethods());
      }
      this.declaringClassProtocolName = 
          RPC.getProtocolName(method.getDeclaringClass());
    }

    /** The name of the method invoked. */
    public String getMethodName() { return methodName; }

    /** The parameter classes. */
    public Class<?>[] getParameterClasses() { return parameterClasses; }

    /** The parameter instances. */
    public Object[] getParameters() { return parameters; }
    
    private long getProtocolVersion() {
      return clientVersion;
    }

    @SuppressWarnings("unused")
    private int getClientMethodsHash() {
      return clientMethodsHash;
    }
    
    /**
     * Returns the rpc version used by the client.
     * @return rpcVersion
     */
    public long getRpcVersion() {
      return rpcVersion;
    }

    @SuppressWarnings("deprecation")
    public void readFields(DataInput in) throws IOException {
      rpcVersion = in.readLong();
      declaringClassProtocolName = UTF8.readString(in);
      methodName = UTF8.readString(in);
      clientVersion = in.readLong();
      clientMethodsHash = in.readInt();
      parameters = new Object[in.readInt()];
      parameterClasses = new Class[parameters.length];
      ObjectWritable objectWritable = new ObjectWritable();
      for (int i = 0; i < parameters.length; i++) {
        parameters[i] = 
            ObjectWritable.readObject(in, objectWritable, this.conf);
        parameterClasses[i] = objectWritable.getDeclaredClass();
      }
    }

    @SuppressWarnings("deprecation")
    public void write(DataOutput out) throws IOException {
      out.writeLong(rpcVersion);
      UTF8.writeString(out, declaringClassProtocolName);
      UTF8.writeString(out, methodName);
      out.writeLong(clientVersion);
      out.writeInt(clientMethodsHash);
      out.writeInt(parameterClasses.length);
      for (int i = 0; i < parameterClasses.length; i++) {
        ObjectWritable.writeObject(out, parameters[i], parameterClasses[i],
                                   conf, true);
      }
    }

    public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append(methodName);
      buffer.append("(");
      for (int i = 0; i < parameters.length; i++) {
        if (i != 0)
          buffer.append(", ");
        buffer.append(parameters[i]);
      }
      buffer.append(")");
      buffer.append(", rpc version="+rpcVersion);
      buffer.append(", client version="+clientVersion);
      buffer.append(", methodsFingerPrint="+clientMethodsHash);
      return buffer.toString();
    }

    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    public Configuration getConf() {
      return this.conf;
    }

  }

  private static ClientCache CLIENTS=new ClientCache();
  
  private static class Invoker implements InvocationHandler, Closeable {
    private Client.ConnectionId remoteId;
    private Client client;
    private boolean isClosed = false;

    public Invoker(Class<?> protocol,
                   InetSocketAddress address, UserGroupInformation ticket,
                   Configuration conf, SocketFactory factory,
                   int rpcTimeout) throws IOException {
      this.remoteId = Client.ConnectionId.getConnectionId(address, protocol,
          ticket, rpcTimeout, conf);
      this.client = CLIENTS.getClient(conf, factory);
    }

    public Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
      long startTime = 0;
      if (LOG.isDebugEnabled()) {
        startTime = System.currentTimeMillis();
      }

      ObjectWritable value = (ObjectWritable)
        client.call(RpcKind.RPC_WRITABLE, new Invocation(method, args), remoteId);
      if (LOG.isDebugEnabled()) {
        long callTime = System.currentTimeMillis() - startTime;
        LOG.debug("Call: " + method.getName() + " " + callTime);
      }
      return value.get();
    }
    
    /* close the IPC client that's responsible for this invoker's RPCs */ 
    synchronized public void close() {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }
  }
  
  // for unit testing only
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  static Client getClient(Configuration conf) {
    return CLIENTS.getClient(conf);
  }
  
  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. 
   * @param <T>*/
  @Override
  @SuppressWarnings("unchecked")
  public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
                         InetSocketAddress addr, UserGroupInformation ticket,
                         Configuration conf, SocketFactory factory,
                         int rpcTimeout)
    throws IOException {    

    T proxy = (T) Proxy.newProxyInstance(protocol.getClassLoader(),
        new Class[] { protocol }, new Invoker(protocol, addr, ticket, conf,
            factory, rpcTimeout));
    return new ProtocolProxy<T>(protocol, proxy, true);
  }
  
  /** Expert: Make multiple, parallel calls to a set of servers. */
  public Object[] call(Method method, Object[][] params,
                       InetSocketAddress[] addrs, 
                       UserGroupInformation ticket, Configuration conf)
    throws IOException, InterruptedException {

    Invocation[] invocations = new Invocation[params.length];
    for (int i = 0; i < params.length; i++)
      invocations[i] = new Invocation(method, params[i]);
    Client client = CLIENTS.getClient(conf);
    try {
    Writable[] wrappedValues = 
      client.call(invocations, addrs, method.getDeclaringClass(), ticket, conf);
    
    if (method.getReturnType() == Void.TYPE) {
      return null;
    }

    Object[] values =
      (Object[])Array.newInstance(method.getReturnType(), wrappedValues.length);
    for (int i = 0; i < values.length; i++)
      if (wrappedValues[i] != null)
        values[i] = ((ObjectWritable)wrappedValues[i]).get();
    
    return values;
    } finally {
      CLIENTS.stopClient(client);
    }
  }

  /** Construct a server for a protocol implementation instance listening on a
   * port and address. */
  public RPC.Server getServer(Class<?> protocolClass,
                      Object protocolImpl, String bindAddress, int port,
                      int numHandlers, int numReaders, int queueSizePerHandler,
                      boolean verbose, Configuration conf,
                      SecretManager<? extends TokenIdentifier> secretManager) 
    throws IOException {
    return new Server(protocolClass, protocolImpl, conf, bindAddress, port,
        numHandlers, numReaders, queueSizePerHandler, verbose, secretManager);
  }


  /** An RPC Server. */
  public static class Server extends RPC.Server {
    private boolean verbose;
    
    /**
     *  The key in Map
     */
    static class ProtoNameVer {
      final String protocol;
      final long   version;
      ProtoNameVer(String protocol, long ver) {
        this.protocol = protocol;
        this.version = ver;
      }
      @Override
      public boolean equals(Object o) {
        if (o == null) 
          return false;
        if (this == o) 
          return true;
        if (! (o instanceof ProtoNameVer))
          return false;
        ProtoNameVer pv = (ProtoNameVer) o;
        return ((pv.protocol.equals(this.protocol)) && 
            (pv.version == this.version));     
      }
      @Override
      public int hashCode() {
        return protocol.hashCode() * 37 + (int) version;    
      }
    }
    
    /**
     * The value in map
     */
    static class ProtoClassProtoImpl {
      final Class<?> protocolClass;
      final Object protocolImpl; 
      ProtoClassProtoImpl(Class<?> protocolClass, Object protocolImpl) {
        this.protocolClass = protocolClass;
        this.protocolImpl = protocolImpl;
      }
    }
    
    private Map<ProtoNameVer, ProtoClassProtoImpl> protocolImplMap = 
        new HashMap<ProtoNameVer, ProtoClassProtoImpl>(10);
    
    // Register  protocol and its impl for rpc calls
    private void registerProtocolAndImpl(Class<?> protocolClass, 
        Object protocolImpl) throws IOException {
      String protocolName = RPC.getProtocolName(protocolClass);
      VersionedProtocol vp = (VersionedProtocol) protocolImpl;
      long version;
      try {
        version = vp.getProtocolVersion(protocolName, 0);
      } catch (Exception ex) {
        LOG.warn("Protocol "  + protocolClass + 
             " NOT registered as getProtocolVersion throws exception ");
        return;
      }
      protocolImplMap.put(new ProtoNameVer(protocolName, version),
          new ProtoClassProtoImpl(protocolClass, protocolImpl)); 
      LOG.info("Protocol Name = " + protocolName +  " version=" + version +
          " ProtocolImpl=" + protocolImpl.getClass().getName() + 
          " protocolClass=" + protocolClass.getName());
    }
    
    private static class VerProtocolImpl {
      final long version;
      final ProtoClassProtoImpl protocolTarget;
      VerProtocolImpl(long ver, ProtoClassProtoImpl protocolTarget) {
        this.version = ver;
        this.protocolTarget = protocolTarget;
      }
    }
    
    
    @SuppressWarnings("unused") // will be useful later.
    private VerProtocolImpl[] getSupportedProtocolVersions(
        String protocolName) {
      VerProtocolImpl[] resultk = new  VerProtocolImpl[protocolImplMap.size()];
      int i = 0;
      for (Map.Entry<ProtoNameVer, ProtoClassProtoImpl> pv :
                                        protocolImplMap.entrySet()) {
        if (pv.getKey().protocol.equals(protocolName)) {
          resultk[i++] = 
              new VerProtocolImpl(pv.getKey().version, pv.getValue());
        }
      }
      if (i == 0) {
        return null;
      }
      VerProtocolImpl[] result = new VerProtocolImpl[i];
      System.arraycopy(resultk, 0, result, 0, i);
      return result;
    }
    
    private VerProtocolImpl getHighestSupportedProtocol(String protocolName) {    
      Long highestVersion = 0L;
      ProtoClassProtoImpl highest = null;
      for (Map.Entry<ProtoNameVer, ProtoClassProtoImpl> pv : protocolImplMap
          .entrySet()) {
        if (pv.getKey().protocol.equals(protocolName)) {
          if ((highest == null) || (pv.getKey().version > highestVersion)) {
            highest = pv.getValue();
            highestVersion = pv.getKey().version;
          } 
        }
      }
      if (highest == null) {
        return null;
      }
      return new VerProtocolImpl(highestVersion,  highest);   
    }
 

    /** Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * 
     * @deprecated Use #Server(Class, Object, Configuration, String, int)
     *    
     */
    @Deprecated
    public Server(Object instance, Configuration conf, String bindAddress,
        int port) 
      throws IOException {
      this(null, instance, conf,  bindAddress, port);
    }
    
    
    /** Construct an RPC server.
     * @param protocol class
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     */
    public Server(Class<?> protocolClass, Object protocolImpl, 
        Configuration conf, String bindAddress, int port) 
      throws IOException {
      this(protocolClass, protocolImpl, conf,  bindAddress, port, 1, -1, -1,
          false, null);
    }
    
    private static String classNameBase(String className) {
      String[] names = className.split("\\.", -1);
      if (names == null || names.length == 0) {
        return className;
      }
      return names[names.length-1];
    }
    
    
    /** Construct an RPC server.
     * @param protocolImpl the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     * 
     * @deprecated use Server#Server(Class, Object, 
     *      Configuration, String, int, int, int, int, boolean, SecretManager)
     */
    @Deprecated
    public Server(Object protocolImpl, Configuration conf, String bindAddress,
        int port, int numHandlers, int numReaders, int queueSizePerHandler,
        boolean verbose, SecretManager<? extends TokenIdentifier> secretManager) 
            throws IOException {
       this(null, protocolImpl,  conf,  bindAddress,   port,
                   numHandlers,  numReaders,  queueSizePerHandler,  verbose, 
                   secretManager);
   
    }
    
    /** Construct an RPC server.
     * @param protocolClass - the protocol being registered
     *     can be null for compatibility with old usage (see below for details)
     * @param protocolImpl the protocol impl that will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     */
    public Server(Class<?> protocolClass, Object protocolImpl,
        Configuration conf, String bindAddress,  int port,
        int numHandlers, int numReaders, int queueSizePerHandler, 
        boolean verbose, SecretManager<? extends TokenIdentifier> secretManager) 
        throws IOException {
      super(bindAddress, port, Invocation.class, numHandlers, numReaders,
          queueSizePerHandler, conf,
          classNameBase(protocolImpl.getClass().getName()), secretManager);

      this.verbose = verbose;
      
      
      Class<?>[] protocols;
      if (protocolClass == null) { // derive protocol from impl
        /*
         * In order to remain compatible with the old usage where a single
         * target protocolImpl is suppled for all protocol interfaces, and
         * the protocolImpl is derived from the protocolClass(es) 
         * we register all interfaces extended by the protocolImpl
         */
        protocols = getProtocolInterfaces(protocolImpl.getClass());

      } else {
        if (!protocolClass.isAssignableFrom(protocolImpl.getClass())) {
          throw new IOException("protocolClass "+ protocolClass +
              " is not implemented by protocolImpl which is of class " +
              protocolImpl.getClass());
        }
        // register protocol class and its super interfaces
        registerProtocolAndImpl(protocolClass, protocolImpl);
        protocols = getProtocolInterfaces(protocolClass);
      }
      for (Class<?> p : protocols) {
        if (!p.equals(VersionedProtocol.class)) {
          registerProtocolAndImpl(p, protocolImpl);
        }
      }

    }

 
    @Override
    public <PROTO, IMPL extends PROTO> Server
      addProtocol(
        Class<PROTO> protocolClass, IMPL protocolImpl) throws IOException {
      registerProtocolAndImpl(protocolClass, protocolImpl);
      return this;
    }
    
    /**
     * Process a client call
     * @param protocolName - the protocol name (the class of the client proxy
     *      used to make calls to the rpc server.
     * @param param  parameters
     * @param receivedTime time at which the call receoved (for metrics)
     * @return the call's return
     * @throws IOException
     */
    public Writable call(String protocolName, Writable param, long receivedTime) 
    throws IOException {
      try {
        Invocation call = (Invocation)param;
        if (verbose) log("Call: " + call);

        // Verify rpc version
        if (call.getRpcVersion() != writableRpcVersion) {
          // Client is using a different version of WritableRpc
          throw new IOException(
              "WritableRpc version mismatch, client side version="
                  + call.getRpcVersion() + ", server side version="
                  + writableRpcVersion);
        }

        long clientVersion = call.getProtocolVersion();
        final String protoName;
        ProtoClassProtoImpl protocolImpl;
        if (call.declaringClassProtocolName.equals(VersionedProtocol.class.getName())) {
          // VersionProtocol methods are often used by client to figure out
          // which version of protocol to use.
          //
          // Versioned protocol methods should go the protocolName protocol
          // rather than the declaring class of the method since the
          // the declaring class is VersionedProtocol which is not 
          // registered directly.
          // Send the call to the highest  protocol version
          protocolImpl = 
              getHighestSupportedProtocol(protocolName).protocolTarget;
        } else {
          protoName = call.declaringClassProtocolName;

          // Find the right impl for the protocol based on client version.
          ProtoNameVer pv = 
              new ProtoNameVer(call.declaringClassProtocolName, clientVersion);
          protocolImpl = protocolImplMap.get(pv);
          if (protocolImpl == null) { // no match for Protocol AND Version
             VerProtocolImpl highest = 
                 getHighestSupportedProtocol(protoName);
            if (highest == null) {
              throw new IOException("Unknown protocol: " + protoName);
            } else { // protocol supported but not the version that client wants
              throw new RPC.VersionMismatch(protoName, clientVersion,
                highest.version);
            }
          }
        }
        

        // Invoke the protocol method

        long startTime = System.currentTimeMillis();
        Method method = 
            protocolImpl.protocolClass.getMethod(call.getMethodName(),
            call.getParameterClasses());
        method.setAccessible(true);
        rpcDetailedMetrics.init(protocolImpl.protocolClass);
        Object value = 
            method.invoke(protocolImpl.protocolImpl, call.getParameters());
        int processingTime = (int) (System.currentTimeMillis() - startTime);
        int qTime = (int) (startTime-receivedTime);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Served: " + call.getMethodName() +
                    " queueTime= " + qTime +
                    " procesingTime= " + processingTime);
        }
        rpcMetrics.addRpcQueueTime(qTime);
        rpcMetrics.addRpcProcessingTime(processingTime);
        rpcDetailedMetrics.addProcessingTime(call.getMethodName(),
                                             processingTime);
        if (verbose) log("Return: "+value);

        return new ObjectWritable(method.getReturnType(), value);

      } catch (InvocationTargetException e) {
        Throwable target = e.getTargetException();
        if (target instanceof IOException) {
          throw (IOException)target;
        } else {
          IOException ioe = new IOException(target.toString());
          ioe.setStackTrace(target.getStackTrace());
          throw ioe;
        }
      } catch (Throwable e) {
        if (!(e instanceof IOException)) {
          LOG.error("Unexpected throwable object ", e);
        }
        IOException ioe = new IOException(e.toString());
        ioe.setStackTrace(e.getStackTrace());
        throw ioe;
      }
    }
  }

  private static void log(String value) {
    if (value!= null && value.length() > 55)
      value = value.substring(0, 55)+"...";
    LOG.info(value);
  }
}
