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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.lang.reflect.Method;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.io.*;
import java.io.Closeable;
import java.util.Map;
import java.util.HashMap;

import javax.net.SocketFactory;

import org.apache.commons.logging.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.ReflectionUtils;

/** A simple RPC mechanism.
 *
 * A <i>protocol</i> is a Java interface.  All parameters and return types must
 * be one of:
 *
 * <ul> <li>a primitive type, <code>boolean</code>, <code>byte</code>,
 * <code>char</code>, <code>short</code>, <code>int</code>, <code>long</code>,
 * <code>float</code>, <code>double</code>, or <code>void</code>; or</li>
 *
 * <li>a {@link String}; or</li>
 *
 * <li>a {@link Writable}; or</li>
 *
 * <li>an array of the above types</li> </ul>
 *
 * All methods in the protocol should throw only IOException.  No field data of
 * the protocol instance is transmitted.
 */
public class RPC {
  static final Log LOG = LogFactory.getLog(RPC.class);
  
  
  /**
   * Get the protocol name.
   *  If the protocol class has a ProtocolAnnotation, then get the protocol
   *  name from the annotation; otherwise the class name is the protocol name.
   */
  static public String getProtocolName(Class<?> protocol) {
    if (protocol == null) {
      return null;
    }
    ProtocolInfo anno = (ProtocolInfo) protocol.getAnnotation(ProtocolInfo.class);
    return  (anno == null) ? protocol.getName() : anno.protocolName();
  }

  private RPC() {}                                  // no public ctor

  // cache of RpcEngines by protocol
  private static final Map<Class<?>,RpcEngine> PROTOCOL_ENGINES
    = new HashMap<Class<?>,RpcEngine>();

  private static final String ENGINE_PROP = "rpc.engine";

  /**
   * Set a protocol to use a non-default RpcEngine.
   * @param conf configuration to use
   * @param protocol the protocol interface
   * @param engine the RpcEngine impl
   */
  public static void setProtocolEngine(Configuration conf,
                                Class<?> protocol, Class<?> engine) {
    conf.setClass(ENGINE_PROP+"."+protocol.getName(), engine, RpcEngine.class);
  }

  // return the RpcEngine configured to handle a protocol
  private static synchronized RpcEngine getProtocolEngine(Class<?> protocol,
                                                          Configuration conf) {
    RpcEngine engine = PROTOCOL_ENGINES.get(protocol);
    if (engine == null) {
      Class<?> impl = conf.getClass(ENGINE_PROP+"."+protocol.getName(),
                                    WritableRpcEngine.class);
      engine = (RpcEngine)ReflectionUtils.newInstance(impl, conf);
      PROTOCOL_ENGINES.put(protocol, engine);
    }
    return engine;
  }

  /**
   * A version mismatch for the RPC protocol.
   */
  public static class VersionMismatch extends IOException {
    private static final long serialVersionUID = 0;

    private String interfaceName;
    private long clientVersion;
    private long serverVersion;
    
    /**
     * Create a version mismatch exception
     * @param interfaceName the name of the protocol mismatch
     * @param clientVersion the client's version of the protocol
     * @param serverVersion the server's version of the protocol
     */
    public VersionMismatch(String interfaceName, long clientVersion,
                           long serverVersion) {
      super("Protocol " + interfaceName + " version mismatch. (client = " +
            clientVersion + ", server = " + serverVersion + ")");
      this.interfaceName = interfaceName;
      this.clientVersion = clientVersion;
      this.serverVersion = serverVersion;
    }
    
    /**
     * Get the interface name
     * @return the java class name 
     *          (eg. org.apache.hadoop.mapred.InterTrackerProtocol)
     */
    public String getInterfaceName() {
      return interfaceName;
    }
    
    /**
     * Get the client's preferred version
     */
    public long getClientVersion() {
      return clientVersion;
    }
    
    /**
     * Get the server's agreed to version.
     */
    public long getServerVersion() {
      return serverVersion;
    }
  }

  /**
   * Get a proxy connection to a remote server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @return the proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> T waitForProxy(
      Class<T> protocol,
      long clientVersion,
      InetSocketAddress addr,
      Configuration conf
      ) throws IOException {
    return waitForProtocolProxy(protocol, clientVersion, addr, conf).getProxy();
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @return the protocol proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> ProtocolProxy<T> waitForProtocolProxy(Class<T> protocol,
                             long clientVersion,
                             InetSocketAddress addr,
                             Configuration conf) throws IOException {
    return waitForProtocolProxy(
        protocol, clientVersion, addr, conf, Long.MAX_VALUE);
  }

  /**
   * Get a proxy connection to a remote server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param connTimeout time in milliseconds before giving up
   * @return the proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> T waitForProxy(Class<T> protocol, long clientVersion,
                             InetSocketAddress addr, Configuration conf,
                             long connTimeout) throws IOException { 
    return waitForProtocolProxy(protocol, clientVersion, addr,
        conf, connTimeout).getProxy();
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param connTimeout time in milliseconds before giving up
   * @return the protocol proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> ProtocolProxy<T> waitForProtocolProxy(Class<T> protocol,
                             long clientVersion,
                             InetSocketAddress addr, Configuration conf,
                             long connTimeout) throws IOException { 
    return waitForProtocolProxy(protocol, clientVersion, addr, conf, 0, connTimeout);
  }
  
  /**
   * Get a proxy connection to a remote server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param rpcTimeout timeout for each RPC
   * @param timeout time in milliseconds before giving up
   * @return the proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> T waitForProxy(Class<T> protocol,
                             long clientVersion,
                             InetSocketAddress addr, Configuration conf,
                             int rpcTimeout,
                             long timeout) throws IOException {
    return waitForProtocolProxy(protocol, clientVersion, addr,
        conf, rpcTimeout, timeout).getProxy();
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param rpcTimeout timeout for each RPC
   * @param timeout time in milliseconds before giving up
   * @return the proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> ProtocolProxy<T> waitForProtocolProxy(Class<T> protocol,
                               long clientVersion,
                               InetSocketAddress addr, Configuration conf,
                               int rpcTimeout,
                               long timeout) throws IOException { 
    long startTime = System.currentTimeMillis();
    IOException ioe;
    while (true) {
      try {
        return getProtocolProxy(protocol, clientVersion, addr, 
            UserGroupInformation.getCurrentUser(), conf, NetUtils
            .getDefaultSocketFactory(conf), rpcTimeout);
      } catch(ConnectException se) {  // namenode has not been started
        LOG.info("Server at " + addr + " not available yet, Zzzzz...");
        ioe = se;
      } catch(SocketTimeoutException te) {  // namenode is busy
        LOG.info("Problem connecting to server: " + addr);
        ioe = te;
      } catch(NoRouteToHostException nrthe) { // perhaps a VIP is failing over
        LOG.info("No route to host for server: " + addr);
        ioe = nrthe;
      }
      // check if timed out
      if (System.currentTimeMillis()-timeout >= startTime) {
        throw ioe;
      }

      // wait for retry
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        // IGNORE
      }
    }
  }

  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. 
   * @param <T>*/
  public static <T> T getProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr, Configuration conf,
                                SocketFactory factory) throws IOException {
    return getProtocolProxy(
        protocol, clientVersion, addr, conf, factory).getProxy();
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param factory socket factory
   * @return the protocol proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> ProtocolProxy<T> getProtocolProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr, Configuration conf,
                                SocketFactory factory) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    return getProtocolProxy(protocol, clientVersion, addr, ugi, conf, factory);
  }
  
  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. 
   * @param <T>*/
  public static <T> T getProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr,
                                UserGroupInformation ticket,
                                Configuration conf,
                                SocketFactory factory) throws IOException {
    return getProtocolProxy(
        protocol, clientVersion, addr, ticket, conf, factory).getProxy();
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param ticket user group information
   * @param conf configuration to use
   * @param factory socket factory
   * @return the protocol proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> ProtocolProxy<T> getProtocolProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr,
                                UserGroupInformation ticket,
                                Configuration conf,
                                SocketFactory factory) throws IOException {
    return getProtocolProxy(
        protocol, clientVersion, addr, ticket, conf, factory, 0);
  }
  
  /**
   * Construct a client-side proxy that implements the named protocol,
   * talking to a server at the named address.
   * @param <T>
   * 
   * @param protocol protocol
   * @param clientVersion client's version
   * @param addr server address
   * @param ticket security ticket
   * @param conf configuration
   * @param factory socket factory
   * @param rpcTimeout max time for each rpc; 0 means no timeout
   * @return the proxy
   * @throws IOException if any error occurs
   */
  public static <T> T getProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr,
                                UserGroupInformation ticket,
                                Configuration conf,
                                SocketFactory factory,
                                int rpcTimeout) throws IOException {
    return getProtocolProxy(protocol, clientVersion, addr, ticket,
             conf, factory, rpcTimeout).getProxy();
  }
  
  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol
   * @param clientVersion client's version
   * @param addr server address
   * @param ticket security ticket
   * @param conf configuration
   * @param factory socket factory
   * @param rpcTimeout max time for each rpc; 0 means no timeout
   * @return the proxy
   * @throws IOException if any error occurs
   */
   public static <T> ProtocolProxy<T> getProtocolProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr,
                                UserGroupInformation ticket,
                                Configuration conf,
                                SocketFactory factory,
                                int rpcTimeout) throws IOException {    
    if (UserGroupInformation.isSecurityEnabled()) {
      SaslRpcServer.init(conf);
    }
    return getProtocolEngine(protocol,conf).getProxy(protocol,
        clientVersion, addr, ticket, conf, factory, rpcTimeout);
  }

   /**
    * Construct a client-side proxy object with the default SocketFactory
    * @param <T>
    * 
    * @param protocol
    * @param clientVersion
    * @param addr
    * @param conf
    * @return a proxy instance
    * @throws IOException
    */
   public static <T> T getProxy(Class<T> protocol,
                                 long clientVersion,
                                 InetSocketAddress addr, Configuration conf)
     throws IOException {

     return getProtocolProxy(protocol, clientVersion, addr, conf).getProxy();
   }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol
   * @param clientVersion
   * @param addr
   * @param conf
   * @return a protocol proxy
   * @throws IOException
   */
  public static <T> ProtocolProxy<T> getProtocolProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr, Configuration conf)
    throws IOException {

    return getProtocolProxy(protocol, clientVersion, addr, conf, NetUtils
        .getDefaultSocketFactory(conf));
  }

  /**
   * Stop this proxy and release its invoker's resource by getting the
   * invocation handler for the given proxy object and calling
   * {@link Closeable#close} if that invocation handler implements
   * {@link Closeable}.
   * 
   * @param proxy the RPC proxy object to be stopped
   */
  public static void stopProxy(Object proxy) {
    InvocationHandler invocationHandler = null;
    try {
      invocationHandler = Proxy.getInvocationHandler(proxy);
    } catch (IllegalArgumentException e) {
      LOG.error("Tried to call RPC.stopProxy on an object that is not a proxy.", e);
    }
    if (proxy != null && invocationHandler != null &&
        invocationHandler instanceof Closeable) {
      try {
        ((Closeable)invocationHandler).close();
      } catch (IOException e) {
        LOG.error("Stopping RPC invocation handler caused exception", e);
      }
    } else {
      LOG.error("Could not get invocation handler " + invocationHandler +
          " for proxy " + proxy + ", or invocation handler is not closeable.");
    }
  }

  /** 
   * Expert: Make multiple, parallel calls to a set of servers.
   * @deprecated Use {@link #call(Method, Object[][], InetSocketAddress[], UserGroupInformation, Configuration)} instead 
   */
  @Deprecated
  public static Object[] call(Method method, Object[][] params,
                              InetSocketAddress[] addrs, Configuration conf)
    throws IOException, InterruptedException {
    return call(method, params, addrs, null, conf);
  }
  
  /** Expert: Make multiple, parallel calls to a set of servers. */
  public static Object[] call(Method method, Object[][] params,
                              InetSocketAddress[] addrs, 
                              UserGroupInformation ticket, Configuration conf)
    throws IOException, InterruptedException {

    return getProtocolEngine(method.getDeclaringClass(), conf)
      .call(method, params, addrs, ticket, conf);
  }

  /** Construct a server for a protocol implementation instance listening on a
   * port and address.
   * @deprecated protocol interface should be passed.
   */
  @Deprecated
  public static Server getServer(final Object instance, final String bindAddress, final int port, Configuration conf) 
    throws IOException {
    return getServer(instance, bindAddress, port, 1, false, conf);
  }

  /** Construct a server for a protocol implementation instance listening on a
   * port and address.
   * @deprecated protocol interface should be passed.
   */
  @Deprecated
  public static Server getServer(final Object instance, final String bindAddress, final int port,
                                 final int numHandlers,
                                 final boolean verbose, Configuration conf) 
    throws IOException {
    return getServer(instance.getClass(),         // use impl class for protocol
                     instance, bindAddress, port, numHandlers, false, conf, null);
  }

  /** Construct a server for a protocol implementation instance. */
  public static Server getServer(Class<?> protocol,
                                 Object instance, String bindAddress,
                                 int port, Configuration conf) 
    throws IOException {
    return getServer(protocol, instance, bindAddress, port, 1, false, conf, null);
  }

  /** Construct a server for a protocol implementation instance.
   * @deprecated secretManager should be passed.
   */
  @Deprecated
  public static Server getServer(Class<?> protocol,
                                 Object instance, String bindAddress, int port,
                                 int numHandlers,
                                 boolean verbose, Configuration conf) 
    throws IOException {
    
    return getServer(protocol, instance, bindAddress, port, numHandlers, verbose,
                 conf, null);
  }
  
  /** Construct a server for a protocol implementation instance. */
  public static Server getServer(Class<?> protocol,
                                 Object instance, String bindAddress, int port,
                                 int numHandlers,
                                 boolean verbose, Configuration conf,
                                 SecretManager<? extends TokenIdentifier> secretManager) 
    throws IOException {
    
    return getProtocolEngine(protocol, conf)
      .getServer(protocol, instance, bindAddress, port, numHandlers, -1, -1,
                 verbose, conf, secretManager);
  }

  /** Construct a server for a protocol implementation instance. */

  public static <PROTO extends VersionedProtocol, IMPL extends PROTO> 
        Server getServer(Class<PROTO> protocol,
                                 IMPL instance, String bindAddress, int port,
                                 int numHandlers, int numReaders, int queueSizePerHandler,
                                 boolean verbose, Configuration conf,
                                 SecretManager<? extends TokenIdentifier> secretManager) 
    throws IOException {
    
    return getProtocolEngine(protocol, conf)
      .getServer(protocol, instance, bindAddress, port, numHandlers,
                 numReaders, queueSizePerHandler, verbose, conf, secretManager);
  }

  /** An RPC Server. */
  public abstract static class Server extends org.apache.hadoop.ipc.Server {
  
    protected Server(String bindAddress, int port, 
                     Class<? extends Writable> paramClass, int handlerCount,
                     int numReaders, int queueSizePerHandler,
                     Configuration conf, String serverName, 
                     SecretManager<? extends TokenIdentifier> secretManager) throws IOException {
      super(bindAddress, port, paramClass, handlerCount, numReaders, queueSizePerHandler,
            conf, serverName, secretManager);
    }
    
    /**
     * Add a protocol to the existing server.
     * @param protocolClass - the protocol class
     * @param protocolImpl - the impl of the protocol that will be called
     * @return the server (for convenience)
     */
    public <PROTO extends VersionedProtocol, IMPL extends PROTO>
      Server addProtocol(Class<PROTO> protocolClass, IMPL protocolImpl
    ) throws IOException {
      throw new IOException("addProtocol Not Implemented");
    }
  }

}
