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

package org.apache.hadoop.hbase.ipc;

import java.lang.reflect.Proxy;
import java.lang.reflect.Method;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.io.*;
import java.util.Map;
import java.util.HashMap;
import java.util.Collection;

import javax.net.SocketFactory;

import org.apache.commons.logging.*;

import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.*;

/** A simple RPC mechanism.
 * 
 * This is a local hbase copy of the hadoop RPC so we can do things like
 * address HADOOP-414 for hbase-only and try other hbase-specific
 * optimizations like using our own version of ObjectWritable.  Class has been
 * renamed to avoid confusing it w/ hadoop versions.
 * 
 * <p>Below are continued the class comments from hadoop RPC class.
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
 * 
 * @see org.apache.hadoop.ipc.RPC
 */
public class HbaseRPC {
  private static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.ipc.RPC");

  private HbaseRPC() {}                                  // no public ctor


  /** A method invocation, including the method name and its parameters.*/
  private static class Invocation implements Writable, Configurable {
    private String methodName;
    private Class[] parameterClasses;
    private Object[] parameters;
    private Configuration conf;

    public Invocation() {}

    public Invocation(Method method, Object[] parameters) {
      this.methodName = method.getName();
      this.parameterClasses = method.getParameterTypes();
      this.parameters = parameters;
    }

    /** The name of the method invoked. */
    public String getMethodName() { return methodName; }

    /** The parameter classes. */
    public Class[] getParameterClasses() { return parameterClasses; }

    /** The parameter instances. */
    public Object[] getParameters() { return parameters; }

    public void readFields(DataInput in) throws IOException {
      methodName = Text.readString(in);
      parameters = new Object[in.readInt()];
      parameterClasses = new Class[parameters.length];
      HbaseObjectWritable objectWritable = new HbaseObjectWritable();
      for (int i = 0; i < parameters.length; i++) {
        parameters[i] = HbaseObjectWritable.readObject(in, objectWritable, this.conf);
        parameterClasses[i] = objectWritable.getDeclaredClass();
      }
    }

    public void write(DataOutput out) throws IOException {
      Text.writeString(out, methodName);
      out.writeInt(parameterClasses.length);
      for (int i = 0; i < parameterClasses.length; i++) {
        HbaseObjectWritable.writeObject(out, parameters[i], parameterClasses[i],
                                   conf);
      }
    }

    public String toString() {
      StringBuffer buffer = new StringBuffer();
      buffer.append(methodName);
      buffer.append("(");
      for (int i = 0; i < parameters.length; i++) {
        if (i != 0)
          buffer.append(", ");
        buffer.append(parameters[i]);
      }
      buffer.append(")");
      return buffer.toString();
    }

    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    public Configuration getConf() {
      return this.conf;
    }

  }

  private static Map<SocketFactory, Client> CLIENTS =
      new HashMap<SocketFactory, Client>();

  private static synchronized Client getClient(Configuration conf,
      SocketFactory factory) {
    // Construct & cache client.  The configuration is only used for timeout,
    // and Clients have connection pools.  So we can either (a) lose some
    // connection pooling and leak sockets, or (b) use the same timeout for all
    // configurations.  Since the IPC is usually intended globally, not
    // per-job, we choose (a).
    Client client = CLIENTS.get(factory);
    if (client == null) {
      client = new Client(HbaseObjectWritable.class, conf, factory);
      CLIENTS.put(factory, client);
    }
    return client;
  }
  
  /**
   * Construct & cache client with the default SocketFactory.
   * @param conf
   * @return
   */
  private static Client getClient(Configuration conf) {
    return getClient(conf, SocketFactory.getDefault());
  }

  /**
   * Stop all RPC client connections
   */
  public static synchronized void stopClient(){
    for (Client client : CLIENTS.values())
      client.stop();
    CLIENTS.clear();
  }

  /*
   * remove specified client from the list of clients.
   */
  static synchronized void removeClients() {
    CLIENTS.clear();
  }

  static synchronized Collection allClients() {
    return CLIENTS.values();
  }

  private static class Invoker implements InvocationHandler {
    private InetSocketAddress address;
    private UserGroupInformation ticket;
    private Client client;

    public Invoker(InetSocketAddress address, UserGroupInformation ticket, 
                   Configuration conf, SocketFactory factory) {
      this.address = address;
      this.ticket = ticket;
      this.client = getClient(conf, factory);
    }

    public Object invoke(Object proxy, Method method, Object[] args)
      throws Throwable {
      long startTime = System.currentTimeMillis();
      HbaseObjectWritable value = (HbaseObjectWritable)
        client.call(new Invocation(method, args), address, ticket);
      long callTime = System.currentTimeMillis() - startTime;
      LOG.debug("Call: " + method.getName() + " " + callTime);
      return value.get();
    }
  }

  /**
   * A version mismatch for the RPC protocol.
   */
  public static class VersionMismatch extends IOException {
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
     * Get the client's prefered version
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
  
  public static VersionedProtocol waitForProxy(Class protocol,
                                               long clientVersion,
                                               InetSocketAddress addr,
                                               Configuration conf
                                               ) throws IOException {
    while (true) {
      try {
        return getProxy(protocol, clientVersion, addr, conf);
      } catch(ConnectException se) {  // namenode has not been started
        LOG.info("Server at " + addr + " not available yet, Zzzzz...");
      } catch(SocketTimeoutException te) {  // namenode is busy
        LOG.info("Problem connecting to server: " + addr);
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        // IGNORE
      }
    }
  }
  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. */
  public static VersionedProtocol getProxy(Class<?> protocol,
      long clientVersion, InetSocketAddress addr, Configuration conf,
      SocketFactory factory) throws IOException {
    return getProxy(protocol, clientVersion, addr, null, conf, factory);
  }
  
  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. */
  public static VersionedProtocol getProxy(Class<?> protocol,
      long clientVersion, InetSocketAddress addr, UserGroupInformation ticket,
      Configuration conf, SocketFactory factory) throws IOException {    

    VersionedProtocol proxy =
        (VersionedProtocol) Proxy.newProxyInstance(
            protocol.getClassLoader(), new Class[] { protocol },
            new Invoker(addr, ticket, conf, factory));
    long serverVersion = proxy.getProtocolVersion(protocol.getName(), 
                                                  clientVersion);
    if (serverVersion == clientVersion) {
      return proxy;
    } else {
      throw new VersionMismatch(protocol.getName(), clientVersion, 
                                serverVersion);
    }
  }

  /**
   * Construct a client-side proxy object with the default SocketFactory
   * 
   * @param protocol
   * @param clientVersion
   * @param addr
   * @param conf
   * @return a proxy instance
   * @throws IOException
   */
  public static VersionedProtocol getProxy(Class<?> protocol,
      long clientVersion, InetSocketAddress addr, Configuration conf)
      throws IOException {

    return getProxy(protocol, clientVersion, addr, conf, NetUtils
        .getDefaultSocketFactory(conf));
  }

  /** Expert: Make multiple, parallel calls to a set of servers. */
  public static Object[] call(Method method, Object[][] params,
                              InetSocketAddress[] addrs, Configuration conf)
    throws IOException {

    Invocation[] invocations = new Invocation[params.length];
    for (int i = 0; i < params.length; i++)
      invocations[i] = new Invocation(method, params[i]);
    Writable[] wrappedValues = getClient(conf).call(invocations, addrs);
    
    if (method.getReturnType() == Void.TYPE) {
      return null;
    }

    Object[] values =
      (Object[])Array.newInstance(method.getReturnType(), wrappedValues.length);
    for (int i = 0; i < values.length; i++)
      if (wrappedValues[i] != null)
        values[i] = ((HbaseObjectWritable)wrappedValues[i]).get();
    
    return values;
  }

  /** Construct a server for a protocol implementation instance listening on a
   * port and address. */
  public static Server getServer(final Object instance, final String bindAddress, final int port, Configuration conf) 
    throws IOException {
    return getServer(instance, bindAddress, port, 1, false, conf);
  }

  /** Construct a server for a protocol implementation instance listening on a
   * port and address. */
  public static Server getServer(final Object instance, final String bindAddress, final int port,
                                 final int numHandlers,
                                 final boolean verbose, Configuration conf) 
    throws IOException {
    return new Server(instance, conf, bindAddress, port, numHandlers, verbose);
  }

  /** An RPC Server. */
  public static class Server extends org.apache.hadoop.ipc.Server {
    private Object instance;
    private Class<?> implementation;
    private boolean verbose;

    /** Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     */
    public Server(Object instance, Configuration conf, String bindAddress, int port) 
      throws IOException {
      this(instance, conf,  bindAddress, port, 1, false);
    }

    /** Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     */
    public Server(Object instance, Configuration conf, String bindAddress,  int port,
                  int numHandlers, boolean verbose) throws IOException {
      super(bindAddress, port, Invocation.class, numHandlers, conf);
      this.instance = instance;
      this.implementation = instance.getClass();
      this.verbose = verbose;
    }

    public Writable call(Writable param, long receiveTime) throws IOException {
      try {
        Invocation call = (Invocation)param;
        if (verbose) log("Call: " + call);
        
        Method method =
          implementation.getMethod(call.getMethodName(),
                                   call.getParameterClasses());

        long startTime = System.currentTimeMillis();
        Object value = method.invoke(instance, call.getParameters());
        long callTime = System.currentTimeMillis() - startTime;
        LOG.debug("Served: " + call.getMethodName() + " " + callTime);
        if (verbose) log("Return: "+value);

        return new HbaseObjectWritable(method.getReturnType(), value);

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
