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

package org.apache.hadoop.hbase.ipc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.security.HBasePolicyProvider;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenSecretManager;
import org.apache.hadoop.hbase.util.Objects;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;

import javax.net.SocketFactory;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.*;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;

/**
 * A loadable RPC engine supporting SASL authentication of connections, using
 * GSSAPI for Kerberos authentication or DIGEST-MD5 for authentication via
 * signed tokens.
 *
 * <p>
 * This is a fork of the {@code org.apache.hadoop.ipc.WriteableRpcEngine} from
 * secure Hadoop, reworked to eliminate code duplication with the existing
 * HBase {@link WritableRpcEngine}.
 * </p>
 *
 * @see SecureClient
 * @see SecureServer
 */
public class SecureRpcEngine implements RpcEngine {
  // Leave this out in the hadoop ipc package but keep class name.  Do this
  // so that we dont' get the logging of this class's invocations by doing our
  // blanket enabling DEBUG on the o.a.h.h. package.
  protected static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.ipc.SecureRpcEngine");

  private SecureRpcEngine() {
    super();
  }                                  // no public ctor

  /* Cache a client using its socket factory as the hash key */
  static private class ClientCache {
    private Map<SocketFactory, SecureClient> clients =
      new HashMap<SocketFactory, SecureClient>();

    protected ClientCache() {}

    /**
     * Construct & cache an IPC client with the user-provided SocketFactory
     * if no cached client exists.
     *
     * @param conf Configuration
     * @param factory socket factory
     * @return an IPC client
     */
    protected synchronized SecureClient getClient(Configuration conf,
        SocketFactory factory) {
      // Construct & cache client.  The configuration is only used for timeout,
      // and Clients have connection pools.  So we can either (a) lose some
      // connection pooling and leak sockets, or (b) use the same timeout for all
      // configurations.  Since the IPC is usually intended globally, not
      // per-job, we choose (a).
      SecureClient client = clients.get(factory);
      if (client == null) {
        // Make an hbase client instead of hadoop Client.
        client = new SecureClient(HbaseObjectWritable.class, conf, factory);
        clients.put(factory, client);
      } else {
        client.incCount();
      }
      return client;
    }

    /**
     * Construct & cache an IPC client with the default SocketFactory
     * if no cached client exists.
     *
     * @param conf Configuration
     * @return an IPC client
     */
    protected synchronized SecureClient getClient(Configuration conf) {
      return getClient(conf, SocketFactory.getDefault());
    }

    /**
     * Stop a RPC client connection
     * A RPC client is closed only when its reference count becomes zero.
     * @param client client to stop
     */
    protected void stopClient(SecureClient client) {
      synchronized (this) {
        client.decCount();
        if (client.isZeroReference()) {
          clients.remove(client.getSocketFactory());
        }
      }
      if (client.isZeroReference()) {
        client.stop();
      }
    }
  }

  protected final static ClientCache CLIENTS = new ClientCache();

  private static class Invoker implements InvocationHandler {
    private Class<? extends VersionedProtocol> protocol;
    private InetSocketAddress address;
    private User ticket;
    private SecureClient client;
    private boolean isClosed = false;
    final private int rpcTimeout;

    public Invoker(Class<? extends VersionedProtocol> protocol,
        InetSocketAddress address, User ticket,
        Configuration conf, SocketFactory factory, int rpcTimeout) {
      this.protocol = protocol;
      this.address = address;
      this.ticket = ticket;
      this.client = CLIENTS.getClient(conf, factory);
      this.rpcTimeout = rpcTimeout;
    }

    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      final boolean logDebug = LOG.isDebugEnabled();
      long startTime = 0;
      if (logDebug) {
        startTime = System.currentTimeMillis();
      }
      HbaseObjectWritable value = (HbaseObjectWritable)
        client.call(new Invocation(method, args), address,
                    protocol, ticket, rpcTimeout);
      if (logDebug) {
        long callTime = System.currentTimeMillis() - startTime;
        LOG.debug("Call: " + method.getName() + " " + callTime);
      }
      return value.get();
    }

    /* close the IPC client that's responsible for this invoker's RPCs */
    synchronized protected void close() {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }
  }

  /**
   * Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address.
   *
   * @param protocol interface
   * @param clientVersion version we are expecting
   * @param addr remote address
   * @param ticket ticket
   * @param conf configuration
   * @param factory socket factory
   * @return proxy
   * @throws java.io.IOException e
   */
  public VersionedProtocol getProxy(
      Class<? extends VersionedProtocol> protocol, long clientVersion,
      InetSocketAddress addr, User ticket,
      Configuration conf, SocketFactory factory, int rpcTimeout)
  throws IOException {
    if (User.isSecurityEnabled()) {
      HBaseSaslRpcServer.init(conf);
    }
    VersionedProtocol proxy =
        (VersionedProtocol) Proxy.newProxyInstance(
            protocol.getClassLoader(), new Class[] { protocol },
            new Invoker(protocol, addr, ticket, conf, factory, rpcTimeout));
    long serverVersion = proxy.getProtocolVersion(protocol.getName(),
                                                  clientVersion);
    if (serverVersion != clientVersion) {
      throw new HBaseRPC.VersionMismatch(protocol.getName(), clientVersion,
                                serverVersion);
    }
    return proxy;
  }

  /**
   * Stop this proxy and release its invoker's resource
   * @param proxy the proxy to be stopped
   */
  public void stopProxy(VersionedProtocol proxy) {
    if (proxy!=null) {
      ((Invoker)Proxy.getInvocationHandler(proxy)).close();
    }
  }


  /** Expert: Make multiple, parallel calls to a set of servers. */
  public Object[] call(Method method, Object[][] params,
                       InetSocketAddress[] addrs,
                       Class<? extends VersionedProtocol> protocol,
                       User ticket, Configuration conf)
    throws IOException, InterruptedException {

    Invocation[] invocations = new Invocation[params.length];
    for (int i = 0; i < params.length; i++)
      invocations[i] = new Invocation(method, params[i]);
    SecureClient client = CLIENTS.getClient(conf);
    try {
      Writable[] wrappedValues =
        client.call(invocations, addrs, protocol, ticket);

      if (method.getReturnType() == Void.TYPE) {
        return null;
      }

      Object[] values =
          (Object[])Array.newInstance(method.getReturnType(), wrappedValues.length);
      for (int i = 0; i < values.length; i++)
        if (wrappedValues[i] != null)
          values[i] = ((HbaseObjectWritable)wrappedValues[i]).get();

      return values;
    } finally {
      CLIENTS.stopClient(client);
    }
  }

  /** Construct a server for a protocol implementation instance listening on a
   * port and address, with a secret manager. */
  public Server getServer(Class<? extends VersionedProtocol> protocol,
      final Object instance,
      Class<?>[] ifaces,
      final String bindAddress, final int port,
      final int numHandlers,
      int metaHandlerCount, final boolean verbose, Configuration conf,
       int highPriorityLevel)
    throws IOException {
    Server server = new Server(instance, ifaces, conf, bindAddress, port,
            numHandlers, metaHandlerCount, verbose,
            highPriorityLevel);
    return server;
  }

  /** An RPC Server. */
  public static class Server extends SecureServer {
    private Object instance;
    private Class<?> implementation;
    private Class<?>[] ifaces;
    private boolean verbose;

    private static String classNameBase(String className) {
      String[] names = className.split("\\.", -1);
      if (names == null || names.length == 0) {
        return className;
      }
      return names[names.length-1];
    }

    /** Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     * @throws java.io.IOException e
     */
    public Server(Object instance, final Class<?>[] ifaces,
                  Configuration conf, String bindAddress,  int port,
                  int numHandlers, int metaHandlerCount, boolean verbose,
                  int highPriorityLevel)
        throws IOException {
      super(bindAddress, port, Invocation.class, numHandlers, metaHandlerCount, conf,
          classNameBase(instance.getClass().getName()), highPriorityLevel);
      this.instance = instance;
      this.implementation = instance.getClass();
      this.verbose = verbose;

      this.ifaces = ifaces;

      // create metrics for the advertised interfaces this server implements.
      this.rpcMetrics.createMetrics(this.ifaces);
    }

    public AuthenticationTokenSecretManager createSecretManager(){
      if (instance instanceof org.apache.hadoop.hbase.Server) {
        org.apache.hadoop.hbase.Server server =
            (org.apache.hadoop.hbase.Server)instance;
        Configuration conf = server.getConfiguration();
        long keyUpdateInterval =
            conf.getLong("hbase.auth.key.update.interval", 24*60*60*1000);
        long maxAge =
            conf.getLong("hbase.auth.token.max.lifetime", 7*24*60*60*1000);
        return new AuthenticationTokenSecretManager(conf, server.getZooKeeper(),
            server.getServerName().toString(), keyUpdateInterval, maxAge);
      }
      return null;
    }

    @Override
    public void startThreads() {
      AuthenticationTokenSecretManager mgr = createSecretManager();
      if (mgr != null) {
        setSecretManager(mgr);
        mgr.start();
      }
      this.authManager = new ServiceAuthorizationManager();
      HBasePolicyProvider.init(conf, authManager);

      // continue with base startup
      super.startThreads();
    }

    @Override
    public Writable call(Class<? extends VersionedProtocol> protocol,
        Writable param, long receivedTime, MonitoredRPCHandler status)
    throws IOException {
      try {
        Invocation call = (Invocation)param;
        if(call.getMethodName() == null) {
          throw new IOException("Could not find requested method, the usual " +
              "cause is a version mismatch between client and server.");
        }
        if (verbose) log("Call: " + call);

        Method method =
          protocol.getMethod(call.getMethodName(),
                                   call.getParameterClasses());
        method.setAccessible(true);

        Object impl = null;
        if (protocol.isAssignableFrom(this.implementation)) {
          impl = this.instance;
        }
        else {
          throw new HBaseRPC.UnknownProtocolException(protocol);
        }

        long startTime = System.currentTimeMillis();
        Object[] params = call.getParameters();
        Object value = method.invoke(impl, params);
        int processingTime = (int) (System.currentTimeMillis() - startTime);
        int qTime = (int) (startTime-receivedTime);
        if (TRACELOG.isDebugEnabled()) {
          TRACELOG.debug("Call #" + CurCall.get().id +
              "; Served: " + protocol.getSimpleName()+"#"+call.getMethodName() +
              " queueTime=" + qTime +
              " processingTime=" + processingTime +
              " contents=" + Objects.describeQuantity(params));
        }
        rpcMetrics.rpcQueueTime.inc(qTime);
        rpcMetrics.rpcProcessingTime.inc(processingTime);
        rpcMetrics.inc(call.getMethodName(), processingTime);
        if (verbose) log("Return: "+value);

        return new HbaseObjectWritable(method.getReturnType(), value);
      } catch (InvocationTargetException e) {
        Throwable target = e.getTargetException();
        if (target instanceof IOException) {
          throw (IOException)target;
        }
        IOException ioe = new IOException(target.toString());
        ioe.setStackTrace(target.getStackTrace());
        throw ioe;
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

  protected static void log(String value) {
    String v = value;
    if (v != null && v.length() > 55)
      v = v.substring(0, 55)+"...";
    LOG.info(v);
  }
}