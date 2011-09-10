/*
 * Copyright 2010 The Apache Software Foundation
 *
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

import java.net.InetSocketAddress;
import java.io.*;
import java.util.Map;
import java.util.HashMap;

import javax.net.SocketFactory;

import org.apache.commons.logging.*;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Operation;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Objects;
import org.apache.hadoop.io.*;
import org.apache.hadoop.hbase.ipc.VersionedProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.conf.*;

import org.codehaus.jackson.map.ObjectMapper;

/** An RpcEngine implementation for Writable data. */
class WritableRpcEngine implements RpcEngine {
  // LOG is NOT in hbase subpackage intentionally so that the default HBase
  // DEBUG log level does NOT emit RPC-level logging. 
  private static final Log LOG = LogFactory.getLog("org.apache.hadoop.ipc.RPCEngine");

  /* Cache a client using its socket factory as the hash key */
  static private class ClientCache {
    private Map<SocketFactory, HBaseClient> clients =
      new HashMap<SocketFactory, HBaseClient>();

    protected ClientCache() {}

    /**
     * Construct & cache an IPC client with the user-provided SocketFactory
     * if no cached client exists.
     *
     * @param conf Configuration
     * @param factory socket factory
     * @return an IPC client
     */
    protected synchronized HBaseClient getClient(Configuration conf,
        SocketFactory factory) {
      // Construct & cache client.  The configuration is only used for timeout,
      // and Clients have connection pools.  So we can either (a) lose some
      // connection pooling and leak sockets, or (b) use the same timeout for
      // all configurations.  Since the IPC is usually intended globally, not
      // per-job, we choose (a).
      HBaseClient client = clients.get(factory);
      if (client == null) {
        // Make an hbase client instead of hadoop Client.
        client = new HBaseClient(HbaseObjectWritable.class, conf, factory);
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
    protected synchronized HBaseClient getClient(Configuration conf) {
      return getClient(conf, SocketFactory.getDefault());
    }

    /**
     * Stop a RPC client connection
     * A RPC client is closed only when its reference count becomes zero.
     * @param client client to stop
     */
    protected void stopClient(HBaseClient client) {
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
    private UserGroupInformation ticket;
    private HBaseClient client;
    private boolean isClosed = false;
    final private int rpcTimeout;

    public Invoker(Class<? extends VersionedProtocol> protocol,
                   InetSocketAddress address, UserGroupInformation ticket,
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
        // FIGURE HOW TO TURN THIS OFF!
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

  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. */
  public VersionedProtocol getProxy(
      Class<? extends VersionedProtocol> protocol, long clientVersion,
      InetSocketAddress addr, UserGroupInformation ticket,
      Configuration conf, SocketFactory factory, int rpcTimeout)
    throws IOException {

      VersionedProtocol proxy =
          (VersionedProtocol) Proxy.newProxyInstance(
              protocol.getClassLoader(), new Class[] { protocol },
              new Invoker(protocol, addr, ticket, conf, factory, rpcTimeout));
    if (proxy instanceof VersionedProtocol) {
      long serverVersion = ((VersionedProtocol)proxy)
        .getProtocolVersion(protocol.getName(), clientVersion);
      if (serverVersion != clientVersion) {
        throw new HBaseRPC.VersionMismatch(protocol.getName(), clientVersion,
                                      serverVersion);
      }
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
                       UserGroupInformation ticket, Configuration conf)
    throws IOException, InterruptedException {

    Invocation[] invocations = new Invocation[params.length];
    for (int i = 0; i < params.length; i++)
      invocations[i] = new Invocation(method, params[i]);
    HBaseClient client = CLIENTS.getClient(conf);
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
   * port and address. */
  public Server getServer(Class<? extends VersionedProtocol> protocol,
                          Object instance,
                          Class<?>[] ifaces,
                          String bindAddress, int port,
                          int numHandlers,
                          int metaHandlerCount, boolean verbose,
                          Configuration conf, int highPriorityLevel)
    throws IOException {
    return new Server(instance, ifaces, conf, bindAddress, port, numHandlers,
        metaHandlerCount, verbose, highPriorityLevel);
  }

  /** An RPC Server. */
  public static class Server extends HBaseServer {
    private Object instance;
    private Class<?> implementation;
    private Class<?>[] ifaces;
    private boolean verbose;
    private boolean authorize = false;

    // for JSON encoding
    private static ObjectMapper mapper = new ObjectMapper();

    private static final String WARN_RESPONSE_TIME =
      "hbase.ipc.warn.response.time";
    private static final String WARN_RESPONSE_SIZE =
      "hbase.ipc.warn.response.size";

    /** Default value for above params */
    private static final int DEFAULT_WARN_RESPONSE_TIME = 10000; // milliseconds
    private static final int DEFAULT_WARN_RESPONSE_SIZE = 100 * 1024 * 1024;

    /** Names for suffixed metrics */
    private static final String ABOVE_ONE_SEC_METRIC = ".aboveOneSec.";

    private final int warnResponseTime;
    private final int warnResponseSize;

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
     * @throws IOException e
     */
    public Server(Object instance, final Class<?>[] ifaces,
                  Configuration conf, String bindAddress,  int port,
                  int numHandlers, int metaHandlerCount, boolean verbose,
                  int highPriorityLevel) throws IOException {
      super(bindAddress, port, Invocation.class, numHandlers, metaHandlerCount,
          conf, classNameBase(instance.getClass().getName()),
          highPriorityLevel);
      this.instance = instance;
      this.implementation = instance.getClass();
      this.verbose = verbose;

      this.ifaces = ifaces;

      // create metrics for the advertised interfaces this server implements.
      String [] metricSuffixes = new String [] {ABOVE_ONE_SEC_METRIC};
      this.rpcMetrics.createMetrics(this.ifaces, false, metricSuffixes);

      this.authorize =
        conf.getBoolean(
            ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, false);

      this.warnResponseTime = conf.getInt(WARN_RESPONSE_TIME,
          DEFAULT_WARN_RESPONSE_TIME);
      this.warnResponseSize = conf.getInt(WARN_RESPONSE_SIZE,
          DEFAULT_WARN_RESPONSE_SIZE);
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
        status.setRPC(call.getMethodName(), call.getParameters(), receivedTime);
        status.setRPCPacket(param);
        status.resume("Servicing call");

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

        HbaseObjectWritable retVal =
          new HbaseObjectWritable(method.getReturnType(), value);
        long responseSize = retVal.getWritableSize();
        // log any RPC responses that are slower than the configured warn
        // response time or larger than configured warning size
        boolean tooSlow = (processingTime > warnResponseTime
            && warnResponseTime > -1);
        boolean tooLarge = (responseSize > warnResponseSize
            && warnResponseSize > -1);
        if (tooSlow || tooLarge) {
          // when tagging, we let TooLarge trump TooSmall to keep output simple
          // note that large responses will often also be slow.
          logResponse(call, (tooLarge ? "TooLarge" : "TooSlow"),
              status.getClient(), startTime, processingTime, qTime,
              responseSize);
          // provides a count of log-reported slow responses
          if (tooSlow) {
            rpcMetrics.rpcSlowResponseTime.inc(processingTime);
          }
        }
        if (processingTime > 1000) {
          // we use a hard-coded one second period so that we can clearly
          // indicate the time period we're warning about in the name of the 
          // metric itself
          rpcMetrics.inc(call.getMethodName() + ABOVE_ONE_SEC_METRIC,
              processingTime);
        }

        return retVal;
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

    /**
     * Logs an RPC response to the LOG file, producing valid JSON objects for
     * client Operations.
     * @param call The call to log.
     * @param tag  The tag that will be used to indicate this event in the log.
     * @param client          The address of the client who made this call.
     * @param startTime       The time that the call was initiated, in ms.
     * @param processingTime  The duration that the call took to run, in ms.
     * @param qTime           The duration that the call spent on the queue 
     *                        prior to being initiated, in ms.
     * @param responseSize    The size in bytes of the response buffer.
     */
    private void logResponse(Invocation call, String tag, String clientAddress,
        long startTime, int processingTime, int qTime, long responseSize)
      throws IOException {
      Object params[] = call.getParameters();
      // for JSON encoding
      ObjectMapper mapper = new ObjectMapper();
      // base information that is reported regardless of type of call
      Map<String, Object> responseInfo = new HashMap<String, Object>();
      responseInfo.put("starttimems", startTime);
      responseInfo.put("processingtimems", processingTime);
      responseInfo.put("queuetimems", qTime);
      responseInfo.put("responsesize", responseSize);
      responseInfo.put("client", clientAddress);
      responseInfo.put("class", instance.getClass().getSimpleName());
      responseInfo.put("method", call.getMethodName());
      if (params.length == 2 && instance instanceof HRegionServer &&
          params[0] instanceof byte[] &&
          params[1] instanceof Operation) {
        // if the slow process is a query, we want to log its table as well 
        // as its own fingerprint
        byte [] tableName =
          HRegionInfo.parseRegionName((byte[]) params[0])[0];
        responseInfo.put("table", Bytes.toStringBinary(tableName));
        // annotate the response map with operation details
        responseInfo.putAll(((Operation) params[1]).toMap());
        // report to the log file
        LOG.warn("(operation" + tag + "): " +
            mapper.writeValueAsString(responseInfo));
      } else if (params.length == 1 && instance instanceof HRegionServer &&
          params[0] instanceof Operation) {
        // annotate the response map with operation details
        responseInfo.putAll(((Operation) params[0]).toMap());
        // report to the log file
        LOG.warn("(operation" + tag + "): " +
            mapper.writeValueAsString(responseInfo));
      } else {
        // can't get JSON details, so just report call.toString() along with 
        // a more generic tag.
        responseInfo.put("call", call.toString());
        LOG.warn("(response" + tag + "): " +
            mapper.writeValueAsString(responseInfo));
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
