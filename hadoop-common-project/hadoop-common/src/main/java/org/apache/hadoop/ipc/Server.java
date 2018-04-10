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

import static org.apache.hadoop.ipc.RpcConstants.AUTHORIZATION_FAILED_CALL_ID;
import static org.apache.hadoop.ipc.RpcConstants.CONNECTION_CONTEXT_CALL_ID;
import static org.apache.hadoop.ipc.RpcConstants.CURRENT_VERSION;
import static org.apache.hadoop.ipc.RpcConstants.HEADER_LEN_AFTER_HRPC_PART;
import static org.apache.hadoop.ipc.RpcConstants.PING_CALL_ID;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.ipc.CallQueueManager.CallQueueOverflowException;
import org.apache.hadoop.ipc.RPC.RpcInvoker;
import org.apache.hadoop.ipc.RPC.VersionMismatch;
import org.apache.hadoop.ipc.metrics.RpcDetailedMetrics;
import org.apache.hadoop.ipc.metrics.RpcMetrics;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcKindProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ProtoUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An abstract IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @see Client
 */
@Public
@InterfaceStability.Evolving
public abstract class Server {
  private final boolean authorize;
  private List<AuthMethod> enabledAuthMethods;
  private RpcSaslProto negotiateResponse;
  private ExceptionsHandler exceptionsHandler = new ExceptionsHandler();
  private Tracer tracer;
  /**
   * Logical name of the server used in metrics and monitor.
   */
  private final String serverName;
  
  /**
   * Add exception classes for which server won't log stack traces.
   *
   * @param exceptionClass exception classes
   */
  public void addTerseExceptions(Class<?>... exceptionClass) {
    exceptionsHandler.addTerseLoggingExceptions(exceptionClass);
  }

  /**
   * Add exception classes which server won't log at all.
   *
   * @param exceptionClass exception classes
   */
  public void addSuppressedLoggingExceptions(Class<?>... exceptionClass) {
    exceptionsHandler.addSuppressedLoggingExceptions(exceptionClass);
  }

  /**
   * ExceptionsHandler manages Exception groups for special handling
   * e.g., terse exception group for concise logging messages
   */
  static class ExceptionsHandler {
    private volatile Set<String> terseExceptions = new HashSet<>();
    private volatile Set<String> suppressedExceptions = new HashSet<>();

    /**
     * Add exception classes for which server won't log stack traces.
     * Optimized for infrequent invocation.
     * @param exceptionClass exception classes 
     */
    void addTerseLoggingExceptions(Class<?>... exceptionClass) {
      // Thread-safe replacement of terseExceptions.
      terseExceptions = addExceptions(terseExceptions, exceptionClass);
    }

    /**
     * Add exception classes which server won't log at all.
     * Optimized for infrequent invocation.
     * @param exceptionClass exception classes
     */
    void addSuppressedLoggingExceptions(Class<?>... exceptionClass) {
      // Thread-safe replacement of suppressedExceptions.
      suppressedExceptions = addExceptions(
          suppressedExceptions, exceptionClass);
    }

    boolean isTerseLog(Class<?> t) {
      return terseExceptions.contains(t.toString());
    }

    boolean isSuppressedLog(Class<?> t) {
      return suppressedExceptions.contains(t.toString());
    }

    /**
     * Return a new set containing all the exceptions in exceptionsSet
     * and exceptionClass.
     * @return
     */
    private static Set<String> addExceptions(
        final Set<String> exceptionsSet, Class<?>[] exceptionClass) {
      // Make a copy of the exceptionSet for performing modification
      final HashSet<String> newSet = new HashSet<>(exceptionsSet);

      // Add all class names into the HashSet
      for (Class<?> name : exceptionClass) {
        newSet.add(name.toString());
      }

      return Collections.unmodifiableSet(newSet);
    }
  }

  
  /**
   * If the user accidentally sends an HTTP GET to an IPC port, we detect this
   * and send back a nicer response.
   */
  private static final ByteBuffer HTTP_GET_BYTES = ByteBuffer.wrap(
      "GET ".getBytes(StandardCharsets.UTF_8));
  
  /**
   * An HTTP response to send back if we detect an HTTP request to our IPC
   * port.
   */
  static final String RECEIVED_HTTP_REQ_RESPONSE =
    "HTTP/1.1 404 Not Found\r\n" +
    "Content-type: text/plain\r\n\r\n" +
    "It looks like you are making an HTTP request to a Hadoop IPC port. " +
    "This is not the correct port for the web interface on this daemon.\r\n";

  /**
   * Initial and max size of response buffer
   */
  static int INITIAL_RESP_BUF_SIZE = 10240;
  
  static class RpcKindMapValue {
    final Class<? extends Writable> rpcRequestWrapperClass;
    final RpcInvoker rpcInvoker;

    RpcKindMapValue (Class<? extends Writable> rpcRequestWrapperClass,
          RpcInvoker rpcInvoker) {
      this.rpcInvoker = rpcInvoker;
      this.rpcRequestWrapperClass = rpcRequestWrapperClass;
    }   
  }
  static Map<RPC.RpcKind, RpcKindMapValue> rpcKindMap = new HashMap<>(4);
  
  

  /**
   * Register a RPC kind and the class to deserialize the rpc request.
   * 
   * Called by static initializers of rpcKind Engines
   * @param rpcKind
   * @param rpcRequestWrapperClass - this class is used to deserialze the
   *  the rpc request.
   *  @param rpcInvoker - use to process the calls on SS.
   */
  
  public static void registerProtocolEngine(RPC.RpcKind rpcKind, 
          Class<? extends Writable> rpcRequestWrapperClass,
          RpcInvoker rpcInvoker) {
    RpcKindMapValue  old = 
        rpcKindMap.put(rpcKind, new RpcKindMapValue(rpcRequestWrapperClass, rpcInvoker));
    if (old != null) {
      rpcKindMap.put(rpcKind, old);
      throw new IllegalArgumentException("ReRegistration of rpcKind: " +
          rpcKind);      
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("rpcKind=" + rpcKind +
          ", rpcRequestWrapperClass=" + rpcRequestWrapperClass +
          ", rpcInvoker=" + rpcInvoker);
    }
  }
  
  public Class<? extends Writable> getRpcRequestWrapper(
      RpcKindProto rpcKind) {
    if (rpcRequestClass != null)
       return rpcRequestClass;
    RpcKindMapValue val = rpcKindMap.get(ProtoUtil.convert(rpcKind));
    return (val == null) ? null : val.rpcRequestWrapperClass; 
  }
  
  public static RpcInvoker  getRpcInvoker(RPC.RpcKind rpcKind) {
    RpcKindMapValue val = rpcKindMap.get(rpcKind);
    return (val == null) ? null : val.rpcInvoker; 
  }
  

  public static final Logger LOG = LoggerFactory.getLogger(Server.class);
  public static final Logger AUDITLOG =
      LoggerFactory.getLogger("SecurityLogger."+Server.class.getName());
  private static final String AUTH_FAILED_FOR = "Auth failed for ";
  private static final String AUTH_SUCCESSFUL_FOR = "Auth successful for ";
  
  private static final ThreadLocal<Server> SERVER = new ThreadLocal<Server>();

  private static final Map<String, Class<?>> PROTOCOL_CACHE = 
    new ConcurrentHashMap<String, Class<?>>();
  
  static Class<?> getProtocolClass(String protocolName, Configuration conf) 
  throws ClassNotFoundException {
    Class<?> protocol = PROTOCOL_CACHE.get(protocolName);
    if (protocol == null) {
      protocol = conf.getClassByName(protocolName);
      PROTOCOL_CACHE.put(protocolName, protocol);
    }
    return protocol;
  }
  
  /** Returns the server instance called under or null.  May be called under
   * {@link #call(Writable, long)} implementations, and under {@link Writable}
   * methods of paramters and return values.  Permits applications to access
   * the server context.*/
  public static Server get() {
    return SERVER.get();
  }
 
  /** This is set to Call object before Handler invokes an RPC and reset
   * after the call returns.
   */
  private static final ThreadLocal<Call> CurCall = new ThreadLocal<Call>();
  
  /** Get the current call */
  @VisibleForTesting
  public static ThreadLocal<Call> getCurCall() {
    return CurCall;
  }
  
  /**
   * Returns the currently active RPC call's sequential ID number.  A negative
   * call ID indicates an invalid value, such as if there is no currently active
   * RPC call.
   * 
   * @return int sequential ID number of currently active RPC call
   */
  public static int getCallId() {
    Call call = CurCall.get();
    return call != null ? call.callId : RpcConstants.INVALID_CALL_ID;
  }
  
  /**
   * @return The current active RPC call's retry count. -1 indicates the retry
   *         cache is not supported in the client side.
   */
  public static int getCallRetryCount() {
    Call call = CurCall.get();
    return call != null ? call.retryCount : RpcConstants.INVALID_RETRY_COUNT;
  }

  /** Returns the remote side ip address when invoked inside an RPC 
   *  Returns null incase of an error.
   */
  public static InetAddress getRemoteIp() {
    Call call = CurCall.get();
    return (call != null ) ? call.getHostInetAddress() : null;
  }

  /**
   * Returns the clientId from the current RPC request
   */
  public static byte[] getClientId() {
    Call call = CurCall.get();
    return call != null ? call.clientId : RpcConstants.DUMMY_CLIENT_ID;
  }
  
  /** Returns remote address as a string when invoked inside an RPC.
   *  Returns null in case of an error.
   */
  public static String getRemoteAddress() {
    InetAddress addr = getRemoteIp();
    return (addr == null) ? null : addr.getHostAddress();
  }

  /** Returns the RPC remote user when invoked inside an RPC.  Note this
   *  may be different than the current user if called within another doAs
   *  @return connection's UGI or null if not an RPC
   */
  public static UserGroupInformation getRemoteUser() {
    Call call = CurCall.get();
    return (call != null) ? call.getRemoteUser() : null;
  }

  public static String getProtocol() {
    Call call = CurCall.get();
    return (call != null) ? call.getProtocol() : null;
  }

  /** Return true if the invocation was through an RPC.
   */
  public static boolean isRpcInvocation() {
    return CurCall.get() != null;
  }

  /**
   * Return the priority level assigned by call queue to an RPC
   * Returns 0 in case no priority is assigned.
   */
  public static int getPriorityLevel() {
    Call call = CurCall.get();
    return call != null? call.getPriorityLevel() : 0;
  }

  private String bindAddress; 
  private int port;                               // port we listen on
  private int handlerCount;                       // number of handler threads
  private int readThreads;                        // number of read threads
  private int readerPendingConnectionQueue;         // number of connections to queue per read thread
  private Class<? extends Writable> rpcRequestClass;   // class used for deserializing the rpc request
  final protected RpcMetrics rpcMetrics;
  final protected RpcDetailedMetrics rpcDetailedMetrics;
  
  private Configuration conf;
  private String portRangeConfig = null;
  private SecretManager<TokenIdentifier> secretManager;
  private SaslPropertiesResolver saslPropsResolver;
  private ServiceAuthorizationManager serviceAuthorizationManager = new ServiceAuthorizationManager();

  private int maxQueueSize;
  private final int maxRespSize;
  private final ThreadLocal<ResponseBuffer> responseBuffer =
      new ThreadLocal<ResponseBuffer>(){
        @Override
        protected ResponseBuffer initialValue() {
          return new ResponseBuffer(INITIAL_RESP_BUF_SIZE);
        }
  };
  private int socketSendBufferSize;
  private final int maxDataLength;
  private final boolean tcpNoDelay; // if T then disable Nagle's Algorithm

  volatile private boolean running = true;         // true while server runs
  private CallQueueManager<Call> callQueue;

  // maintains the set of client connections and handles idle timeouts
  private ConnectionManager connectionManager;
  private Listener listener = null;
  private Responder responder = null;
  private Handler[] handlers = null;

  private boolean logSlowRPC = false;

  /**
   * Checks if LogSlowRPC is set true.
   * @return true, if LogSlowRPC is set true, false, otherwise.
   */
  protected boolean isLogSlowRPC() {
    return logSlowRPC;
  }

  /**
   * Sets slow RPC flag.
   * @param logSlowRPCFlag
   */
  @VisibleForTesting
  protected void setLogSlowRPC(boolean logSlowRPCFlag) {
    this.logSlowRPC = logSlowRPCFlag;
  }


  /**
   * Logs a Slow RPC Request.
   *
   * @param methodName - RPC Request method name
   * @param processingTime - Processing Time.
   *
   * if this request took too much time relative to other requests
   * we consider that as a slow RPC. 3 is a magic number that comes
   * from 3 sigma deviation. A very simple explanation can be found
   * by searching for 68-95-99.7 rule. We flag an RPC as slow RPC
   * if and only if it falls above 99.7% of requests. We start this logic
   * only once we have enough sample size.
   */
  void logSlowRpcCalls(String methodName, int processingTime) {
    final int deviation = 3;

    // 1024 for minSampleSize just a guess -- not a number computed based on
    // sample size analysis. It is chosen with the hope that this
    // number is high enough to avoid spurious logging, yet useful
    // in practice.
    final int minSampleSize = 1024;
    final double threeSigma = rpcMetrics.getProcessingMean() +
        (rpcMetrics.getProcessingStdDev() * deviation);

    if ((rpcMetrics.getProcessingSampleCount() > minSampleSize) &&
        (processingTime > threeSigma)) {
      if(LOG.isWarnEnabled()) {
        String client = CurCall.get().toString();
        LOG.warn(
            "Slow RPC : " + methodName + " took " + processingTime +
                " milliseconds to process from client " + client);
      }
      rpcMetrics.incrSlowRpc();
    }
  }

  void updateMetrics(String name, int queueTime, int processingTime,
                     boolean deferredCall) {
    rpcMetrics.addRpcQueueTime(queueTime);
    if (!deferredCall) {
      rpcMetrics.addRpcProcessingTime(processingTime);
      rpcDetailedMetrics.addProcessingTime(name, processingTime);
      callQueue.addResponseTime(name, getPriorityLevel(), queueTime,
          processingTime);
      if (isLogSlowRPC()) {
        logSlowRpcCalls(name, processingTime);
      }
    }
  }

  void updateDeferredMetrics(String name, long processingTime) {
    rpcMetrics.addDeferredRpcProcessingTime(processingTime);
    rpcDetailedMetrics.addDeferredProcessingTime(name, processingTime);
  }

  /**
   * A convenience method to bind to a given address and report 
   * better exceptions if the address is not a valid host.
   * @param socket the socket to bind
   * @param address the address to bind to
   * @param backlog the number of connections allowed in the queue
   * @throws BindException if the address can't be bound
   * @throws UnknownHostException if the address isn't a valid host name
   * @throws IOException other random errors from bind
   */
  public static void bind(ServerSocket socket, InetSocketAddress address, 
                          int backlog) throws IOException {
    bind(socket, address, backlog, null, null);
  }

  public static void bind(ServerSocket socket, InetSocketAddress address, 
      int backlog, Configuration conf, String rangeConf) throws IOException {
    try {
      IntegerRanges range = null;
      if (rangeConf != null) {
        range = conf.getRange(rangeConf, "");
      }
      if (range == null || range.isEmpty() || (address.getPort() != 0)) {
        socket.bind(address, backlog);
      } else {
        for (Integer port : range) {
          if (socket.isBound()) break;
          try {
            InetSocketAddress temp = new InetSocketAddress(address.getAddress(),
                port);
            socket.bind(temp, backlog);
          } catch(BindException e) {
            //Ignored
          }
        }
        if (!socket.isBound()) {
          throw new BindException("Could not find a free port in "+range);
        }
      }
    } catch (SocketException e) {
      throw NetUtils.wrapException(null,
          0,
          address.getHostName(),
          address.getPort(), e);
    }
  }
  
  /**
   * Returns a handle to the rpcMetrics (required in tests)
   * @return rpc metrics
   */
  @VisibleForTesting
  public RpcMetrics getRpcMetrics() {
    return rpcMetrics;
  }

  @VisibleForTesting
  public RpcDetailedMetrics getRpcDetailedMetrics() {
    return rpcDetailedMetrics;
  }
  
  @VisibleForTesting
  Iterable<? extends Thread> getHandlers() {
    return Arrays.asList(handlers);
  }

  @VisibleForTesting
  Connection[] getConnections() {
    return connectionManager.toArray();
  }

  /**
   * Refresh the service authorization ACL for the service handled by this server.
   */
  public void refreshServiceAcl(Configuration conf, PolicyProvider provider) {
    serviceAuthorizationManager.refresh(conf, provider);
  }

  /**
   * Refresh the service authorization ACL for the service handled by this server
   * using the specified Configuration.
   */
  @Private
  public void refreshServiceAclWithLoadedConfiguration(Configuration conf,
      PolicyProvider provider) {
    serviceAuthorizationManager.refreshWithLoadedConfiguration(conf, provider);
  }
  /**
   * Returns a handle to the serviceAuthorizationManager (required in tests)
   * @return instance of ServiceAuthorizationManager for this server
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  public ServiceAuthorizationManager getServiceAuthorizationManager() {
    return serviceAuthorizationManager;
  }

  private String getQueueClassPrefix() {
    return CommonConfigurationKeys.IPC_NAMESPACE + "." + port;
  }

  static Class<? extends BlockingQueue<Call>> getQueueClass(
      String prefix, Configuration conf) {
    String name = prefix + "." + CommonConfigurationKeys.IPC_CALLQUEUE_IMPL_KEY;
    Class<?> queueClass = conf.getClass(name, LinkedBlockingQueue.class);
    return CallQueueManager.convertQueueClass(queueClass, Call.class);
  }

  static Class<? extends RpcScheduler> getSchedulerClass(
      String prefix, Configuration conf) {
    String schedulerKeyname = prefix + "." + CommonConfigurationKeys
        .IPC_SCHEDULER_IMPL_KEY;
    Class<?> schedulerClass = conf.getClass(schedulerKeyname, null);
    // Patch the configuration for legacy fcq configuration that does not have
    // a separate scheduler setting
    if (schedulerClass == null) {
      String queueKeyName = prefix + "." + CommonConfigurationKeys
          .IPC_CALLQUEUE_IMPL_KEY;
      Class<?> queueClass = conf.getClass(queueKeyName, null);
      if (queueClass != null) {
        if (queueClass.getCanonicalName().equals(
            FairCallQueue.class.getCanonicalName())) {
          conf.setClass(schedulerKeyname, DecayRpcScheduler.class,
              RpcScheduler.class);
        }
      }
    }
    schedulerClass = conf.getClass(schedulerKeyname,
        DefaultRpcScheduler.class);

    return CallQueueManager.convertSchedulerClass(schedulerClass);
  }

  /*
   * Refresh the call queue
   */
  public synchronized void refreshCallQueue(Configuration conf) {
    // Create the next queue
    String prefix = getQueueClassPrefix();
    this.maxQueueSize = handlerCount * conf.getInt(
        CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_KEY,
        CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT);
    callQueue.swapQueue(getSchedulerClass(prefix, conf),
        getQueueClass(prefix, conf), maxQueueSize, prefix, conf);
  }

  /**
   * Get from config if client backoff is enabled on that port.
   */
  static boolean getClientBackoffEnable(
      String prefix, Configuration conf) {
    String name = prefix + "." +
        CommonConfigurationKeys.IPC_BACKOFF_ENABLE;
    return conf.getBoolean(name,
        CommonConfigurationKeys.IPC_BACKOFF_ENABLE_DEFAULT);
  }

  /** A generic call queued for handling. */
  public static class Call implements Schedulable,
  PrivilegedExceptionAction<Void> {
    final int callId;            // the client's call id
    final int retryCount;        // the retry count of the call
    long timestamp;              // time received when response is null
                                 // time served when response is not null
    private AtomicInteger responseWaitCount = new AtomicInteger(1);
    final RPC.RpcKind rpcKind;
    final byte[] clientId;
    private final TraceScope traceScope; // the HTrace scope on the server side
    private final CallerContext callerContext; // the call context
    private boolean deferredResponse = false;
    private int priorityLevel;
    // the priority level assigned by scheduler, 0 by default

    Call() {
      this(RpcConstants.INVALID_CALL_ID, RpcConstants.INVALID_RETRY_COUNT,
        RPC.RpcKind.RPC_BUILTIN, RpcConstants.DUMMY_CLIENT_ID);
    }

    Call(Call call) {
      this(call.callId, call.retryCount, call.rpcKind, call.clientId,
          call.traceScope, call.callerContext);
    }

    Call(int id, int retryCount, RPC.RpcKind kind, byte[] clientId) {
      this(id, retryCount, kind, clientId, null, null);
    }

    @VisibleForTesting // primarily TestNamenodeRetryCache
    public Call(int id, int retryCount, Void ignore1, Void ignore2,
        RPC.RpcKind kind, byte[] clientId) {
      this(id, retryCount, kind, clientId, null, null);
    }

    Call(int id, int retryCount, RPC.RpcKind kind, byte[] clientId,
        TraceScope traceScope, CallerContext callerContext) {
      this.callId = id;
      this.retryCount = retryCount;
      this.timestamp = Time.now();
      this.rpcKind = kind;
      this.clientId = clientId;
      this.traceScope = traceScope;
      this.callerContext = callerContext;
    }

    @Override
    public String toString() {
      return "Call#" + callId + " Retry#" + retryCount;
    }

    @Override
    public Void run() throws Exception {
      return null;
    }
    // should eventually be abstract but need to avoid breaking tests
    public UserGroupInformation getRemoteUser() {
      return null;
    }
    public InetAddress getHostInetAddress() {
      return null;
    }
    public String getHostAddress() {
      InetAddress addr = getHostInetAddress();
      return (addr != null) ? addr.getHostAddress() : null;
    }

    public String getProtocol() {
      return null;
    }

    /**
     * Allow a IPC response to be postponed instead of sent immediately
     * after the handler returns from the proxy method.  The intended use
     * case is freeing up the handler thread when the response is known,
     * but an expensive pre-condition must be satisfied before it's sent
     * to the client.
     */
    @InterfaceStability.Unstable
    @InterfaceAudience.LimitedPrivate({"HDFS"})
    public final void postponeResponse() {
      int count = responseWaitCount.incrementAndGet();
      assert count > 0 : "response has already been sent";
    }

    @InterfaceStability.Unstable
    @InterfaceAudience.LimitedPrivate({"HDFS"})
    public final void sendResponse() throws IOException {
      int count = responseWaitCount.decrementAndGet();
      assert count >= 0 : "response has already been sent";
      if (count == 0) {
        doResponse(null);
      }
    }

    @InterfaceStability.Unstable
    @InterfaceAudience.LimitedPrivate({"HDFS"})
    public final void abortResponse(Throwable t) throws IOException {
      // don't send response if the call was already sent or aborted.
      if (responseWaitCount.getAndSet(-1) > 0) {
        doResponse(t);
      }
    }

    void doResponse(Throwable t) throws IOException {}

    // For Schedulable
    @Override
    public UserGroupInformation getUserGroupInformation() {
      return getRemoteUser();
    }

    @Override
    public int getPriorityLevel() {
      return this.priorityLevel;
    }

    public void setPriorityLevel(int priorityLevel) {
      this.priorityLevel = priorityLevel;
    }

    @InterfaceStability.Unstable
    public void deferResponse() {
      this.deferredResponse = true;
    }

    @InterfaceStability.Unstable
    public boolean isResponseDeferred() {
      return this.deferredResponse;
    }

    public void setDeferredResponse(Writable response) {
    }

    public void setDeferredError(Throwable t) {
    }
  }

  /** A RPC extended call queued for handling. */
  private class RpcCall extends Call {
    final Connection connection;  // connection to client
    final Writable rpcRequest;    // Serialized Rpc request from client
    ByteBuffer rpcResponse;       // the response for this call

    RpcCall(RpcCall call) {
      super(call);
      this.connection = call.connection;
      this.rpcRequest = call.rpcRequest;
    }

    RpcCall(Connection connection, int id) {
      this(connection, id, RpcConstants.INVALID_RETRY_COUNT);
    }

    RpcCall(Connection connection, int id, int retryCount) {
      this(connection, id, retryCount, null,
          RPC.RpcKind.RPC_BUILTIN, RpcConstants.DUMMY_CLIENT_ID,
          null, null);
    }

    RpcCall(Connection connection, int id, int retryCount,
        Writable param, RPC.RpcKind kind, byte[] clientId,
        TraceScope traceScope, CallerContext context) {
      super(id, retryCount, kind, clientId, traceScope, context);
      this.connection = connection;
      this.rpcRequest = param;
    }

    @Override
    public String getProtocol() {
      return "rpc";
    }

    @Override
    public UserGroupInformation getRemoteUser() {
      return connection.user;
    }

    @Override
    public InetAddress getHostInetAddress() {
      return connection.getHostInetAddress();
    }

    @Override
    public Void run() throws Exception {
      if (!connection.channel.isOpen()) {
        Server.LOG.info(Thread.currentThread().getName() + ": skipped " + this);
        return null;
      }
      Writable value = null;
      ResponseParams responseParams = new ResponseParams();

      try {
        value = call(
            rpcKind, connection.protocolName, rpcRequest, timestamp);
      } catch (Throwable e) {
        populateResponseParamsOnError(e, responseParams);
      }
      if (!isResponseDeferred()) {
        setupResponse(this, responseParams.returnStatus,
            responseParams.detailedErr,
            value, responseParams.errorClass, responseParams.error);
        sendResponse();
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Deferring response for callId: " + this.callId);
        }
      }
      return null;
    }

    /**
     * @param t              the {@link java.lang.Throwable} to use to set
     *                       errorInfo
     * @param responseParams the {@link ResponseParams} instance to populate
     */
    private void populateResponseParamsOnError(Throwable t,
                                               ResponseParams responseParams) {
      if (t instanceof UndeclaredThrowableException) {
        t = t.getCause();
      }
      logException(Server.LOG, t, this);
      if (t instanceof RpcServerException) {
        RpcServerException rse = ((RpcServerException) t);
        responseParams.returnStatus = rse.getRpcStatusProto();
        responseParams.detailedErr = rse.getRpcErrorCodeProto();
      } else {
        responseParams.returnStatus = RpcStatusProto.ERROR;
        responseParams.detailedErr = RpcErrorCodeProto.ERROR_APPLICATION;
      }
      responseParams.errorClass = t.getClass().getName();
      responseParams.error = StringUtils.stringifyException(t);
      // Remove redundant error class name from the beginning of the
      // stack trace
      String exceptionHdr = responseParams.errorClass + ": ";
      if (responseParams.error.startsWith(exceptionHdr)) {
        responseParams.error =
            responseParams.error.substring(exceptionHdr.length());
      }
    }

    void setResponse(ByteBuffer response) throws IOException {
      this.rpcResponse = response;
    }

    @Override
    void doResponse(Throwable t) throws IOException {
      RpcCall call = this;
      if (t != null) {
        // clone the call to prevent a race with another thread stomping
        // on the response while being sent.  the original call is
        // effectively discarded since the wait count won't hit zero
        call = new RpcCall(this);
        setupResponse(call,
            RpcStatusProto.FATAL, RpcErrorCodeProto.ERROR_RPC_SERVER,
            null, t.getClass().getName(), StringUtils.stringifyException(t));
      }
      connection.sendResponse(call);
    }

    /**
     * Send a deferred response, ignoring errors.
     */
    private void sendDeferedResponse() {
      try {
        connection.sendResponse(this);
      } catch (Exception e) {
        // For synchronous calls, application code is done once it's returned
        // from a method. It does not expect to receive an error.
        // This is equivalent to what happens in synchronous calls when the
        // Responder is not able to send out the response.
        LOG.error("Failed to send deferred response. ThreadName=" + Thread
            .currentThread().getName() + ", CallId="
            + callId + ", hostname=" + getHostAddress());
      }
    }

    @Override
    public void setDeferredResponse(Writable response) {
      if (this.connection.getServer().running) {
        try {
          setupResponse(this, RpcStatusProto.SUCCESS, null, response,
              null, null);
        } catch (IOException e) {
          // For synchronous calls, application code is done once it has
          // returned from a method. It does not expect to receive an error.
          // This is equivalent to what happens in synchronous calls when the
          // response cannot be sent.
          LOG.error(
              "Failed to setup deferred successful response. ThreadName=" +
                  Thread.currentThread().getName() + ", Call=" + this);
          return;
        }
        sendDeferedResponse();
      }
    }

    @Override
    public void setDeferredError(Throwable t) {
      if (this.connection.getServer().running) {
        if (t == null) {
          t = new IOException(
              "User code indicated an error without an exception");
        }
        try {
          ResponseParams responseParams = new ResponseParams();
          populateResponseParamsOnError(t, responseParams);
          setupResponse(this, responseParams.returnStatus,
              responseParams.detailedErr,
              null, responseParams.errorClass, responseParams.error);
        } catch (IOException e) {
          // For synchronous calls, application code is done once it has
          // returned from a method. It does not expect to receive an error.
          // This is equivalent to what happens in synchronous calls when the
          // response cannot be sent.
          LOG.error(
              "Failed to setup deferred error response. ThreadName=" +
                  Thread.currentThread().getName() + ", Call=" + this);
        }
        sendDeferedResponse();
      }
    }

    /**
     * Holds response parameters. Defaults set to work for successful
     * invocations
     */
    private class ResponseParams {
      String errorClass = null;
      String error = null;
      RpcErrorCodeProto detailedErr = null;
      RpcStatusProto returnStatus = RpcStatusProto.SUCCESS;
    }

    @Override
    public String toString() {
      return super.toString() + " " + rpcRequest + " from " + connection;
    }
  }

  /** Listens on the socket. Creates jobs for the handler threads*/
  private class Listener extends Thread {
    
    private ServerSocketChannel acceptChannel = null; //the accept channel
    private Selector selector = null; //the selector that we use for the server
    private Reader[] readers = null;
    private int currentReader = 0;
    private InetSocketAddress address; //the address we bind at
    private int backlogLength = conf.getInt(
        CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_KEY,
        CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT);
    
    public Listener() throws IOException {
      address = new InetSocketAddress(bindAddress, port);
      // Create a new server socket and set to non blocking mode
      acceptChannel = ServerSocketChannel.open();
      acceptChannel.configureBlocking(false);

      // Bind the server socket to the local host and port
      bind(acceptChannel.socket(), address, backlogLength, conf, portRangeConfig);
      port = acceptChannel.socket().getLocalPort(); //Could be an ephemeral port
      // create a selector;
      selector= Selector.open();
      readers = new Reader[readThreads];
      for (int i = 0; i < readThreads; i++) {
        Reader reader = new Reader(
            "Socket Reader #" + (i + 1) + " for port " + port);
        readers[i] = reader;
        reader.start();
      }

      // Register accepts on the server socket with the selector.
      acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
      this.setName("IPC Server listener on " + port);
      this.setDaemon(true);
    }
    
    private class Reader extends Thread {
      final private BlockingQueue<Connection> pendingConnections;
      private final Selector readSelector;

      Reader(String name) throws IOException {
        super(name);

        this.pendingConnections =
            new LinkedBlockingQueue<Connection>(readerPendingConnectionQueue);
        this.readSelector = Selector.open();
      }
      
      @Override
      public void run() {
        LOG.info("Starting " + Thread.currentThread().getName());
        try {
          doRunLoop();
        } finally {
          try {
            readSelector.close();
          } catch (IOException ioe) {
            LOG.error("Error closing read selector in " + Thread.currentThread().getName(), ioe);
          }
        }
      }

      private synchronized void doRunLoop() {
        while (running) {
          SelectionKey key = null;
          try {
            // consume as many connections as currently queued to avoid
            // unbridled acceptance of connections that starves the select
            int size = pendingConnections.size();
            for (int i=size; i>0; i--) {
              Connection conn = pendingConnections.take();
              conn.channel.register(readSelector, SelectionKey.OP_READ, conn);
            }
            readSelector.select();

            Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
            while (iter.hasNext()) {
              key = iter.next();
              iter.remove();
              try {
                if (key.isReadable()) {
                  doRead(key);
                }
              } catch (CancelledKeyException cke) {
                // something else closed the connection, ex. responder or
                // the listener doing an idle scan.  ignore it and let them
                // clean up.
                LOG.info(Thread.currentThread().getName() +
                    ": connection aborted from " + key.attachment());
              }
              key = null;
            }
          } catch (InterruptedException e) {
            if (running) {                      // unexpected -- log it
              LOG.info(Thread.currentThread().getName() + " unexpectedly interrupted", e);
            }
          } catch (IOException ex) {
            LOG.error("Error in Reader", ex);
          } catch (Throwable re) {
            LOG.error("Bug in read selector!", re);
            ExitUtil.terminate(1, "Bug in read selector!");
          }
        }
      }

      /**
       * Updating the readSelector while it's being used is not thread-safe,
       * so the connection must be queued.  The reader will drain the queue
       * and update its readSelector before performing the next select
       */
      public void addConnection(Connection conn) throws InterruptedException {
        pendingConnections.put(conn);
        readSelector.wakeup();
      }

      void shutdown() {
        assert !running;
        readSelector.wakeup();
        try {
          super.interrupt();
          super.join();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }

    @Override
    public void run() {
      LOG.info(Thread.currentThread().getName() + ": starting");
      SERVER.set(Server.this);
      connectionManager.startIdleScan();
      while (running) {
        SelectionKey key = null;
        try {
          getSelector().select();
          Iterator<SelectionKey> iter = getSelector().selectedKeys().iterator();
          while (iter.hasNext()) {
            key = iter.next();
            iter.remove();
            try {
              if (key.isValid()) {
                if (key.isAcceptable())
                  doAccept(key);
              }
            } catch (IOException e) {
            }
            key = null;
          }
        } catch (OutOfMemoryError e) {
          // we can run out of memory if we have too many threads
          // log the event and sleep for a minute and give 
          // some thread(s) a chance to finish
          LOG.warn("Out of Memory in server select", e);
          closeCurrentConnection(key, e);
          connectionManager.closeIdle(true);
          try { Thread.sleep(60000); } catch (Exception ie) {}
        } catch (Exception e) {
          closeCurrentConnection(key, e);
        }
      }
      LOG.info("Stopping " + Thread.currentThread().getName());

      synchronized (this) {
        try {
          acceptChannel.close();
          selector.close();
        } catch (IOException e) { }

        selector= null;
        acceptChannel= null;
        
        // close all connections
        connectionManager.stopIdleScan();
        connectionManager.closeAll();
      }
    }

    private void closeCurrentConnection(SelectionKey key, Throwable e) {
      if (key != null) {
        Connection c = (Connection)key.attachment();
        if (c != null) {
          closeConnection(c);
          c = null;
        }
      }
    }

    InetSocketAddress getAddress() {
      return (InetSocketAddress)acceptChannel.socket().getLocalSocketAddress();
    }
    
    void doAccept(SelectionKey key) throws InterruptedException, IOException,  OutOfMemoryError {
      ServerSocketChannel server = (ServerSocketChannel) key.channel();
      SocketChannel channel;
      while ((channel = server.accept()) != null) {

        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(tcpNoDelay);
        channel.socket().setKeepAlive(true);
        
        Reader reader = getReader();
        Connection c = connectionManager.register(channel);
        // If the connectionManager can't take it, close the connection.
        if (c == null) {
          if (channel.isOpen()) {
            IOUtils.cleanup(null, channel);
          }
          connectionManager.droppedConnections.getAndIncrement();
          continue;
        }
        key.attach(c);  // so closeCurrentConnection can get the object
        reader.addConnection(c);
      }
    }

    void doRead(SelectionKey key) throws InterruptedException {
      int count;
      Connection c = (Connection)key.attachment();
      if (c == null) {
        return;  
      }
      c.setLastContact(Time.now());
      
      try {
        count = c.readAndProcess();
      } catch (InterruptedException ieo) {
        LOG.info(Thread.currentThread().getName() + ": readAndProcess caught InterruptedException", ieo);
        throw ieo;
      } catch (Exception e) {
        // Any exceptions that reach here are fatal unexpected internal errors
        // that could not be sent to the client.
        LOG.info(Thread.currentThread().getName() +
            ": readAndProcess from client " + c +
            " threw exception [" + e + "]", e);
        count = -1; //so that the (count < 0) block is executed
      }
      // setupResponse will signal the connection should be closed when a
      // fatal response is sent.
      if (count < 0 || c.shouldClose()) {
        closeConnection(c);
        c = null;
      }
      else {
        c.setLastContact(Time.now());
      }
    }   

    synchronized void doStop() {
      if (selector != null) {
        selector.wakeup();
        Thread.yield();
      }
      if (acceptChannel != null) {
        try {
          acceptChannel.socket().close();
        } catch (IOException e) {
          LOG.info(Thread.currentThread().getName() + ":Exception in closing listener socket. " + e);
        }
      }
      for (Reader r : readers) {
        r.shutdown();
      }
    }
    
    synchronized Selector getSelector() { return selector; }
    // The method that will return the next reader to work with
    // Simplistic implementation of round robin for now
    Reader getReader() {
      currentReader = (currentReader + 1) % readers.length;
      return readers[currentReader];
    }
  }

  // Sends responses of RPC back to clients.
  private class Responder extends Thread {
    private final Selector writeSelector;
    private int pending;         // connections waiting to register
    
    final static int PURGE_INTERVAL = 900000; // 15mins

    Responder() throws IOException {
      this.setName("IPC Server Responder");
      this.setDaemon(true);
      writeSelector = Selector.open(); // create a selector
      pending = 0;
    }

    @Override
    public void run() {
      LOG.info(Thread.currentThread().getName() + ": starting");
      SERVER.set(Server.this);
      try {
        doRunLoop();
      } finally {
        LOG.info("Stopping " + Thread.currentThread().getName());
        try {
          writeSelector.close();
        } catch (IOException ioe) {
          LOG.error("Couldn't close write selector in " + Thread.currentThread().getName(), ioe);
        }
      }
    }
    
    private void doRunLoop() {
      long lastPurgeTime = 0;   // last check for old calls.

      while (running) {
        try {
          waitPending();     // If a channel is being registered, wait.
          writeSelector.select(PURGE_INTERVAL);
          Iterator<SelectionKey> iter = writeSelector.selectedKeys().iterator();
          while (iter.hasNext()) {
            SelectionKey key = iter.next();
            iter.remove();
            try {
              if (key.isWritable()) {
                doAsyncWrite(key);
              }
            } catch (CancelledKeyException cke) {
              // something else closed the connection, ex. reader or the
              // listener doing an idle scan.  ignore it and let them clean
              // up
              RpcCall call = (RpcCall)key.attachment();
              if (call != null) {
                LOG.info(Thread.currentThread().getName() +
                    ": connection aborted from " + call.connection);
              }
            } catch (IOException e) {
              LOG.info(Thread.currentThread().getName() + ": doAsyncWrite threw exception " + e);
            }
          }
          long now = Time.now();
          if (now < lastPurgeTime + PURGE_INTERVAL) {
            continue;
          }
          lastPurgeTime = now;
          //
          // If there were some calls that have not been sent out for a
          // long time, discard them.
          //
          if(LOG.isDebugEnabled()) {
            LOG.debug("Checking for old call responses.");
          }
          ArrayList<RpcCall> calls;
          
          // get the list of channels from list of keys.
          synchronized (writeSelector.keys()) {
            calls = new ArrayList<RpcCall>(writeSelector.keys().size());
            iter = writeSelector.keys().iterator();
            while (iter.hasNext()) {
              SelectionKey key = iter.next();
              RpcCall call = (RpcCall)key.attachment();
              if (call != null && key.channel() == call.connection.channel) { 
                calls.add(call);
              }
            }
          }

          for (RpcCall call : calls) {
            doPurge(call, now);
          }
        } catch (OutOfMemoryError e) {
          //
          // we can run out of memory if we have too many threads
          // log the event and sleep for a minute and give
          // some thread(s) a chance to finish
          //
          LOG.warn("Out of Memory in server select", e);
          try { Thread.sleep(60000); } catch (Exception ie) {}
        } catch (Exception e) {
          LOG.warn("Exception in Responder", e);
        }
      }
    }

    private void doAsyncWrite(SelectionKey key) throws IOException {
      RpcCall call = (RpcCall)key.attachment();
      if (call == null) {
        return;
      }
      if (key.channel() != call.connection.channel) {
        throw new IOException("doAsyncWrite: bad channel");
      }

      synchronized(call.connection.responseQueue) {
        if (processResponse(call.connection.responseQueue, false)) {
          try {
            key.interestOps(0);
          } catch (CancelledKeyException e) {
            /* The Listener/reader might have closed the socket.
             * We don't explicitly cancel the key, so not sure if this will
             * ever fire.
             * This warning could be removed.
             */
            LOG.warn("Exception while changing ops : " + e);
          }
        }
      }
    }

    //
    // Remove calls that have been pending in the responseQueue 
    // for a long time.
    //
    private void doPurge(RpcCall call, long now) {
      LinkedList<RpcCall> responseQueue = call.connection.responseQueue;
      synchronized (responseQueue) {
        Iterator<RpcCall> iter = responseQueue.listIterator(0);
        while (iter.hasNext()) {
          call = iter.next();
          if (now > call.timestamp + PURGE_INTERVAL) {
            closeConnection(call.connection);
            break;
          }
        }
      }
    }

    // Processes one response. Returns true if there are no more pending
    // data for this channel.
    //
    private boolean processResponse(LinkedList<RpcCall> responseQueue,
                                    boolean inHandler) throws IOException {
      boolean error = true;
      boolean done = false;       // there is more data for this channel.
      int numElements = 0;
      RpcCall call = null;
      try {
        synchronized (responseQueue) {
          //
          // If there are no items for this channel, then we are done
          //
          numElements = responseQueue.size();
          if (numElements == 0) {
            error = false;
            return true;              // no more data for this channel.
          }
          //
          // Extract the first call
          //
          call = responseQueue.removeFirst();
          SocketChannel channel = call.connection.channel;
          if (LOG.isDebugEnabled()) {
            LOG.debug(Thread.currentThread().getName() + ": responding to " + call);
          }
          //
          // Send as much data as we can in the non-blocking fashion
          //
          int numBytes = channelWrite(channel, call.rpcResponse);
          if (numBytes < 0) {
            return true;
          }
          if (!call.rpcResponse.hasRemaining()) {
            //Clear out the response buffer so it can be collected
            call.rpcResponse = null;
            call.connection.decRpcCount();
            if (numElements == 1) {    // last call fully processes.
              done = true;             // no more data for this channel.
            } else {
              done = false;            // more calls pending to be sent.
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(Thread.currentThread().getName() + ": responding to " + call
                  + " Wrote " + numBytes + " bytes.");
            }
          } else {
            //
            // If we were unable to write the entire response out, then 
            // insert in Selector queue. 
            //
            call.connection.responseQueue.addFirst(call);
            
            if (inHandler) {
              // set the serve time when the response has to be sent later
              call.timestamp = Time.now();
              
              incPending();
              try {
                // Wakeup the thread blocked on select, only then can the call 
                // to channel.register() complete.
                writeSelector.wakeup();
                channel.register(writeSelector, SelectionKey.OP_WRITE, call);
              } catch (ClosedChannelException e) {
                //Its ok. channel might be closed else where.
                done = true;
              } finally {
                decPending();
              }
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(Thread.currentThread().getName() + ": responding to " + call
                  + " Wrote partial " + numBytes + " bytes.");
            }
          }
          error = false;              // everything went off well
        }
      } finally {
        if (error && call != null) {
          LOG.warn(Thread.currentThread().getName()+", call " + call + ": output error");
          done = true;               // error. no more data for this channel.
          closeConnection(call.connection);
        }
      }
      return done;
    }

    //
    // Enqueue a response from the application.
    //
    void doRespond(RpcCall call) throws IOException {
      synchronized (call.connection.responseQueue) {
        // must only wrap before adding to the responseQueue to prevent
        // postponed responses from being encrypted and sent out of order.
        if (call.connection.useWrap) {
          wrapWithSasl(call);
        }
        call.connection.responseQueue.addLast(call);
        if (call.connection.responseQueue.size() == 1) {
          processResponse(call.connection.responseQueue, true);
        }
      }
    }

    private synchronized void incPending() {   // call waiting to be enqueued.
      pending++;
    }

    private synchronized void decPending() { // call done enqueueing.
      pending--;
      notify();
    }

    private synchronized void waitPending() throws InterruptedException {
      while (pending > 0) {
        wait();
      }
    }
  }

  @InterfaceAudience.Private
  public enum AuthProtocol {
    NONE(0),
    SASL(-33);
    
    public final int callId;
    AuthProtocol(int callId) {
      this.callId = callId;
    }
    
    static AuthProtocol valueOf(int callId) {
      for (AuthProtocol authType : AuthProtocol.values()) {
        if (authType.callId == callId) {
          return authType;
        }
      }
      return null;
    }
  };
  
  /**
   * Wrapper for RPC IOExceptions to be returned to the client.  Used to
   * let exceptions bubble up to top of processOneRpc where the correct
   * callId can be associated with the response.  Also used to prevent
   * unnecessary stack trace logging if it's not an internal server error. 
   */
  @SuppressWarnings("serial")
  private static class FatalRpcServerException extends RpcServerException {
    private final RpcErrorCodeProto errCode;
    public FatalRpcServerException(RpcErrorCodeProto errCode, IOException ioe) {
      super(ioe.toString(), ioe);
      this.errCode = errCode;
    }
    public FatalRpcServerException(RpcErrorCodeProto errCode, String message) {
      this(errCode, new RpcServerException(message));
    }
    @Override
    public RpcStatusProto getRpcStatusProto() {
      return RpcStatusProto.FATAL;
    }
    @Override
    public RpcErrorCodeProto getRpcErrorCodeProto() {
      return errCode;
    }
    @Override
    public String toString() {
      return getCause().toString();
    }
  }

  /** Reads calls from a connection and queues them for handling. */
  public class Connection {
    private boolean connectionHeaderRead = false; // connection  header is read?
    private boolean connectionContextRead = false; //if connection context that
                                            //follows connection header is read

    private SocketChannel channel;
    private ByteBuffer data;
    private ByteBuffer dataLengthBuffer;
    private LinkedList<RpcCall> responseQueue;
    // number of outstanding rpcs
    private AtomicInteger rpcCount = new AtomicInteger();
    private long lastContact;
    private int dataLength;
    private Socket socket;
    // Cache the remote host & port info so that even if the socket is 
    // disconnected, we can say where it used to connect to.
    private String hostAddress;
    private int remotePort;
    private InetAddress addr;
    
    IpcConnectionContextProto connectionContext;
    String protocolName;
    SaslServer saslServer;
    private AuthMethod authMethod;
    private AuthProtocol authProtocol;
    private boolean saslContextEstablished;
    private ByteBuffer connectionHeaderBuf = null;
    private ByteBuffer unwrappedData;
    private ByteBuffer unwrappedDataLengthBuffer;
    private int serviceClass;
    private boolean shouldClose = false;

    UserGroupInformation user = null;
    public UserGroupInformation attemptingUser = null; // user name before auth

    // Fake 'call' for failed authorization response
    private final RpcCall authFailedCall =
        new RpcCall(this, AUTHORIZATION_FAILED_CALL_ID);

    private boolean sentNegotiate = false;
    private boolean useWrap = false;
    
    public Connection(SocketChannel channel, long lastContact) {
      this.channel = channel;
      this.lastContact = lastContact;
      this.data = null;
      
      // the buffer is initialized to read the "hrpc" and after that to read
      // the length of the Rpc-packet (i.e 4 bytes)
      this.dataLengthBuffer = ByteBuffer.allocate(4);
      this.unwrappedData = null;
      this.unwrappedDataLengthBuffer = ByteBuffer.allocate(4);
      this.socket = channel.socket();
      this.addr = socket.getInetAddress();
      if (addr == null) {
        this.hostAddress = "*Unknown*";
      } else {
        this.hostAddress = addr.getHostAddress();
      }
      this.remotePort = socket.getPort();
      this.responseQueue = new LinkedList<RpcCall>();
      if (socketSendBufferSize != 0) {
        try {
          socket.setSendBufferSize(socketSendBufferSize);
        } catch (IOException e) {
          LOG.warn("Connection: unable to set socket send buffer size to " +
                   socketSendBufferSize);
        }
      }
    }   

    @Override
    public String toString() {
      return getHostAddress() + ":" + remotePort; 
    }

    boolean setShouldClose() {
      return shouldClose = true;
    }

    boolean shouldClose() {
      return shouldClose;
    }

    public String getHostAddress() {
      return hostAddress;
    }

    public InetAddress getHostInetAddress() {
      return addr;
    }
    
    public void setLastContact(long lastContact) {
      this.lastContact = lastContact;
    }

    public long getLastContact() {
      return lastContact;
    }

    public Server getServer() {
      return Server.this;
    }

    /* Return true if the connection has no outstanding rpc */
    private boolean isIdle() {
      return rpcCount.get() == 0;
    }
    
    /* Decrement the outstanding RPC count */
    private void decRpcCount() {
      rpcCount.decrementAndGet();
    }
    
    /* Increment the outstanding RPC count */
    private void incRpcCount() {
      rpcCount.incrementAndGet();
    }
    
    private UserGroupInformation getAuthorizedUgi(String authorizedId)
        throws InvalidToken, AccessControlException {
      if (authMethod == AuthMethod.TOKEN) {
        TokenIdentifier tokenId = SaslRpcServer.getIdentifier(authorizedId,
            secretManager);
        UserGroupInformation ugi = tokenId.getUser();
        if (ugi == null) {
          throw new AccessControlException(
              "Can't retrieve username from tokenIdentifier.");
        }
        ugi.addTokenIdentifier(tokenId);
        return ugi;
      } else {
        return UserGroupInformation.createRemoteUser(authorizedId, authMethod);
      }
    }

    private void saslReadAndProcess(RpcWritable.Buffer buffer) throws
        RpcServerException, IOException, InterruptedException {
      final RpcSaslProto saslMessage =
          getMessage(RpcSaslProto.getDefaultInstance(), buffer);
      switch (saslMessage.getState()) {
        case WRAP: {
          if (!saslContextEstablished || !useWrap) {
            throw new FatalRpcServerException(
                RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                new SaslException("Server is not wrapping data"));
          }
          // loops over decoded data and calls processOneRpc
          unwrapPacketAndProcessRpcs(saslMessage.getToken().toByteArray());
          break;
        }
        default:
          saslProcess(saslMessage);
      }
    }

    /**
     * Some exceptions ({@link RetriableException} and {@link StandbyException})
     * that are wrapped as a cause of parameter e are unwrapped so that they can
     * be sent as the true cause to the client side. In case of
     * {@link InvalidToken} we go one level deeper to get the true cause.
     * 
     * @param e the exception that may have a cause we want to unwrap.
     * @return the true cause for some exceptions.
     */
    private Throwable getTrueCause(IOException e) {
      Throwable cause = e;
      while (cause != null) {
        if (cause instanceof RetriableException) {
          return cause;
        } else if (cause instanceof StandbyException) {
          return cause;
        } else if (cause instanceof InvalidToken) {
          // FIXME: hadoop method signatures are restricting the SASL
          // callbacks to only returning InvalidToken, but some services
          // need to throw other exceptions (ex. NN + StandyException),
          // so for now we'll tunnel the real exceptions via an
          // InvalidToken's cause which normally is not set 
          if (cause.getCause() != null) {
            cause = cause.getCause();
          }
          return cause;
        }
        cause = cause.getCause();
      }
      return e;
    }
    
    /**
     * Process saslMessage and send saslResponse back
     * @param saslMessage received SASL message
     * @throws RpcServerException setup failed due to SASL negotiation
     *         failure, premature or invalid connection context, or other state 
     *         errors. This exception needs to be sent to the client. This 
     *         exception will wrap {@link RetriableException}, 
     *         {@link InvalidToken}, {@link StandbyException} or 
     *         {@link SaslException}.
     * @throws IOException if sending reply fails
     * @throws InterruptedException
     */
    private void saslProcess(RpcSaslProto saslMessage)
        throws RpcServerException, IOException, InterruptedException {
      if (saslContextEstablished) {
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
            new SaslException("Negotiation is already complete"));
      }
      RpcSaslProto saslResponse = null;
      try {
        try {
          saslResponse = processSaslMessage(saslMessage);
        } catch (IOException e) {
          rpcMetrics.incrAuthenticationFailures();
          if (LOG.isDebugEnabled()) {
            LOG.debug(StringUtils.stringifyException(e));
          }
          // attempting user could be null
          IOException tce = (IOException) getTrueCause(e);
          AUDITLOG.warn(AUTH_FAILED_FOR + this.toString() + ":"
              + attemptingUser + " (" + e.getLocalizedMessage()
              + ") with true cause: (" + tce.getLocalizedMessage() + ")");
          throw tce;
        }
        
        if (saslServer != null && saslServer.isComplete()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL server context established. Negotiated QoP is "
                + saslServer.getNegotiatedProperty(Sasl.QOP));
          }
          user = getAuthorizedUgi(saslServer.getAuthorizationID());
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL server successfully authenticated client: " + user);
          }
          rpcMetrics.incrAuthenticationSuccesses();
          AUDITLOG.info(AUTH_SUCCESSFUL_FOR + user);
          saslContextEstablished = true;
        }
      } catch (RpcServerException rse) { // don't re-wrap
        throw rse;
      } catch (IOException ioe) {
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_UNAUTHORIZED, ioe);
      }
      // send back response if any, may throw IOException
      if (saslResponse != null) {
        doSaslReply(saslResponse);
      }
      // do NOT enable wrapping until the last auth response is sent
      if (saslContextEstablished) {
        String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
        // SASL wrapping is only used if the connection has a QOP, and
        // the value is not auth.  ex. auth-int & auth-priv
        useWrap = (qop != null && !"auth".equalsIgnoreCase(qop));
        if (!useWrap) {
          disposeSasl();
        }
      }
    }
    
    /**
     * Process a saslMessge.
     * @param saslMessage received SASL message
     * @return the sasl response to send back to client
     * @throws SaslException if authentication or generating response fails, 
     *                       or SASL protocol mixup
     * @throws IOException if a SaslServer cannot be created
     * @throws AccessControlException if the requested authentication type 
     *         is not supported or trying to re-attempt negotiation.
     * @throws InterruptedException
     */
    private RpcSaslProto processSaslMessage(RpcSaslProto saslMessage)
        throws SaslException, IOException, AccessControlException,
        InterruptedException {
      final RpcSaslProto saslResponse;
      final SaslState state = saslMessage.getState(); // required      
      switch (state) {
        case NEGOTIATE: {
          if (sentNegotiate) {
            // FIXME shouldn't this be SaslException?
            throw new AccessControlException(
                "Client already attempted negotiation");
          }
          saslResponse = buildSaslNegotiateResponse();
          // simple-only server negotiate response is success which client
          // interprets as switch to simple
          if (saslResponse.getState() == SaslState.SUCCESS) {
            switchToSimple();
          }
          break;
        }
        case INITIATE: {
          if (saslMessage.getAuthsCount() != 1) {
            throw new SaslException("Client mechanism is malformed");
          }
          // verify the client requested an advertised authType
          SaslAuth clientSaslAuth = saslMessage.getAuths(0);
          if (!negotiateResponse.getAuthsList().contains(clientSaslAuth)) {
            if (sentNegotiate) {
              throw new AccessControlException(
                  clientSaslAuth.getMethod() + " authentication is not enabled."
                      + "  Available:" + enabledAuthMethods);
            }
            saslResponse = buildSaslNegotiateResponse();
            break;
          }
          authMethod = AuthMethod.valueOf(clientSaslAuth.getMethod());
          // abort SASL for SIMPLE auth, server has already ensured that
          // SIMPLE is a legit option above.  we will send no response
          if (authMethod == AuthMethod.SIMPLE) {
            switchToSimple();
            saslResponse = null;
            break;
          }
          // sasl server for tokens may already be instantiated
          if (saslServer == null || authMethod != AuthMethod.TOKEN) {
            saslServer = createSaslServer(authMethod);
          }
          saslResponse = processSaslToken(saslMessage);
          break;
        }
        case RESPONSE: {
          saslResponse = processSaslToken(saslMessage);
          break;
        }
        default:
          throw new SaslException("Client sent unsupported state " + state);
      }
      return saslResponse;
    }

    private RpcSaslProto processSaslToken(RpcSaslProto saslMessage)
        throws SaslException {
      if (!saslMessage.hasToken()) {
        throw new SaslException("Client did not send a token");
      }
      byte[] saslToken = saslMessage.getToken().toByteArray();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Have read input token of size " + saslToken.length
            + " for processing by saslServer.evaluateResponse()");
      }
      saslToken = saslServer.evaluateResponse(saslToken);
      return buildSaslResponse(
          saslServer.isComplete() ? SaslState.SUCCESS : SaslState.CHALLENGE,
          saslToken);
    }

    private void switchToSimple() {
      // disable SASL and blank out any SASL server
      authProtocol = AuthProtocol.NONE;
      disposeSasl();
    }

    private RpcSaslProto buildSaslResponse(SaslState state, byte[] replyToken) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Will send " + state + " token of size "
            + ((replyToken != null) ? replyToken.length : null)
            + " from saslServer.");
      }
      RpcSaslProto.Builder response = RpcSaslProto.newBuilder();
      response.setState(state);
      if (replyToken != null) {
        response.setToken(ByteString.copyFrom(replyToken));
      }
      return response.build();
    }

    private void doSaslReply(Message message) throws IOException {
      final RpcCall saslCall = new RpcCall(this, AuthProtocol.SASL.callId);
      setupResponse(saslCall,
          RpcStatusProto.SUCCESS, null,
          RpcWritable.wrap(message), null, null);
      sendResponse(saslCall);
    }

    private void doSaslReply(Exception ioe) throws IOException {
      setupResponse(authFailedCall,
          RpcStatusProto.FATAL, RpcErrorCodeProto.FATAL_UNAUTHORIZED,
          null, ioe.getClass().getName(), ioe.getLocalizedMessage());
      sendResponse(authFailedCall);
    }

    private void disposeSasl() {
      if (saslServer != null) {
        try {
          saslServer.dispose();
        } catch (SaslException ignored) {
        } finally {
          saslServer = null;
        }
      }
    }

    private void checkDataLength(int dataLength) throws IOException {
      if (dataLength < 0) {
        String error = "Unexpected data length " + dataLength +
                       "!! from " + getHostAddress();
        LOG.warn(error);
        throw new IOException(error);
      } else if (dataLength > maxDataLength) {
        String error = "Requested data length " + dataLength +
              " is longer than maximum configured RPC length " + 
            maxDataLength + ".  RPC came from " + getHostAddress();
        LOG.warn(error);
        throw new IOException(error);
      }
    }

    /**
     * This method reads in a non-blocking fashion from the channel: 
     * this method is called repeatedly when data is present in the channel; 
     * when it has enough data to process one rpc it processes that rpc.
     * 
     * On the first pass, it processes the connectionHeader, 
     * connectionContext (an outOfBand RPC) and at most one RPC request that 
     * follows that. On future passes it will process at most one RPC request.
     *  
     * Quirky things: dataLengthBuffer (4 bytes) is used to read "hrpc" OR 
     * rpc request length.
     *    
     * @return -1 in case of error, else num bytes read so far
     * @throws IOException - internal error that should not be returned to
     *         client, typically failure to respond to client
     * @throws InterruptedException
     */
    public int readAndProcess() throws IOException, InterruptedException {
      while (!shouldClose()) { // stop if a fatal response has been sent.
        // dataLengthBuffer is used to read "hrpc" or the rpc-packet length
        int count = -1;
        if (dataLengthBuffer.remaining() > 0) {
          count = channelRead(channel, dataLengthBuffer);       
          if (count < 0 || dataLengthBuffer.remaining() > 0) 
            return count;
        }
        
        if (!connectionHeaderRead) {
          // Every connection is expected to send the header;
          // so far we read "hrpc" of the connection header.
          if (connectionHeaderBuf == null) {
            // for the bytes that follow "hrpc", in the connection header
            connectionHeaderBuf = ByteBuffer.allocate(HEADER_LEN_AFTER_HRPC_PART);
          }
          count = channelRead(channel, connectionHeaderBuf);
          if (count < 0 || connectionHeaderBuf.remaining() > 0) {
            return count;
          }
          int version = connectionHeaderBuf.get(0);
          // TODO we should add handler for service class later
          this.setServiceClass(connectionHeaderBuf.get(1));
          dataLengthBuffer.flip();
          
          // Check if it looks like the user is hitting an IPC port
          // with an HTTP GET - this is a common error, so we can
          // send back a simple string indicating as much.
          if (HTTP_GET_BYTES.equals(dataLengthBuffer)) {
            setupHttpRequestOnIpcPortResponse();
            return -1;
          }
          
          if (!RpcConstants.HEADER.equals(dataLengthBuffer)
              || version != CURRENT_VERSION) {
            //Warning is ok since this is not supposed to happen.
            LOG.warn("Incorrect header or version mismatch from " + 
                     hostAddress + ":" + remotePort +
                     " got version " + version + 
                     " expected version " + CURRENT_VERSION);
            setupBadVersionResponse(version);
            return -1;
          }
          
          // this may switch us into SIMPLE
          authProtocol = initializeAuthContext(connectionHeaderBuf.get(2));          
          
          dataLengthBuffer.clear(); // clear to next read rpc packet len
          connectionHeaderBuf = null;
          connectionHeaderRead = true;
          continue; // connection header read, now read  4 bytes rpc packet len
        }
        
        if (data == null) { // just read 4 bytes -  length of RPC packet
          dataLengthBuffer.flip();
          dataLength = dataLengthBuffer.getInt();
          checkDataLength(dataLength);
          // Set buffer for reading EXACTLY the RPC-packet length and no more.
          data = ByteBuffer.allocate(dataLength);
        }
        // Now read the RPC packet
        count = channelRead(channel, data);
        
        if (data.remaining() == 0) {
          dataLengthBuffer.clear(); // to read length of future rpc packets
          data.flip();
          ByteBuffer requestData = data;
          data = null; // null out in case processOneRpc throws.
          boolean isHeaderRead = connectionContextRead;
          processOneRpc(requestData);
          // the last rpc-request we processed could have simply been the
          // connectionContext; if so continue to read the first RPC.
          if (!isHeaderRead) {
            continue;
          }
        } 
        return count;
      }
      return -1;
    }

    private AuthProtocol initializeAuthContext(int authType)
        throws IOException {
      AuthProtocol authProtocol = AuthProtocol.valueOf(authType);
      if (authProtocol == null) {
        IOException ioe = new IpcException("Unknown auth protocol:" + authType);
        doSaslReply(ioe);
        throw ioe;        
      }
      boolean isSimpleEnabled = enabledAuthMethods.contains(AuthMethod.SIMPLE);
      switch (authProtocol) {
        case NONE: {
          // don't reply if client is simple and server is insecure
          if (!isSimpleEnabled) {
            IOException ioe = new AccessControlException(
                "SIMPLE authentication is not enabled."
                    + "  Available:" + enabledAuthMethods);
            doSaslReply(ioe);
            throw ioe;
          }
          break;
        }
        default: {
          break;
        }
      }
      return authProtocol;
    }

    /**
     * Process the Sasl's Negotiate request, including the optimization of 
     * accelerating token negotiation.
     * @return the response to Negotiate request - the list of enabled 
     *         authMethods and challenge if the TOKENS are supported. 
     * @throws SaslException - if attempt to generate challenge fails.
     * @throws IOException - if it fails to create the SASL server for Tokens
     */
    private RpcSaslProto buildSaslNegotiateResponse()
        throws InterruptedException, SaslException, IOException {
      RpcSaslProto negotiateMessage = negotiateResponse;
      // accelerate token negotiation by sending initial challenge
      // in the negotiation response
      if (enabledAuthMethods.contains(AuthMethod.TOKEN)) {
        saslServer = createSaslServer(AuthMethod.TOKEN);
        byte[] challenge = saslServer.evaluateResponse(new byte[0]);
        RpcSaslProto.Builder negotiateBuilder =
            RpcSaslProto.newBuilder(negotiateResponse);
        negotiateBuilder.getAuthsBuilder(0)  // TOKEN is always first
            .setChallenge(ByteString.copyFrom(challenge));
        negotiateMessage = negotiateBuilder.build();
      }
      sentNegotiate = true;
      return negotiateMessage;
    }
    
    private SaslServer createSaslServer(AuthMethod authMethod)
        throws IOException, InterruptedException {
      final Map<String,?> saslProps =
                  saslPropsResolver.getServerProperties(addr);
      return new SaslRpcServer(authMethod).create(this, saslProps, secretManager);
    }
    
    /**
     * Try to set up the response to indicate that the client version
     * is incompatible with the server. This can contain special-case
     * code to speak enough of past IPC protocols to pass back
     * an exception to the caller.
     * @param clientVersion the version the caller is using 
     * @throws IOException
     */
    private void setupBadVersionResponse(int clientVersion) throws IOException {
      String errMsg = "Server IPC version " + CURRENT_VERSION +
      " cannot communicate with client version " + clientVersion;
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      
      if (clientVersion >= 9) {
        // Versions >>9  understand the normal response
        RpcCall fakeCall = new RpcCall(this, -1);
        setupResponse(fakeCall,
            RpcStatusProto.FATAL, RpcErrorCodeProto.FATAL_VERSION_MISMATCH,
            null, VersionMismatch.class.getName(), errMsg);
        sendResponse(fakeCall);
      } else if (clientVersion >= 3) {
        RpcCall fakeCall = new RpcCall(this, -1);
        // Versions 3 to 8 use older response
        setupResponseOldVersionFatal(buffer, fakeCall,
            null, VersionMismatch.class.getName(), errMsg);

        sendResponse(fakeCall);
      } else if (clientVersion == 2) { // Hadoop 0.18.3
        RpcCall fakeCall = new RpcCall(this, 0);
        DataOutputStream out = new DataOutputStream(buffer);
        out.writeInt(0); // call ID
        out.writeBoolean(true); // error
        WritableUtils.writeString(out, VersionMismatch.class.getName());
        WritableUtils.writeString(out, errMsg);
        fakeCall.setResponse(ByteBuffer.wrap(buffer.toByteArray()));
        sendResponse(fakeCall);
      }
    }
    
    private void setupHttpRequestOnIpcPortResponse() throws IOException {
      RpcCall fakeCall = new RpcCall(this, 0);
      fakeCall.setResponse(ByteBuffer.wrap(
          RECEIVED_HTTP_REQ_RESPONSE.getBytes(StandardCharsets.UTF_8)));
      sendResponse(fakeCall);
    }

    /** Reads the connection context following the connection header
     * @throws RpcServerException - if the header cannot be
     *         deserialized, or the user is not authorized
     */ 
    private void processConnectionContext(RpcWritable.Buffer buffer)
        throws RpcServerException {
      // allow only one connection context during a session
      if (connectionContextRead) {
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
            "Connection context already processed");
      }
      connectionContext = getMessage(IpcConnectionContextProto.getDefaultInstance(), buffer);
      protocolName = connectionContext.hasProtocol() ? connectionContext
          .getProtocol() : null;

      UserGroupInformation protocolUser = ProtoUtil.getUgi(connectionContext);
      if (authProtocol == AuthProtocol.NONE) {
        user = protocolUser;
      } else {
        // user is authenticated
        user.setAuthenticationMethod(authMethod);
        //Now we check if this is a proxy user case. If the protocol user is
        //different from the 'user', it is a proxy user scenario. However, 
        //this is not allowed if user authenticated with DIGEST.
        if ((protocolUser != null)
            && (!protocolUser.getUserName().equals(user.getUserName()))) {
          if (authMethod == AuthMethod.TOKEN) {
            // Not allowed to doAs if token authentication is used
            throw new FatalRpcServerException(
                RpcErrorCodeProto.FATAL_UNAUTHORIZED,
                new AccessControlException("Authenticated user (" + user
                    + ") doesn't match what the client claims to be ("
                    + protocolUser + ")"));
          } else {
            // Effective user can be different from authenticated user
            // for simple auth or kerberos auth
            // The user is the real user. Now we create a proxy user
            UserGroupInformation realUser = user;
            user = UserGroupInformation.createProxyUser(protocolUser
                .getUserName(), realUser);
          }
        }
      }
      authorizeConnection();
      // don't set until after authz because connection isn't established
      connectionContextRead = true;
      if (user != null) {
        connectionManager.incrUserConnections(user.getShortUserName());
      }
    }
    
    /**
     * Process a wrapped RPC Request - unwrap the SASL packet and process
     * each embedded RPC request 
     * @param inBuf - SASL wrapped request of one or more RPCs
     * @throws IOException - SASL packet cannot be unwrapped
     * @throws InterruptedException
     */    
    private void unwrapPacketAndProcessRpcs(byte[] inBuf)
        throws IOException, InterruptedException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Have read input token of size " + inBuf.length
            + " for processing by saslServer.unwrap()");
      }
      inBuf = saslServer.unwrap(inBuf, 0, inBuf.length);
      ReadableByteChannel ch = Channels.newChannel(new ByteArrayInputStream(
          inBuf));
      // Read all RPCs contained in the inBuf, even partial ones
      while (!shouldClose()) { // stop if a fatal response has been sent.
        int count = -1;
        if (unwrappedDataLengthBuffer.remaining() > 0) {
          count = channelRead(ch, unwrappedDataLengthBuffer);
          if (count <= 0 || unwrappedDataLengthBuffer.remaining() > 0)
            return;
        }

        if (unwrappedData == null) {
          unwrappedDataLengthBuffer.flip();
          int unwrappedDataLength = unwrappedDataLengthBuffer.getInt();
          unwrappedData = ByteBuffer.allocate(unwrappedDataLength);
        }

        count = channelRead(ch, unwrappedData);
        if (count <= 0 || unwrappedData.remaining() > 0)
          return;

        if (unwrappedData.remaining() == 0) {
          unwrappedDataLengthBuffer.clear();
          unwrappedData.flip();
          ByteBuffer requestData = unwrappedData;
          unwrappedData = null; // null out in case processOneRpc throws.
          processOneRpc(requestData);
        }
      }
    }
    
    /**
     * Process one RPC Request from buffer read from socket stream 
     *  - decode rpc in a rpc-Call
     *  - handle out-of-band RPC requests such as the initial connectionContext
     *  - A successfully decoded RpcCall will be deposited in RPC-Q and
     *    its response will be sent later when the request is processed.
     * 
     * Prior to this call the connectionHeader ("hrpc...") has been handled and
     * if SASL then SASL has been established and the buf we are passed
     * has been unwrapped from SASL.
     * 
     * @param bb - contains the RPC request header and the rpc request
     * @throws IOException - internal error that should not be returned to
     *         client, typically failure to respond to client
     * @throws InterruptedException
     */
    private void processOneRpc(ByteBuffer bb)
        throws IOException, InterruptedException {
      // exceptions that escape this method are fatal to the connection.
      // setupResponse will use the rpc status to determine if the connection
      // should be closed.
      int callId = -1;
      int retry = RpcConstants.INVALID_RETRY_COUNT;
      try {
        final RpcWritable.Buffer buffer = RpcWritable.Buffer.wrap(bb);
        final RpcRequestHeaderProto header =
            getMessage(RpcRequestHeaderProto.getDefaultInstance(), buffer);
        callId = header.getCallId();
        retry = header.getRetryCount();
        if (LOG.isDebugEnabled()) {
          LOG.debug(" got #" + callId);
        }
        checkRpcHeaders(header);

        if (callId < 0) { // callIds typically used during connection setup
          processRpcOutOfBandRequest(header, buffer);
        } else if (!connectionContextRead) {
          throw new FatalRpcServerException(
              RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
              "Connection context not established");
        } else {
          processRpcRequest(header, buffer);
        }
      } catch (RpcServerException rse) {
        // inform client of error, but do not rethrow else non-fatal
        // exceptions will close connection!
        if (LOG.isDebugEnabled()) {
          LOG.debug(Thread.currentThread().getName() +
              ": processOneRpc from client " + this +
              " threw exception [" + rse + "]");
        }
        // use the wrapped exception if there is one.
        Throwable t = (rse.getCause() != null) ? rse.getCause() : rse;
        final RpcCall call = new RpcCall(this, callId, retry);
        setupResponse(call,
            rse.getRpcStatusProto(), rse.getRpcErrorCodeProto(), null,
            t.getClass().getName(), t.getMessage());
        sendResponse(call);
      }
    }

    /**
     * Verify RPC header is valid
     * @param header - RPC request header
     * @throws RpcServerException - header contains invalid values
     */
    private void checkRpcHeaders(RpcRequestHeaderProto header)
        throws RpcServerException {
      if (!header.hasRpcOp()) {
        String err = " IPC Server: No rpc op in rpcRequestHeader";
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
      }
      if (header.getRpcOp() != 
          RpcRequestHeaderProto.OperationProto.RPC_FINAL_PACKET) {
        String err = "IPC Server does not implement rpc header operation" + 
                header.getRpcOp();
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
      }
      // If we know the rpc kind, get its class so that we can deserialize
      // (Note it would make more sense to have the handler deserialize but 
      // we continue with this original design.
      if (!header.hasRpcKind()) {
        String err = " IPC Server: No rpc kind in rpcRequestHeader";
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
      }
    }

    /**
     * Process an RPC Request 
     *   - the connection headers and context must have been already read.
     *   - Based on the rpcKind, decode the rpcRequest.
     *   - A successfully decoded RpcCall will be deposited in RPC-Q and
     *     its response will be sent later when the request is processed.
     * @param header - RPC request header
     * @param buffer - stream to request payload
     * @throws RpcServerException - generally due to fatal rpc layer issues
     *   such as invalid header or deserialization error.  The call queue
     *   may also throw a fatal or non-fatal exception on overflow.
     * @throws IOException - fatal internal error that should/could not
     *   be sent to client.
     * @throws InterruptedException
     */
    private void processRpcRequest(RpcRequestHeaderProto header,
        RpcWritable.Buffer buffer) throws RpcServerException,
        InterruptedException {
      Class<? extends Writable> rpcRequestClass = 
          getRpcRequestWrapper(header.getRpcKind());
      if (rpcRequestClass == null) {
        LOG.warn("Unknown rpc kind "  + header.getRpcKind() + 
            " from client " + getHostAddress());
        final String err = "Unknown rpc kind in rpc header"  + 
            header.getRpcKind();
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
      }
      Writable rpcRequest;
      try { //Read the rpc request
        rpcRequest = buffer.newInstance(rpcRequestClass, conf);
      } catch (RpcServerException rse) { // lets tests inject failures.
        throw rse;
      } catch (Throwable t) { // includes runtime exception from newInstance
        LOG.warn("Unable to read call parameters for client " +
                 getHostAddress() + "on connection protocol " +
            this.protocolName + " for rpcKind " + header.getRpcKind(),  t);
        String err = "IPC server unable to read call parameters: "+ t.getMessage();
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_DESERIALIZING_REQUEST, err);
      }
        
      TraceScope traceScope = null;
      if (header.hasTraceInfo()) {
        if (tracer != null) {
          // If the incoming RPC included tracing info, always continue the
          // trace
          SpanId parentSpanId = new SpanId(
              header.getTraceInfo().getTraceId(),
              header.getTraceInfo().getParentId());
          traceScope = tracer.newScope(
              RpcClientUtil.toTraceName(rpcRequest.toString()),
              parentSpanId);
          traceScope.detach();
        }
      }

      CallerContext callerContext = null;
      if (header.hasCallerContext()) {
        callerContext =
            new CallerContext.Builder(header.getCallerContext().getContext())
                .setSignature(header.getCallerContext().getSignature()
                    .toByteArray())
                .build();
      }

      RpcCall call = new RpcCall(this, header.getCallId(),
          header.getRetryCount(), rpcRequest,
          ProtoUtil.convert(header.getRpcKind()),
          header.getClientId().toByteArray(), traceScope, callerContext);

      // Save the priority level assignment by the scheduler
      call.setPriorityLevel(callQueue.getPriorityLevel(call));

      try {
        internalQueueCall(call);
      } catch (RpcServerException rse) {
        throw rse;
      } catch (IOException ioe) {
        throw new FatalRpcServerException(
            RpcErrorCodeProto.ERROR_RPC_SERVER, ioe);
      }
      incRpcCount();  // Increment the rpc count
    }

    /**
     * Establish RPC connection setup by negotiating SASL if required, then
     * reading and authorizing the connection header
     * @param header - RPC header
     * @param buffer - stream to request payload
     * @throws RpcServerException - setup failed due to SASL
     *         negotiation failure, premature or invalid connection context,
     *         or other state errors. This exception needs to be sent to the 
     *         client.
     * @throws IOException - failed to send a response back to the client
     * @throws InterruptedException
     */
    private void processRpcOutOfBandRequest(RpcRequestHeaderProto header,
        RpcWritable.Buffer buffer) throws RpcServerException,
            IOException, InterruptedException {
      final int callId = header.getCallId();
      if (callId == CONNECTION_CONTEXT_CALL_ID) {
        // SASL must be established prior to connection context
        if (authProtocol == AuthProtocol.SASL && !saslContextEstablished) {
          throw new FatalRpcServerException(
              RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
              "Connection header sent during SASL negotiation");
        }
        // read and authorize the user
        processConnectionContext(buffer);
      } else if (callId == AuthProtocol.SASL.callId) {
        // if client was switched to simple, ignore first SASL message
        if (authProtocol != AuthProtocol.SASL) {
          throw new FatalRpcServerException(
              RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
              "SASL protocol not requested by client");
        }
        saslReadAndProcess(buffer);
      } else if (callId == PING_CALL_ID) {
        LOG.debug("Received ping message");
      } else {
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
            "Unknown out of band call #" + callId);
      }
    }    

    /**
     * Authorize proxy users to access this server
     * @throws RpcServerException - user is not allowed to proxy
     */
    private void authorizeConnection() throws RpcServerException {
      try {
        // If auth method is TOKEN, the token was obtained by the
        // real user for the effective user, therefore not required to
        // authorize real user. doAs is allowed only for simple or kerberos
        // authentication
        if (user != null && user.getRealUser() != null
            && (authMethod != AuthMethod.TOKEN)) {
          ProxyUsers.authorize(user, this.getHostAddress());
        }
        authorize(user, protocolName, getHostInetAddress());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Successfully authorized " + connectionContext);
        }
        rpcMetrics.incrAuthorizationSuccesses();
      } catch (AuthorizationException ae) {
        LOG.info("Connection from " + this
            + " for protocol " + connectionContext.getProtocol()
            + " is unauthorized for user " + user);
        rpcMetrics.incrAuthorizationFailures();
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_UNAUTHORIZED, ae);
      }
    }
    
    /**
     * Decode the a protobuf from the given input stream 
     * @return Message - decoded protobuf
     * @throws RpcServerException - deserialization failed
     */
    @SuppressWarnings("unchecked")
    <T extends Message> T getMessage(Message message,
        RpcWritable.Buffer buffer) throws RpcServerException {
      try {
        return (T)buffer.getValue(message);
      } catch (Exception ioe) {
        Class<?> protoClass = message.getClass();
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_DESERIALIZING_REQUEST,
            "Error decoding " + protoClass.getSimpleName() + ": "+ ioe);
      }
    }

    // ipc reader threads should invoke this directly, whereas handlers
    // must invoke call.sendResponse to allow lifecycle management of
    // external, postponed, deferred calls, etc.
    private void sendResponse(RpcCall call) throws IOException {
      responder.doRespond(call);
    }

    /**
     * Get service class for connection
     * @return the serviceClass
     */
    public int getServiceClass() {
      return serviceClass;
    }

    /**
     * Set service class for connection
     * @param serviceClass the serviceClass to set
     */
    public void setServiceClass(int serviceClass) {
      this.serviceClass = serviceClass;
    }

    private synchronized void close() {
      disposeSasl();
      data = null;
      dataLengthBuffer = null;
      if (!channel.isOpen())
        return;
      try {socket.shutdownOutput();} catch(Exception e) {
        LOG.debug("Ignoring socket shutdown exception", e);
      }
      if (channel.isOpen()) {
        IOUtils.cleanup(null, channel);
      }
      IOUtils.cleanup(null, socket);
    }
  }

  public void queueCall(Call call) throws IOException, InterruptedException {
    // external non-rpc calls don't need server exception wrapper.
    try {
      internalQueueCall(call);
    } catch (RpcServerException rse) {
      throw (IOException)rse.getCause();
    }
  }

  private void internalQueueCall(Call call)
      throws IOException, InterruptedException {
    try {
      callQueue.put(call); // queue the call; maybe blocked here
    } catch (CallQueueOverflowException cqe) {
      // If rpc scheduler indicates back off based on performance degradation
      // such as response time or rpc queue is full, we will ask the client
      // to back off by throwing RetriableException. Whether the client will
      // honor RetriableException and retry depends the client and its policy.
      // For example, IPC clients using FailoverOnNetworkExceptionRetry handle
      // RetriableException.
      rpcMetrics.incrClientBackoff();
      // unwrap retriable exception.
      throw cqe.getCause();
    }
  }

  /** Handles queued calls . */
  private class Handler extends Thread {
    public Handler(int instanceNumber) {
      this.setDaemon(true);
      this.setName("IPC Server handler "+ instanceNumber + " on " + port);
    }

    @Override
    public void run() {
      LOG.debug(Thread.currentThread().getName() + ": starting");
      SERVER.set(Server.this);
      while (running) {
        TraceScope traceScope = null;
        try {
          final Call call = callQueue.take(); // pop the queue; maybe blocked here
          if (LOG.isDebugEnabled()) {
            LOG.debug(Thread.currentThread().getName() + ": " + call + " for RpcKind " + call.rpcKind);
          }
          CurCall.set(call);
          if (call.traceScope != null) {
            call.traceScope.reattach();
            traceScope = call.traceScope;
            traceScope.getSpan().addTimelineAnnotation("called");
          }
          // always update the current call context
          CallerContext.setCurrent(call.callerContext);
          UserGroupInformation remoteUser = call.getRemoteUser();
          if (remoteUser != null) {
            remoteUser.doAs(call);
          } else {
            call.run();
          }
        } catch (InterruptedException e) {
          if (running) {                          // unexpected -- log it
            LOG.info(Thread.currentThread().getName() + " unexpectedly interrupted", e);
            if (traceScope != null) {
              traceScope.getSpan().addTimelineAnnotation("unexpectedly interrupted: " +
                  StringUtils.stringifyException(e));
            }
          }
        } catch (Exception e) {
          LOG.info(Thread.currentThread().getName() + " caught an exception", e);
          if (traceScope != null) {
            traceScope.getSpan().addTimelineAnnotation("Exception: " +
                StringUtils.stringifyException(e));
          }
        } finally {
          CurCall.set(null);
          IOUtils.cleanupWithLogger(LOG, traceScope);
        }
      }
      LOG.debug(Thread.currentThread().getName() + ": exiting");
    }

  }

  @VisibleForTesting
  void logException(Logger logger, Throwable e, Call call) {
    if (exceptionsHandler.isSuppressedLog(e.getClass())) {
      return; // Log nothing.
    }

    final String logMsg = Thread.currentThread().getName() + ", call " + call;
    if (exceptionsHandler.isTerseLog(e.getClass())) {
      // Don't log the whole stack trace. Way too noisy!
      logger.info(logMsg + ": " + e);
    } else if (e instanceof RuntimeException || e instanceof Error) {
      // These exception types indicate something is probably wrong
      // on the server side, as opposed to just a normal exceptional
      // result.
      logger.warn(logMsg, e);
    } else {
      logger.info(logMsg, e);
    }
  }
  
  protected Server(String bindAddress, int port,
                  Class<? extends Writable> paramClass, int handlerCount, 
                  Configuration conf)
    throws IOException 
  {
    this(bindAddress, port, paramClass, handlerCount, -1, -1, conf, Integer
        .toString(port), null, null);
  }
  
  protected Server(String bindAddress, int port,
      Class<? extends Writable> rpcRequestClass, int handlerCount,
      int numReaders, int queueSizePerHandler, Configuration conf,
      String serverName, SecretManager<? extends TokenIdentifier> secretManager)
    throws IOException {
    this(bindAddress, port, rpcRequestClass, handlerCount, numReaders, 
        queueSizePerHandler, conf, serverName, secretManager, null);
  }
  
  /** 
   * Constructs a server listening on the named port and address.  Parameters passed must
   * be of the named class.  The <code>handlerCount</handlerCount> determines
   * the number of handler threads that will be used to process calls.
   * If queueSizePerHandler or numReaders are not -1 they will be used instead of parameters
   * from configuration. Otherwise the configuration will be picked up.
   * 
   * If rpcRequestClass is null then the rpcRequestClass must have been 
   * registered via {@link #registerProtocolEngine(RPC.RpcKind,
   *  Class, RPC.RpcInvoker)}
   * This parameter has been retained for compatibility with existing tests
   * and usage.
   */
  @SuppressWarnings("unchecked")
  protected Server(String bindAddress, int port,
      Class<? extends Writable> rpcRequestClass, int handlerCount,
      int numReaders, int queueSizePerHandler, Configuration conf,
      String serverName, SecretManager<? extends TokenIdentifier> secretManager,
      String portRangeConfig)
    throws IOException {
    this.bindAddress = bindAddress;
    this.conf = conf;
    this.portRangeConfig = portRangeConfig;
    this.port = port;
    this.rpcRequestClass = rpcRequestClass; 
    this.handlerCount = handlerCount;
    this.socketSendBufferSize = 0;
    this.serverName = serverName;
    this.maxDataLength = conf.getInt(CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH,
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
    if (queueSizePerHandler != -1) {
      this.maxQueueSize = handlerCount * queueSizePerHandler;
    } else {
      this.maxQueueSize = handlerCount * conf.getInt(
          CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_KEY,
          CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT);      
    }
    this.maxRespSize = conf.getInt(
        CommonConfigurationKeys.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY,
        CommonConfigurationKeys.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT);
    if (numReaders != -1) {
      this.readThreads = numReaders;
    } else {
      this.readThreads = conf.getInt(
          CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY,
          CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_DEFAULT);
    }
    this.readerPendingConnectionQueue = conf.getInt(
        CommonConfigurationKeys.IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_KEY,
        CommonConfigurationKeys.IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_DEFAULT);

    // Setup appropriate callqueue
    final String prefix = getQueueClassPrefix();
    this.callQueue = new CallQueueManager<Call>(getQueueClass(prefix, conf),
        getSchedulerClass(prefix, conf),
        getClientBackoffEnable(prefix, conf), maxQueueSize, prefix, conf);

    this.secretManager = (SecretManager<TokenIdentifier>) secretManager;
    this.authorize = 
      conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, 
                      false);

    // configure supported authentications
    this.enabledAuthMethods = getAuthMethods(secretManager, conf);
    this.negotiateResponse = buildNegotiateResponse(enabledAuthMethods);
    
    // Start the listener here and let it bind to the port
    listener = new Listener();
    this.port = listener.getAddress().getPort();    
    connectionManager = new ConnectionManager();
    this.rpcMetrics = RpcMetrics.create(this, conf);
    this.rpcDetailedMetrics = RpcDetailedMetrics.create(this.port);
    this.tcpNoDelay = conf.getBoolean(
        CommonConfigurationKeysPublic.IPC_SERVER_TCPNODELAY_KEY,
        CommonConfigurationKeysPublic.IPC_SERVER_TCPNODELAY_DEFAULT);

    this.setLogSlowRPC(conf.getBoolean(
        CommonConfigurationKeysPublic.IPC_SERVER_LOG_SLOW_RPC,
        CommonConfigurationKeysPublic.IPC_SERVER_LOG_SLOW_RPC_DEFAULT));

    // Create the responder here
    responder = new Responder();
    
    if (secretManager != null || UserGroupInformation.isSecurityEnabled()) {
      SaslRpcServer.init(conf);
      saslPropsResolver = SaslPropertiesResolver.getInstance(conf);
    }
    
    this.exceptionsHandler.addTerseLoggingExceptions(StandbyException.class);
  }
  
  private RpcSaslProto buildNegotiateResponse(List<AuthMethod> authMethods)
      throws IOException {
    RpcSaslProto.Builder negotiateBuilder = RpcSaslProto.newBuilder();
    if (authMethods.contains(AuthMethod.SIMPLE) && authMethods.size() == 1) {
      // SIMPLE-only servers return success in response to negotiate
      negotiateBuilder.setState(SaslState.SUCCESS);
    } else {
      negotiateBuilder.setState(SaslState.NEGOTIATE);
      for (AuthMethod authMethod : authMethods) {
        SaslRpcServer saslRpcServer = new SaslRpcServer(authMethod);      
        SaslAuth.Builder builder = negotiateBuilder.addAuthsBuilder()
            .setMethod(authMethod.toString())
            .setMechanism(saslRpcServer.mechanism);
        if (saslRpcServer.protocol != null) {
          builder.setProtocol(saslRpcServer.protocol);
        }
        if (saslRpcServer.serverId != null) {
          builder.setServerId(saslRpcServer.serverId);
        }
      }
    }
    return negotiateBuilder.build();
  }

  // get the security type from the conf. implicitly include token support
  // if a secret manager is provided, or fail if token is the conf value but
  // there is no secret manager
  private List<AuthMethod> getAuthMethods(SecretManager<?> secretManager,
                                             Configuration conf) {
    AuthenticationMethod confAuthenticationMethod =
        SecurityUtil.getAuthenticationMethod(conf);        
    List<AuthMethod> authMethods = new ArrayList<AuthMethod>();
    if (confAuthenticationMethod == AuthenticationMethod.TOKEN) {
      if (secretManager == null) {
        throw new IllegalArgumentException(AuthenticationMethod.TOKEN +
            " authentication requires a secret manager");
      } 
    } else if (secretManager != null) {
      LOG.debug(AuthenticationMethod.TOKEN +
          " authentication enabled for secret manager");
      // most preferred, go to the front of the line!
      authMethods.add(AuthenticationMethod.TOKEN.getAuthMethod());
    }
    authMethods.add(confAuthenticationMethod.getAuthMethod());        
    
    LOG.debug("Server accepts auth methods:" + authMethods);
    return authMethods;
  }
  
  private void closeConnection(Connection connection) {
    connectionManager.close(connection);
  }

  /**
   * Setup response for the IPC Call.
   * 
   * @param call {@link Call} to which we are setting up the response
   * @param status of the IPC call
   * @param rv return value for the IPC Call, if the call was successful
   * @param errorClass error class, if the the call failed
   * @param error error message, if the call failed
   * @throws IOException
   */
  private void setupResponse(
      RpcCall call, RpcStatusProto status, RpcErrorCodeProto erCode,
      Writable rv, String errorClass, String error)
          throws IOException {
    // fatal responses will cause the reader to close the connection.
    if (status == RpcStatusProto.FATAL) {
      call.connection.setShouldClose();
    }
    RpcResponseHeaderProto.Builder headerBuilder =
        RpcResponseHeaderProto.newBuilder();
    headerBuilder.setClientId(ByteString.copyFrom(call.clientId));
    headerBuilder.setCallId(call.callId);
    headerBuilder.setRetryCount(call.retryCount);
    headerBuilder.setStatus(status);
    headerBuilder.setServerIpcVersionNum(CURRENT_VERSION);

    if (status == RpcStatusProto.SUCCESS) {
      RpcResponseHeaderProto header = headerBuilder.build();
      try {
        setupResponse(call, header, rv);
      } catch (Throwable t) {
        LOG.warn("Error serializing call response for call " + call, t);
        // Call back to same function - this is OK since the
        // buffer is reset at the top, and since status is changed
        // to ERROR it won't infinite loop.
        setupResponse(call, RpcStatusProto.ERROR,
            RpcErrorCodeProto.ERROR_SERIALIZING_RESPONSE,
            null, t.getClass().getName(),
            StringUtils.stringifyException(t));
        return;
      }
    } else { // Rpc Failure
      headerBuilder.setExceptionClassName(errorClass);
      headerBuilder.setErrorMsg(error);
      headerBuilder.setErrorDetail(erCode);
      setupResponse(call, headerBuilder.build(), null);
    }
  }

  private void setupResponse(RpcCall call,
      RpcResponseHeaderProto header, Writable rv) throws IOException {
    final byte[] response;
    if (rv == null || (rv instanceof RpcWritable.ProtobufWrapper)) {
      response = setupResponseForProtobuf(header, rv);
    } else {
      response = setupResponseForWritable(header, rv);
    }
    if (response.length > maxRespSize) {
      LOG.warn("Large response size " + response.length + " for call "
          + call.toString());
    }
    call.setResponse(ByteBuffer.wrap(response));
  }

  private byte[] setupResponseForWritable(
      RpcResponseHeaderProto header, Writable rv) throws IOException {
    ResponseBuffer buf = responseBuffer.get().reset();
    try {
      RpcWritable.wrap(header).writeTo(buf);
      if (rv != null) {
        RpcWritable.wrap(rv).writeTo(buf);
      }
      return buf.toByteArray();
    } finally {
      // Discard a large buf and reset it back to smaller size
      // to free up heap.
      if (buf.capacity() > maxRespSize) {
        buf.setCapacity(INITIAL_RESP_BUF_SIZE);
      }
    }
  }


  // writing to a pre-allocated array is the most efficient way to construct
  // a protobuf response.
  private byte[] setupResponseForProtobuf(
      RpcResponseHeaderProto header, Writable rv) throws IOException {
    Message payload = (rv != null)
        ? ((RpcWritable.ProtobufWrapper)rv).getMessage() : null;
    int length = getDelimitedLength(header);
    if (payload != null) {
      length += getDelimitedLength(payload);
    }
    byte[] buf = new byte[length + 4];
    CodedOutputStream cos = CodedOutputStream.newInstance(buf);
    // the stream only supports little endian ints
    cos.writeRawByte((byte)((length >>> 24) & 0xFF));
    cos.writeRawByte((byte)((length >>> 16) & 0xFF));
    cos.writeRawByte((byte)((length >>>  8) & 0xFF));
    cos.writeRawByte((byte)((length >>>  0) & 0xFF));
    cos.writeRawVarint32(header.getSerializedSize());
    header.writeTo(cos);
    if (payload != null) {
      cos.writeRawVarint32(payload.getSerializedSize());
      payload.writeTo(cos);
    }
    return buf;
  }

  private static int getDelimitedLength(Message message) {
    int length = message.getSerializedSize();
    return length + CodedOutputStream.computeRawVarint32Size(length);
  }

  /**
   * Setup response for the IPC Call on Fatal Error from a 
   * client that is using old version of Hadoop.
   * The response is serialized using the previous protocol's response
   * layout.
   * 
   * @param response buffer to serialize the response into
   * @param call {@link Call} to which we are setting up the response
   * @param rv return value for the IPC Call, if the call was successful
   * @param errorClass error class, if the the call failed
   * @param error error message, if the call failed
   * @throws IOException
   */
  private void setupResponseOldVersionFatal(ByteArrayOutputStream response, 
                             RpcCall call,
                             Writable rv, String errorClass, String error) 
  throws IOException {
    final int OLD_VERSION_FATAL_STATUS = -1;
    response.reset();
    DataOutputStream out = new DataOutputStream(response);
    out.writeInt(call.callId);                // write call id
    out.writeInt(OLD_VERSION_FATAL_STATUS);   // write FATAL_STATUS
    WritableUtils.writeString(out, errorClass);
    WritableUtils.writeString(out, error);
    call.setResponse(ByteBuffer.wrap(response.toByteArray()));
  }

  private void wrapWithSasl(RpcCall call) throws IOException {
    if (call.connection.saslServer != null) {
      byte[] token = call.rpcResponse.array();
      // synchronization may be needed since there can be multiple Handler
      // threads using saslServer to wrap responses.
      synchronized (call.connection.saslServer) {
        token = call.connection.saslServer.wrap(token, 0, token.length);
      }
      if (LOG.isDebugEnabled())
        LOG.debug("Adding saslServer wrapped token of size " + token.length
            + " as call response.");
      // rebuild with sasl header and payload
      RpcResponseHeaderProto saslHeader = RpcResponseHeaderProto.newBuilder()
          .setCallId(AuthProtocol.SASL.callId)
          .setStatus(RpcStatusProto.SUCCESS)
          .build();
      RpcSaslProto saslMessage = RpcSaslProto.newBuilder()
          .setState(SaslState.WRAP)
          .setToken(ByteString.copyFrom(token))
          .build();
      setupResponse(call, saslHeader, RpcWritable.wrap(saslMessage));
    }
  }
  
  Configuration getConf() {
    return conf;
  }
  
  /** Sets the socket buffer size used for responding to RPCs */
  public void setSocketSendBufSize(int size) { this.socketSendBufferSize = size; }

  public void setTracer(Tracer t) {
    this.tracer = t;
  }

  /** Starts the service.  Must be called before any calls will be handled. */
  public synchronized void start() {
    responder.start();
    listener.start();
    handlers = new Handler[handlerCount];
    
    for (int i = 0; i < handlerCount; i++) {
      handlers[i] = new Handler(i);
      handlers[i].start();
    }
  }

  /** Stops the service.  No new calls will be handled after this is called. */
  public synchronized void stop() {
    LOG.info("Stopping server on " + port);
    running = false;
    if (handlers != null) {
      for (int i = 0; i < handlerCount; i++) {
        if (handlers[i] != null) {
          handlers[i].interrupt();
        }
      }
    }
    listener.interrupt();
    listener.doStop();
    responder.interrupt();
    notifyAll();
    this.rpcMetrics.shutdown();
    this.rpcDetailedMetrics.shutdown();
  }

  /** Wait for the server to be stopped.
   * Does not wait for all subthreads to finish.
   *  See {@link #stop()}.
   */
  public synchronized void join() throws InterruptedException {
    while (running) {
      wait();
    }
  }

  /**
   * Return the socket (ip+port) on which the RPC server is listening to.
   * @return the socket (ip+port) on which the RPC server is listening to.
   */
  public synchronized InetSocketAddress getListenerAddress() {
    return listener.getAddress();
  }
  
  /** 
   * Called for each call. 
   * @deprecated Use  {@link #call(RPC.RpcKind, String,
   *  Writable, long)} instead
   */
  @Deprecated
  public Writable call(Writable param, long receiveTime) throws Exception {
    return call(RPC.RpcKind.RPC_BUILTIN, null, param, receiveTime);
  }
  
  /** Called for each call. */
  public abstract Writable call(RPC.RpcKind rpcKind, String protocol,
      Writable param, long receiveTime) throws Exception;
  
  /**
   * Authorize the incoming client connection.
   * 
   * @param user client user
   * @param protocolName - the protocol
   * @param addr InetAddress of incoming connection
   * @throws AuthorizationException when the client isn't authorized to talk the protocol
   */
  private void authorize(UserGroupInformation user, String protocolName,
      InetAddress addr) throws AuthorizationException {
    if (authorize) {
      if (protocolName == null) {
        throw new AuthorizationException("Null protocol not authorized");
      }
      Class<?> protocol = null;
      try {
        protocol = getProtocolClass(protocolName, getConf());
      } catch (ClassNotFoundException cfne) {
        throw new AuthorizationException("Unknown protocol: " + 
                                         protocolName);
      }
      serviceAuthorizationManager.authorize(user, protocol, getConf(), addr);
    }
  }
  
  /**
   * Get the port on which the IPC Server is listening for incoming connections.
   * This could be an ephemeral port too, in which case we return the real
   * port on which the Server has bound.
   * @return port on which IPC Server is listening
   */
  public int getPort() {
    return port;
  }
  
  /**
   * The number of open RPC conections
   * @return the number of open rpc connections
   */
  public int getNumOpenConnections() {
    return connectionManager.size();
  }

  /**
   * Get the NumOpenConnections/User.
   */
  public String getNumOpenConnectionsPerUser() {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper
          .writeValueAsString(connectionManager.getUserToConnectionsMap());
    } catch (IOException ignored) {
    }
    return null;
  }

  /**
   * The number of RPC connections dropped due to
   * too many connections.
   * @return the number of dropped rpc connections
   */
  public long getNumDroppedConnections() {
    return connectionManager.getDroppedConnections();

  }

  /**
   * The number of rpc calls in the queue.
   * @return The number of rpc calls in the queue.
   */
  public int getCallQueueLen() {
    return callQueue.size();
  }

  public boolean isClientBackoffEnabled() {
    return callQueue.isClientBackoffEnabled();
  }

  public void setClientBackoffEnabled(boolean value) {
    callQueue.setClientBackoffEnabled(value);
  }

  /**
   * The maximum size of the rpc call queue of this server.
   * @return The maximum size of the rpc call queue.
   */
  public int getMaxQueueSize() {
    return maxQueueSize;
  }

  /**
   * The number of reader threads for this server.
   * @return The number of reader threads.
   */
  public int getNumReaders() {
    return readThreads;
  }

  /**
   * When the read or write buffer size is larger than this limit, i/o will be 
   * done in chunks of this size. Most RPC requests and responses would be
   * be smaller.
   */
  private static int NIO_BUFFER_LIMIT = 8*1024; //should not be more than 64KB.
  
  /**
   * This is a wrapper around {@link WritableByteChannel#write(ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks. 
   * This is to avoid jdk from creating many direct buffers as the size of 
   * buffer increases. This also minimizes extra copies in NIO layer
   * as a result of multiple write operations required to write a large 
   * buffer.  
   *
   * @see WritableByteChannel#write(ByteBuffer)
   */
  private int channelWrite(WritableByteChannel channel, 
                           ByteBuffer buffer) throws IOException {
    
    int count =  (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
                 channel.write(buffer) : channelIO(null, channel, buffer);
    if (count > 0) {
      rpcMetrics.incrSentBytes(count);
    }
    return count;
  }
  
  
  /**
   * This is a wrapper around {@link ReadableByteChannel#read(ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks. 
   * This is to avoid jdk from creating many direct buffers as the size of 
   * ByteBuffer increases. There should not be any performance degredation.
   * 
   * @see ReadableByteChannel#read(ByteBuffer)
   */
  private int channelRead(ReadableByteChannel channel, 
                          ByteBuffer buffer) throws IOException {
    
    int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
                channel.read(buffer) : channelIO(channel, null, buffer);
    if (count > 0) {
      rpcMetrics.incrReceivedBytes(count);
    }
    return count;
  }
  
  /**
   * Helper for {@link #channelRead(ReadableByteChannel, ByteBuffer)}
   * and {@link #channelWrite(WritableByteChannel, ByteBuffer)}. Only
   * one of readCh or writeCh should be non-null.
   * 
   * @see #channelRead(ReadableByteChannel, ByteBuffer)
   * @see #channelWrite(WritableByteChannel, ByteBuffer)
   */
  private static int channelIO(ReadableByteChannel readCh, 
                               WritableByteChannel writeCh,
                               ByteBuffer buf) throws IOException {
    
    int originalLimit = buf.limit();
    int initialRemaining = buf.remaining();
    int ret = 0;
    
    while (buf.remaining() > 0) {
      try {
        int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
        buf.limit(buf.position() + ioSize);
        
        ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf); 
        
        if (ret < ioSize) {
          break;
        }

      } finally {
        buf.limit(originalLimit);        
      }
    }

    int nBytes = initialRemaining - buf.remaining(); 
    return (nBytes > 0) ? nBytes : ret;
  }
  
  private class ConnectionManager {
    final private AtomicInteger count = new AtomicInteger();
    final private AtomicLong droppedConnections = new AtomicLong();
    final private Set<Connection> connections;
    /* Map to maintain the statistics per User */
    final private Map<String, Integer> userToConnectionsMap;
    final private Object userToConnectionsMapLock = new Object();

    final private Timer idleScanTimer;
    final private int idleScanThreshold;
    final private int idleScanInterval;
    final private int maxIdleTime;
    final private int maxIdleToClose;
    final private int maxConnections;
    
    ConnectionManager() {
      this.idleScanTimer = new Timer(
          "IPC Server idle connection scanner for port " + getPort(), true);
      this.idleScanThreshold = conf.getInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_KEY,
          CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_DEFAULT);
      this.idleScanInterval = conf.getInt(
          CommonConfigurationKeys.IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_KEY,
          CommonConfigurationKeys.IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_DEFAULT);
      this.maxIdleTime = 2 * conf.getInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);
      this.maxIdleToClose = conf.getInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_KEY,
          CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_DEFAULT);
      this.maxConnections = conf.getInt(
          CommonConfigurationKeysPublic.IPC_SERVER_MAX_CONNECTIONS_KEY,
          CommonConfigurationKeysPublic.IPC_SERVER_MAX_CONNECTIONS_DEFAULT);
      // create a set with concurrency -and- a thread-safe iterator, add 2
      // for listener and idle closer threads
      this.connections = Collections.newSetFromMap(
          new ConcurrentHashMap<Connection,Boolean>(
              maxQueueSize, 0.75f, readThreads+2));
      this.userToConnectionsMap = new ConcurrentHashMap<>();
    }

    private boolean add(Connection connection) {
      boolean added = connections.add(connection);
      if (added) {
        count.getAndIncrement();
      }
      return added;
    }
    
    private boolean remove(Connection connection) {
      boolean removed = connections.remove(connection);
      if (removed) {
        count.getAndDecrement();
      }
      return removed;
    }

    void incrUserConnections(String user) {
      synchronized (userToConnectionsMapLock) {
        Integer count = userToConnectionsMap.get(user);
        if (count == null) {
          count = 1;
        } else {
          count++;
        }
        userToConnectionsMap.put(user, count);
      }
    }

    void decrUserConnections(String user) {
      synchronized (userToConnectionsMapLock) {
        Integer count = userToConnectionsMap.get(user);
        if (count == null) {
          return;
        } else {
          count--;
        }
        if (count == 0) {
          userToConnectionsMap.remove(user);
        } else {
          userToConnectionsMap.put(user, count);
        }
      }
    }

    Map<String, Integer> getUserToConnectionsMap() {
      return userToConnectionsMap;
    }


    long getDroppedConnections() {
      return droppedConnections.get();
    }

    int size() {
      return count.get();
    }

    boolean isFull() {
      // The check is disabled when maxConnections <= 0.
      return ((maxConnections > 0) && (size() >= maxConnections));
    }

    Connection[] toArray() {
      return connections.toArray(new Connection[0]);
    }

    Connection register(SocketChannel channel) {
      if (isFull()) {
        return null;
      }
      Connection connection = new Connection(channel, Time.now());
      add(connection);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Server connection from " + connection +
            "; # active connections: " + size() +
            "; # queued calls: " + callQueue.size());
      }      
      return connection;
    }
    
    boolean close(Connection connection) {
      boolean exists = remove(connection);
      if (exists) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(Thread.currentThread().getName() +
              ": disconnecting client " + connection +
              ". Number of active connections: "+ size());
        }
        // only close if actually removed to avoid double-closing due
        // to possible races
        connection.close();
        // Remove authorized users only
        if (connection.user != null && connection.connectionContextRead) {
          decrUserConnections(connection.user.getShortUserName());
        }
      }
      return exists;
    }
    
    // synch'ed to avoid explicit invocation upon OOM from colliding with
    // timer task firing
    synchronized void closeIdle(boolean scanAll) {
      long minLastContact = Time.now() - maxIdleTime;
      // concurrent iterator might miss new connections added
      // during the iteration, but that's ok because they won't
      // be idle yet anyway and will be caught on next scan
      int closed = 0;
      for (Connection connection : connections) {
        // stop if connections dropped below threshold unless scanning all
        if (!scanAll && size() < idleScanThreshold) {
          break;
        }
        // stop if not scanning all and max connections are closed
        if (connection.isIdle() &&
            connection.getLastContact() < minLastContact &&
            close(connection) &&
            !scanAll && (++closed == maxIdleToClose)) {
          break;
        }
      }
    }
    
    void closeAll() {
      // use a copy of the connections to be absolutely sure the concurrent
      // iterator doesn't miss a connection
      for (Connection connection : toArray()) {
        close(connection);
      }
    }
    
    void startIdleScan() {
      scheduleIdleScanTask();
    }
    
    void stopIdleScan() {
      idleScanTimer.cancel();
    }
    
    private void scheduleIdleScanTask() {
      if (!running) {
        return;
      }
      TimerTask idleScanTask = new TimerTask(){
        @Override
        public void run() {
          if (!running) {
            return;
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug(Thread.currentThread().getName()+": task running");
          }
          try {
            closeIdle(false);
          } finally {
            // explicitly reschedule so next execution occurs relative
            // to the end of this scan, not the beginning
            scheduleIdleScanTask();
          }
        }
      };
      idleScanTimer.schedule(idleScanTask, idleScanInterval);
    }
  }

  public String getServerName() {
    return serverName;
  }
}
