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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;
import org.apache.hadoop.ipc.RPC.RpcKind;
import org.apache.hadoop.ipc.Server.AuthProtocol;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.OperationProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SaslRpcClient;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ProtoUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.AsyncGet;
import org.apache.hadoop.tracing.Span;
import org.apache.hadoop.tracing.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.apache.hadoop.ipc.RpcConstants.CONNECTION_CONTEXT_CALL_ID;
import static org.apache.hadoop.ipc.RpcConstants.PING_CALL_ID;

/** A client for an IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @see Server
 */
@Public
@InterfaceStability.Evolving
public class Client implements AutoCloseable {
  public static final Logger LOG = LoggerFactory.getLogger(Client.class);

  /** A counter for generating call IDs. */
  private static final AtomicInteger callIdCounter = new AtomicInteger();

  private static final ThreadLocal<Integer> callId = new ThreadLocal<Integer>();
  private static final ThreadLocal<Integer> retryCount = new ThreadLocal<Integer>();
  private static final ThreadLocal<Object> EXTERNAL_CALL_HANDLER
      = new ThreadLocal<>();
  private static final ThreadLocal<CompletableFuture<Writable>> ASYNC_RPC_RESPONSE
      = new ThreadLocal<>();
  private static final ThreadLocal<Boolean> asynchronousMode =
      new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
          return false;
        }
      };

  @SuppressWarnings("unchecked")
  @Unstable
  public static <T extends Writable> AsyncGet<T, IOException>
      getAsyncRpcResponse() {
    CompletableFuture<Writable> responseFuture = ASYNC_RPC_RESPONSE.get();
    return new AsyncGet<T, IOException>() {
      @Override
      public T get(long timeout, TimeUnit unit)
          throws IOException, TimeoutException, InterruptedException {
        try {
          if (unit == null || timeout < 0) {
            return (T) responseFuture.get();
          }
          return (T) responseFuture.get(timeout, unit);
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof IOException) {
            throw (IOException) cause;
          }
          throw new IllegalStateException(e);
        }
      }

      @Override
      public boolean isDone() {
        return responseFuture.isDone();
      }
    };
  }

  /**
   * Retrieves the current response future from the thread-local storage.
   *
   * @return A {@link CompletableFuture} of type T that represents the
   *         asynchronous operation. If no response future is present in
   *         the thread-local storage, this method returns {@code null}.
   * @param <T> The type of the value completed by the returned
   *            {@link CompletableFuture}. It must be a subclass of
   *            {@link Writable}.
   * @see CompletableFuture
   * @see Writable
   */
  public static <T extends Writable> CompletableFuture<T> getResponseFuture() {
    return (CompletableFuture<T>) ASYNC_RPC_RESPONSE.get();
  }

  /**
   * Set call id and retry count for the next call.
   * @param cid input cid.
   * @param rc input rc.
   * @param externalHandler input externalHandler.
   */
  public static void setCallIdAndRetryCount(int cid, int rc,
                                            Object externalHandler) {
    Preconditions.checkArgument(cid != RpcConstants.INVALID_CALL_ID);
    Preconditions.checkState(callId.get() == null);
    Preconditions.checkArgument(rc != RpcConstants.INVALID_RETRY_COUNT);
    setCallIdAndRetryCountUnprotected(cid, rc, externalHandler);
  }

  public static void setCallIdAndRetryCountUnprotected(Integer cid, int rc,
      Object externalHandler) {
    callId.set(cid);
    retryCount.set(rc);
    EXTERNAL_CALL_HANDLER.set(externalHandler);
  }

  public static int getCallId() {
    return callId.get() != null ? callId.get() : nextCallId();
  }

  public static int getRetryCount() {
    return retryCount.get() != null ? retryCount.get() : 0;
  }

  public static Object getExternalHandler() {
    return EXTERNAL_CALL_HANDLER.get();
  }

  private final ConcurrentMap<ConnectionId, Connection> connections =
      new ConcurrentHashMap<>();
  private final Object putLock = new Object();
  private final Object emptyCondition = new Object();
  private final AtomicBoolean running = new AtomicBoolean(true);

  private Class<? extends Writable> valueClass;   // class of call values
  final private Configuration conf;

  private SocketFactory socketFactory;           // how to create sockets
  private final AtomicInteger refCount = new AtomicInteger(1);

  private final int connectionTimeout;

  private final boolean fallbackAllowed;
  private final boolean bindToWildCardAddress;
  private final byte[] clientId;
  private final int maxAsyncCalls;
  private final AtomicInteger asyncCallCounter = new AtomicInteger(0);

  /**
   * set the ping interval value in configuration
   * 
   * @param conf Configuration
   * @param pingInterval the ping interval
   */
  public static final void setPingInterval(Configuration conf,
      int pingInterval) {
    conf.setInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY, pingInterval);
  }

  /**
   * Get the ping interval from configuration;
   * If not set in the configuration, return the default value.
   * 
   * @param conf Configuration
   * @return the ping interval
   */
  public static final int getPingInterval(Configuration conf) {
    return conf.getInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY,
        CommonConfigurationKeys.IPC_PING_INTERVAL_DEFAULT);
  }

  /**
   * The time after which a RPC will timeout.
   * If ping is not enabled (via ipc.client.ping), then the timeout value is the 
   * same as the pingInterval.
   * If ping is enabled, then there is no timeout value.
   * 
   * @param conf Configuration
   * @return the timeout period in milliseconds. -1 if no timeout value is set
   * @deprecated use {@link #getRpcTimeout(Configuration)} instead
   */
  @Deprecated
  final public static int getTimeout(Configuration conf) {
    int timeout = getRpcTimeout(conf);
    if (timeout > 0)  {
      return timeout;
    }
    if (!conf.getBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY,
        CommonConfigurationKeys.IPC_CLIENT_PING_DEFAULT)) {
      return getPingInterval(conf);
    }
    return -1;
  }

  /**
   * The time after which a RPC will timeout.
   *
   * @param conf Configuration
   * @return the timeout period in milliseconds.
   */
  public static final int getRpcTimeout(Configuration conf) {
    int timeout =
        conf.getInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY,
            CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_DEFAULT);
    return (timeout < 0) ? 0 : timeout;
  }
  /**
   * set the connection timeout value in configuration
   * 
   * @param conf Configuration
   * @param timeout the socket connect timeout value
   */
  public static final void setConnectTimeout(Configuration conf, int timeout) {
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_KEY, timeout);
  }

  /**
   * Increment this client's reference count
   */
  void incCount() {
    refCount.incrementAndGet();
  }
  
  /**
   * Decrement this client's reference count
   */
  int decAndGetCount() {
    return refCount.decrementAndGet();
  }

  /** Check the rpc response header. */
  void checkResponse(RpcResponseHeaderProto header) throws IOException {
    if (header == null) {
      throw new EOFException("Response is null.");
    }
    if (header.hasClientId()) {
      // check client IDs
      final byte[] id = header.getClientId().toByteArray();
      if (!Arrays.equals(id, RpcConstants.DUMMY_CLIENT_ID)) {
        if (!Arrays.equals(id, clientId)) {
          throw new IOException("Client IDs not matched: local ID="
              + StringUtils.byteToHexString(clientId) + ", ID in response="
              + StringUtils.byteToHexString(header.getClientId().toByteArray()));
        }
      }
    }
  }

  Call createCall(RPC.RpcKind rpcKind, Writable rpcRequest) {
    return new Call(rpcKind, rpcRequest);
  }

  /** 
   * Class that represents an RPC call
   */
  static class Call {
    final int id;               // call id
    final int retry;           // retry count
    final Writable rpcRequest;  // the serialized rpc request
    private final CompletableFuture<Writable> rpcResponseFuture;
    final RPC.RpcKind rpcKind;      // Rpc EngineKind
    private final Object externalHandler;
    private AlignmentContext alignmentContext;

    private Call(RPC.RpcKind rpcKind, Writable param) {
      this.rpcKind = rpcKind;
      this.rpcRequest = param;

      final Integer id = callId.get();
      if (id == null) {
        this.id = nextCallId();
      } else {
        callId.set(null);
        this.id = id;
      }
      
      final Integer rc = retryCount.get();
      if (rc == null) {
        this.retry = 0;
      } else {
        this.retry = rc;
      }

      this.externalHandler = EXTERNAL_CALL_HANDLER.get();
      this.rpcResponseFuture = new CompletableFuture<>();
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + id;
    }

    /** Indicate when the call is complete and the
     * value or error are available.  Notifies by default.  */
    protected synchronized void callComplete() {
      if (externalHandler != null) {
        synchronized (externalHandler) {
          externalHandler.notify();
        }
      }
    }

    /**
     * Set an AlignmentContext for the call to update when call is done.
     *
     * @param ac alignment context to update.
     */
    public synchronized void setAlignmentContext(AlignmentContext ac) {
      this.alignmentContext = ac;
    }

    /** Set the exception when there is an error.
     * Notify the caller the call is done.
     * 
     * @param error exception thrown by the call; either local or remote
     */
    public synchronized void setException(IOException error) {
      rpcResponseFuture.completeExceptionally(error);
      callComplete();
    }
    
    /** Set the return value when there is no error. 
     * Notify the caller the call is done.
     * 
     * @param rpcResponse return value of the rpc call.
     */
    public synchronized void setRpcResponse(Writable rpcResponse) {
      rpcResponseFuture.complete(rpcResponse);
      callComplete();
    }
  }

  /** Thread that reads responses and notifies callers.  Each connection owns a
   * socket connected to a remote address.  Calls are multiplexed through this
   * socket: responses may be delivered out of order. */
  private class Connection extends Thread {
    private InetSocketAddress server;             // server ip:port
    private final ConnectionId remoteId;          // connection id
    private AuthMethod authMethod; // authentication method
    private AuthProtocol authProtocol;
    private int serviceClass;
    private SaslRpcClient saslRpcClient;
    
    private Socket socket = null;                 // connected socket
    private IpcStreams ipcStreams;
    private final int maxResponseLength;
    private final int rpcTimeout;
    private int maxIdleTime; //connections will be culled if it was idle for 
    //maxIdleTime msecs
    private final RetryPolicy connectionRetryPolicy;
    private final int maxRetriesOnSasl;
    private int maxRetriesOnSocketTimeouts;
    private final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
    private final boolean tcpLowLatency; // if T then use low-delay QoS
    private final boolean doPing; //do we need to send ping message
    private final int pingInterval; // how often sends ping to the server
    private final int soTimeout; // used by ipc ping and rpc timeout
    private byte[] pingRequest; // ping message

    // currently active calls
    private Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>();
    private AtomicLong lastActivity = new AtomicLong();// last I/O activity time
    private AtomicBoolean shouldCloseConnection = new AtomicBoolean();  // indicate if the connection is closed
    private IOException closeException; // close reason

    private final Thread rpcRequestThread;
    private final SynchronousQueue<Pair<Call, ResponseBuffer>> rpcRequestQueue =
        new SynchronousQueue<>(true);

    private AtomicReference<Thread> connectingThread = new AtomicReference<>();
    private final Consumer<Connection> removeMethod;

    Connection(ConnectionId remoteId, int serviceClass,
        Consumer<Connection> removeMethod) {
      this.remoteId = remoteId;
      this.server = remoteId.getAddress();
      this.rpcRequestThread = new Thread(new RpcRequestSender(),
          "IPC Parameter Sending Thread for " + remoteId);
      this.rpcRequestThread.setDaemon(true);

      this.maxResponseLength = remoteId.conf.getInt(
          CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH,
          CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT);
      this.rpcTimeout = remoteId.getRpcTimeout();
      this.maxIdleTime = remoteId.getMaxIdleTime();
      this.connectionRetryPolicy = remoteId.connectionRetryPolicy;
      this.maxRetriesOnSasl = remoteId.getMaxRetriesOnSasl();
      this.maxRetriesOnSocketTimeouts = remoteId.getMaxRetriesOnSocketTimeouts();
      this.tcpNoDelay = remoteId.getTcpNoDelay();
      this.tcpLowLatency = remoteId.getTcpLowLatency();
      this.doPing = remoteId.getDoPing();
      if (doPing) {
        // construct a RPC header with the callId as the ping callId
        ResponseBuffer buf = new ResponseBuffer();
        RpcRequestHeaderProto pingHeader = ProtoUtil
            .makeRpcRequestHeader(RpcKind.RPC_PROTOCOL_BUFFER,
                OperationProto.RPC_FINAL_PACKET, PING_CALL_ID,
                RpcConstants.INVALID_RETRY_COUNT, clientId);
        try {
          pingHeader.writeDelimitedTo(buf);
        } catch (IOException e) {
          throw new IllegalStateException("Failed to write to buf for "
              + remoteId + " in " + Client.this + " due to " + e, e);
        }
        pingRequest = buf.toByteArray();
      }
      this.pingInterval = remoteId.getPingInterval();
      if (rpcTimeout > 0) {
        // effective rpc timeout is rounded up to multiple of pingInterval
        // if pingInterval < rpcTimeout.
        this.soTimeout = (doPing && pingInterval < rpcTimeout) ?
            pingInterval : rpcTimeout;
      } else {
        this.soTimeout = pingInterval;
      }
      this.serviceClass = serviceClass;
      this.removeMethod = removeMethod;

      if (LOG.isDebugEnabled()) {
        LOG.debug("The ping interval is " + this.pingInterval + " ms.");
      }

      UserGroupInformation ticket = remoteId.getTicket();
      // try SASL if security is enabled or if the ugi contains tokens.
      // this causes a SIMPLE client with tokens to attempt SASL
      boolean trySasl = UserGroupInformation.isSecurityEnabled() ||
                        (ticket != null && !ticket.getTokens().isEmpty());
      this.authProtocol = trySasl ? AuthProtocol.SASL : AuthProtocol.NONE;
      
      this.setName("IPC Client (" + socketFactory.hashCode() +") connection to " +
          server.toString() +
          " from " + ((ticket==null)?"an unknown user":ticket.getUserName()));
      this.setDaemon(true);
    }

    /** Update lastActivity with the current time. */
    private void touch() {
      lastActivity.set(Time.now());
    }

    /**
     * Add a call to this connection's call queue and notify
     * a listener; synchronized.
     * Returns false if called during shutdown.
     * @param call to add
     * @return true if the call was added.
     */
    private synchronized boolean addCall(Call call) {
      if (shouldCloseConnection.get())
        return false;
      calls.put(call.id, call);
      notify();
      return true;
    }

    /** This class sends a ping to the remote side when timeout on
     * reading. If no failure is detected, it retries until at least
     * a byte is read.
     */
    private class PingInputStream extends FilterInputStream {
      /* constructor */
      protected PingInputStream(InputStream in) {
        super(in);
      }

      /* Process timeout exception
       * if the connection is not going to be closed or 
       * the RPC is not timed out yet, send a ping.
       */
      private void handleTimeout(SocketTimeoutException e, int waiting)
          throws IOException {
        if (shouldCloseConnection.get() || !running.get() ||
            (0 < rpcTimeout && rpcTimeout <= waiting)) {
          throw e;
        } else {
          sendPing();
        }
      }
      
      /** Read a byte from the stream.
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       * @throws IOException for any IO problem other than socket timeout
       */
      @Override
      public int read() throws IOException {
        int waiting = 0;
        do {
          try {
            return super.read();
          } catch (SocketTimeoutException e) {
            waiting += soTimeout;
            handleTimeout(e, waiting);
          }
        } while (true);
      }

      /** Read bytes into a buffer starting from offset <code>off</code>
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       * 
       * @return the total number of bytes read; -1 if the connection is closed.
       */
      @Override
      public int read(byte[] buf, int off, int len) throws IOException {
        int waiting = 0;
        do {
          try {
            return super.read(buf, off, len);
          } catch (SocketTimeoutException e) {
            waiting += soTimeout;
            handleTimeout(e, waiting);
          }
        } while (true);
      }
    }
    
    private synchronized void disposeSasl() {
      if (saslRpcClient != null) {
        try {
          saslRpcClient.dispose();
          saslRpcClient = null;
        } catch (IOException ignored) {
        }
      }
    }
    
    private synchronized boolean shouldAuthenticateOverKrb() throws IOException {
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      UserGroupInformation realUser = currentUser.getRealUser();
      if (authMethod == AuthMethod.KERBEROS && loginUser != null &&
      // Make sure user logged in using Kerberos either keytab or TGT
          loginUser.hasKerberosCredentials() &&
          // relogin only in case it is the login user (e.g. JT)
          // or superuser (like oozie).
          (loginUser.equals(currentUser) || loginUser.equals(realUser))) {
        return true;
      }
      return false;
    }

    private synchronized AuthMethod setupSaslConnection(IpcStreams streams)
        throws IOException {
      // Do not use Client.conf here! We must use ConnectionId.conf, since the
      // Client object is cached and shared between all RPC clients, even those
      // for separate services.
      saslRpcClient = new SaslRpcClient(remoteId.getTicket(),
          remoteId.getProtocol(), remoteId.getAddress(), remoteId.conf);
      return saslRpcClient.saslConnect(streams);
    }

    /**
     * Update the server address if the address corresponding to the host
     * name has changed.
     *
     * @return true if an addr change was detected.
     * @throws IOException when the hostname cannot be resolved.
     */
    private synchronized boolean updateAddress() throws IOException {
      // Do a fresh lookup with the old host name.
      InetSocketAddress currentAddr = NetUtils.createSocketAddrForHost(
                               server.getHostName(), server.getPort());

      if (!currentAddr.isUnresolved() && !server.equals(currentAddr)) {
        LOG.warn("Address change detected. Old: {} New: {}", server, currentAddr);
        server = currentAddr;
        // Update the remote address so that reconnections are with the updated address.
        // This avoids thrashing.
        remoteId.setAddress(currentAddr);
        UserGroupInformation ticket = remoteId.getTicket();
        this.setName("IPC Client (" + socketFactory.hashCode()
            + ") connection to " + server.toString() + " from "
            + ((ticket == null) ? "an unknown user" : ticket.getUserName()));
        return true;
      }
      return false;
    }
    
    private synchronized void setupConnection(
        UserGroupInformation ticket) throws IOException {
      LOG.debug("Setup connection to " + server.toString());
      short ioFailures = 0;
      short timeoutFailures = 0;
      while (true) {
        try {
          if (server.isUnresolved()) {
            // Jump into the catch block. updateAddress() will re-resolve
            // the address if this is just a temporary DNS failure. If not,
            // it will timeout after max ipc client retries
            throw NetUtils.wrapException(server.getHostName(),
                server.getPort(),
                NetUtils.getHostname(),
                0,
                new UnknownHostException());
          }
          this.socket = socketFactory.createSocket();
          this.socket.setTcpNoDelay(tcpNoDelay);
          this.socket.setKeepAlive(true);
          
          if (tcpLowLatency) {
            /*
             * This allows intermediate switches to shape IPC traffic
             * differently from Shuffle/HDFS DataStreamer traffic.
             *
             * IPTOS_RELIABILITY (0x04) | IPTOS_LOWDELAY (0x10)
             *
             * Prefer to optimize connect() speed & response latency over net
             * throughput.
             */
            this.socket.setTrafficClass(0x04 | 0x10);
            this.socket.setPerformancePreferences(1, 2, 0);
          }

          /*
           * Bind the socket to the host specified in the principal name of the
           * client, to ensure Server matching address of the client connection
           * to host name in principal passed.
           */
          InetSocketAddress bindAddr = null;
          if (ticket != null && ticket.hasKerberosCredentials()) {
            KerberosInfo krbInfo = 
              remoteId.getProtocol().getAnnotation(KerberosInfo.class);
            if (krbInfo != null) {
              String principal = ticket.getUserName();
              String host = SecurityUtil.getHostFromPrincipal(principal);
              // If host name is a valid local address then bind socket to it
              InetAddress localAddr = NetUtils.getLocalInetAddress(host);
              if (localAddr != null) {
                this.socket.setReuseAddress(true);
                localAddr = NetUtils.bindToLocalAddress(localAddr,
                    bindToWildCardAddress);
                LOG.debug("Binding {} to {}", principal,
                    (bindToWildCardAddress) ? "0.0.0.0" : localAddr);
                this.socket.bind(new InetSocketAddress(localAddr, 0));
              }
            }
          }
          
          NetUtils.connect(this.socket, server, bindAddr, connectionTimeout);
          this.socket.setSoTimeout(soTimeout);
          return;
        } catch (ConnectTimeoutException toe) {
          /* Check for an address change and update the local reference.
           * Reset the failure counter if the address was changed
           */
          if (updateAddress()) {
            timeoutFailures = ioFailures = 0;
          }
          handleConnectionTimeout(timeoutFailures++,
              maxRetriesOnSocketTimeouts, toe);
        } catch (IOException ie) {
          if (updateAddress()) {
            timeoutFailures = ioFailures = 0;
            try {
              // HADOOP-17068: when server changed, ignore the exception.
              handleConnectionFailure(ioFailures++, ie);
            } catch (IOException ioe) {
              LOG.warn("Exception when handle ConnectionFailure: "
                  + ioe.getMessage());
            }
          } else {
            handleConnectionFailure(ioFailures++, ie);
          }
        }
      }
    }

    /**
     * If multiple clients with the same principal try to connect to the same
     * server at the same time, the server assumes a replay attack is in
     * progress. This is a feature of kerberos. In order to work around this,
     * what is done is that the client backs off randomly and tries to initiate
     * the connection again. The other problem is to do with ticket expiry. To
     * handle that, a relogin is attempted.
     */
    private synchronized void handleSaslConnectionFailure(
        final int currRetries, final int maxRetries, final IOException ex,
        final Random rand, final UserGroupInformation ugi) throws IOException,
        InterruptedException {
      ugi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws IOException, InterruptedException {
          final short MAX_BACKOFF = 5000;
          closeConnection();
          disposeSasl();
          if (shouldAuthenticateOverKrb()) {
            if (currRetries < maxRetries) {
              LOG.debug("Exception encountered while connecting to the server {}", remoteId, ex);
              // try re-login
              if (UserGroupInformation.isLoginKeytabBased()) {
                UserGroupInformation.getLoginUser().reloginFromKeytab();
              } else if (UserGroupInformation.isLoginTicketBased()) {
                UserGroupInformation.getLoginUser().reloginFromTicketCache();
              }
              // have granularity of milliseconds
              //we are sleeping with the Connection lock held but since this
              //connection instance is being used for connecting to the server
              //in question, it is okay
              Thread.sleep((rand.nextInt(MAX_BACKOFF) + 1));
              return null;
            } else {
              String msg = "Couldn't setup connection for "
                  + UserGroupInformation.getLoginUser().getUserName() + " to "
                  + remoteId;
              LOG.warn(msg, ex);
              throw NetUtils.wrapException(remoteId.getAddress().getHostName(),
                  remoteId.getAddress().getPort(),
                  NetUtils.getHostname(),
                  0,
                  ex);
            }
          } else {
            // With RequestHedgingProxyProvider, one rpc call will send multiple
            // requests to all namenodes. After one request return successfully,
            // all other requests will be interrupted. It's not a big problem,
            // and should not print a warning log.
            if (ex instanceof InterruptedIOException) {
              LOG.debug("Exception encountered while connecting to the server {}", remoteId, ex);
            } else {
              LOG.warn("Exception encountered while connecting to the server {}", remoteId, ex);
            }
          }
          if (ex instanceof RemoteException)
            throw (RemoteException) ex;
          throw new IOException(ex);
        }
      });
    }

    
    /** Connect to the server and set up the I/O streams. It then sends
     * a header to the server and starts
     * the connection thread that waits for responses.
     */
    private synchronized void setupIOstreams(
        AtomicBoolean fallbackToSimpleAuth) {
      try {
        if (socket != null || shouldCloseConnection.get()) {
          setFallBackToSimpleAuth(fallbackToSimpleAuth);
          return;
        }
        UserGroupInformation ticket = remoteId.getTicket();
        if (ticket != null) {
          final UserGroupInformation realUser = ticket.getRealUser();
          if (realUser != null) {
            ticket = realUser;
          }
        }
        connectingThread.set(Thread.currentThread());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connecting to "+server);
        }
        Span span = Tracer.getCurrentSpan();
        if (span != null) {
          span.addTimelineAnnotation("IPC client connecting to " + server);
        }
        short numRetries = 0;
        Random rand = null;
        while (true) {
          setupConnection(ticket);
          ipcStreams = new IpcStreams(socket, maxResponseLength);
          writeConnectionHeader(ipcStreams);
          if (authProtocol == AuthProtocol.SASL) {
            try {
              authMethod = ticket
                  .doAs(new PrivilegedExceptionAction<AuthMethod>() {
                    @Override
                    public AuthMethod run()
                        throws IOException, InterruptedException {
                      return setupSaslConnection(ipcStreams);
                    }
                  });
            } catch (IOException ex) {
              if (saslRpcClient == null) {
                // whatever happened -it can't be handled, so rethrow
                throw ex;
              }
              // otherwise, assume a connection problem
              authMethod = saslRpcClient.getAuthMethod();
              if (rand == null) {
                rand = new Random();
              }
              handleSaslConnectionFailure(numRetries++, maxRetriesOnSasl, ex,
                  rand, ticket);
              continue;
            }
            if (authMethod != AuthMethod.SIMPLE) {
              // Sasl connect is successful. Let's set up Sasl i/o streams.
              ipcStreams.setSaslClient(saslRpcClient);
              // for testing
              remoteId.saslQop =
                  (String)saslRpcClient.getNegotiatedProperty(Sasl.QOP);
              LOG.debug("Negotiated QOP is :" + remoteId.saslQop);
            }
            setFallBackToSimpleAuth(fallbackToSimpleAuth);
          }

          if (doPing) {
            ipcStreams.setInputStream(new PingInputStream(ipcStreams.in));
          }

          writeConnectionContext(remoteId, authMethod);

          // update last activity time
          touch();

          span = Tracer.getCurrentSpan();
          if (span != null) {
            span.addTimelineAnnotation("IPC client connected to " + server);
          }

          // start the receiver thread after the socket connection has been set
          // up
          start();
          return;
        }
      } catch (Throwable t) {
        if (t instanceof IOException) {
          markClosed((IOException)t);
        } else {
          markClosed(new IOException("Couldn't set up IO streams: " + t, t));
        }
        close();
      } finally {
        connectingThread.set(null);
      }
    }

    private void setFallBackToSimpleAuth(AtomicBoolean fallbackToSimpleAuth)
        throws AccessControlException {
      if (authMethod == null || authProtocol != AuthProtocol.SASL) {
        if (authProtocol == AuthProtocol.SASL) {
          LOG.trace("Auth method is not set, yield from setting auth fallback.");
        }
        return;
      }
      if (fallbackToSimpleAuth == null) {
        // this should happen only during testing.
        LOG.trace("Connection {} will skip to set fallbackToSimpleAuth as it is null.", remoteId);
      } else {
        if (fallbackToSimpleAuth.get()) {
          // we already set the value to true, we do not need to examine again.
          return;
        }
      }
      if (authMethod != AuthMethod.SIMPLE) {
        if (fallbackToSimpleAuth != null) {
          LOG.trace("Disabling fallbackToSimpleAuth, target does not use SIMPLE authentication.");
          fallbackToSimpleAuth.set(false);
        }
      } else if (UserGroupInformation.isSecurityEnabled()) {
        if (!fallbackAllowed) {
          throw new AccessControlException("Server asks us to fall back to SIMPLE auth, but this "
              + "client is configured to only allow secure connections.");
        }
        if (fallbackToSimpleAuth != null) {
          LOG.trace("Enabling fallbackToSimpleAuth for target, as we are allowed to fall back.");
          fallbackToSimpleAuth.set(true);
        }
      }
    }

    private void closeConnection() {
      if (socket == null) {
        return;
      }
      // close the current connection
      try {
        socket.close();
      } catch (IOException e) {
        LOG.warn("Not able to close a socket", e);
      }
      // set socket to null so that the next call to setupIOstreams
      // can start the process of connect all over again.
      socket = null;
    }

    /* Handle connection failures due to timeout on connect
     *
     * If the current number of retries is equal to the max number of retries,
     * stop retrying and throw the exception; Otherwise backoff 1 second and
     * try connecting again.
     *
     * This Method is only called from inside setupIOstreams(), which is
     * synchronized. Hence the sleep is synchronized; the locks will be retained.
     *
     * @param curRetries current number of retries
     * @param maxRetries max number of retries allowed
     * @param ioe failure reason
     * @throws IOException if max number of retries is reached
     */
    private void handleConnectionTimeout(
        int curRetries, int maxRetries, IOException ioe) throws IOException {

      closeConnection();

      // throw the exception if the maximum number of retries is reached
      if (curRetries >= maxRetries) {
        throw ioe;
      }
      LOG.info("Retrying connect to server: " + server + ". Already tried "
          + curRetries + " time(s); maxRetries=" + maxRetries);
    }

    private void handleConnectionFailure(int curRetries, IOException ioe
        ) throws IOException {
      closeConnection();

      final RetryAction action;
      try {
        action = connectionRetryPolicy.shouldRetry(ioe, curRetries, 0, true);
      } catch(Exception e) {
        throw e instanceof IOException? (IOException)e: new IOException(e);
      }
      if (action.action == RetryAction.RetryDecision.FAIL) {
        if (action.reason != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Failed to connect to server: " + server + ": "
                    + action.reason, ioe);
          }
        }
        throw ioe;
      }

      // Throw the exception if the thread is interrupted
      if (Thread.currentThread().isInterrupted()) {
        LOG.warn("Interrupted while trying for connection");
        throw ioe;
      }

      try {
        Thread.sleep(action.delayMillis);
      } catch (InterruptedException e) {
        throw (IOException)new InterruptedIOException("Interrupted: action="
            + action + ", retry policy=" + connectionRetryPolicy).initCause(e);
      }
      LOG.info("Retrying connect to server: " + server + ". Already tried "
          + curRetries + " time(s); retry policy is " + connectionRetryPolicy);
    }

    /**
     * Write the connection header - this is sent when connection is established
     * +----------------------------------+
     * |  "hrpc" 4 bytes                  |      
     * +----------------------------------+
     * |  Version (1 byte)                |
     * +----------------------------------+
     * |  Service Class (1 byte)          |
     * +----------------------------------+
     * |  AuthProtocol (1 byte)           |      
     * +----------------------------------+
     */
    private void writeConnectionHeader(IpcStreams streams)
        throws IOException {
      // Write out the header, version and authentication method.
      // The output stream is buffered but we must not flush it yet.  The
      // connection setup protocol requires the client to send multiple
      // messages before reading a response.
      //
      //   insecure: send header+context+call, read
      //   secure  : send header+negotiate, read, (sasl), context+call, read
      //
      // The client must flush only when it's prepared to read.  Otherwise
      // "broken pipe" exceptions occur if the server closes the connection
      // before all messages are sent.
      final DataOutputStream out = streams.out;
      synchronized (out) {
        out.write(RpcConstants.HEADER.array());
        out.write(RpcConstants.CURRENT_VERSION);
        out.write(serviceClass);
        out.write(authProtocol.callId);
      }
    }

    /* Write the connection context header for each connection
     * Out is not synchronized because only the first thread does this.
     */
    private void writeConnectionContext(ConnectionId remoteId,
                                        AuthMethod authMethod)
                                            throws IOException {
      // Write out the ConnectionHeader
      IpcConnectionContextProto message = ProtoUtil.makeIpcConnectionContext(
          RPC.getProtocolName(remoteId.getProtocol()),
          remoteId.getTicket(),
          authMethod);
      RpcRequestHeaderProto connectionContextHeader = ProtoUtil
          .makeRpcRequestHeader(RpcKind.RPC_PROTOCOL_BUFFER,
              OperationProto.RPC_FINAL_PACKET, CONNECTION_CONTEXT_CALL_ID,
              RpcConstants.INVALID_RETRY_COUNT, clientId);
      // do not flush.  the context and first ipc call request must be sent
      // together to avoid possibility of broken pipes upon authz failure.
      // see writeConnectionHeader
      final ResponseBuffer buf = new ResponseBuffer();
      connectionContextHeader.writeDelimitedTo(buf);
      message.writeDelimitedTo(buf);
      synchronized (ipcStreams.out) {
        ipcStreams.sendRequest(buf.toByteArray());
      }
    }

    /* wait till someone signals us to start reading RPC response or
     * it is idle too long, it is marked as to be closed, 
     * or the client is marked as not running.
     * 
     * Return true if it is time to read a response; false otherwise.
     */
    private synchronized boolean waitForWork() {
      if (calls.isEmpty() && !shouldCloseConnection.get()  && running.get())  {
        long timeout = maxIdleTime-
              (Time.now()-lastActivity.get());
        if (timeout>0) {
          try {
            wait(timeout);
          } catch (InterruptedException e) {
            LOG.trace("Interrupted while waiting to retrieve RPC response.");
            Thread.currentThread().interrupt();
          }
        }
      }
      
      if (!calls.isEmpty() && !shouldCloseConnection.get() && running.get()) {
        return true;
      } else if (shouldCloseConnection.get()) {
        return false;
      } else if (calls.isEmpty()) { // idle connection closed or stopped
        markClosed(null);
        return false;
      } else { // get stopped but there are still pending requests 
        markClosed((IOException)new IOException().initCause(
            new InterruptedException()));
        return false;
      }
    }

    public InetSocketAddress getRemoteAddress() {
      return server;
    }

    /* Send a ping to the server if the time elapsed 
     * since last I/O activity is equal to or greater than the ping interval
     */
    private synchronized void sendPing() throws IOException {
      long curTime = Time.now();
      if ( curTime - lastActivity.get() >= pingInterval) {
        lastActivity.set(curTime);
        synchronized (ipcStreams.out) {
          ipcStreams.sendRequest(pingRequest);
          ipcStreams.flush();
        }
      }
    }

    @Override
    public void run() {
      try {
        // Don't start the ipc parameter sending thread until we start this
        // thread, because the shutdown logic only gets triggered if this
        // thread is started.
        rpcRequestThread.start();
        if (LOG.isDebugEnabled()) {
          LOG.debug(getName() + ": starting, having connections " + connections.size());
        }

        while (waitForWork()) {//wait here for work - read or close connection
          receiveRpcResponse();
        }
      } catch (Throwable t) {
        // This truly is unexpected, since we catch IOException in receiveResponse
        // -- this is only to be really sure that we don't leave a client hanging
        // forever.
        String msg = String.format("Unexpected error on connection %s. Closing it.", this);
        LOG.warn(msg, t);
        markClosed(new IOException(msg, t));
      }

      close();

      if (LOG.isDebugEnabled()) {
        LOG.debug(getName() + ": stopped, remaining connections "
            + connections.size());
      }
    }

    /**
     * A thread to write rpc requests to the socket.
     */
    private class RpcRequestSender implements Runnable {
      @Override
      public void run() {
        while (!shouldCloseConnection.get()) {
          ResponseBuffer buf = null;
          try {
            Pair<Call, ResponseBuffer> pair =
                rpcRequestQueue.poll(maxIdleTime, TimeUnit.MILLISECONDS);
            if (pair == null || shouldCloseConnection.get()) {
              continue;
            }
            buf = pair.getRight();
            synchronized (ipcStreams.out) {
              if (LOG.isDebugEnabled()) {
                Call call = pair.getLeft();
                LOG.debug("{} sending #{} {}", getName(), call.id, call.rpcRequest);
              }
              // RpcRequestHeader + RpcRequest
              ipcStreams.sendRequest(buf.toByteArray());
              ipcStreams.flush();
            }
          } catch (InterruptedException ie) {
            // stop this thread
            return;
          } catch (IOException e) {
            // exception at this point would leave the connection in an
            // unrecoverable state (eg half a call left on the wire).
            // So, close the connection, killing any outstanding calls
            markClosed(e);
          } finally {
            //the buffer is just an in-memory buffer, but it is still polite to
            // close early
            IOUtils.closeStream(buf);
          }
        }
      }
    }

    /** Initiates a rpc call by sending the rpc request to the remote server.
     * Note: this is not called from the current thread, but by another
     * thread, so that if the current thread is interrupted that the socket
     * state isn't corrupted with a partially written message.
     * @param call - the rpc request
     */
    public void sendRpcRequest(final Call call)
        throws InterruptedException, IOException {
      if (shouldCloseConnection.get()) {
        return;
      }

      // Serialize the call to be sent. This is done from the actual
      // caller thread, rather than the rpcRequestThread in the connection,
      // so that if the serialization throws an error, it is reported
      // properly. This also parallelizes the serialization.
      //
      // Format of a call on the wire:
      // 0) Length of rest below (1 + 2)
      // 1) RpcRequestHeader  - is serialized Delimited hence contains length
      // 2) RpcRequest
      //
      // Items '1' and '2' are prepared here. 
      RpcRequestHeaderProto header = ProtoUtil.makeRpcRequestHeader(
          call.rpcKind, OperationProto.RPC_FINAL_PACKET, call.id, call.retry,
          clientId, call.alignmentContext);

      final ResponseBuffer buf = new ResponseBuffer();
      header.writeDelimitedTo(buf);
      RpcWritable.wrap(call.rpcRequest).writeTo(buf);
      // Wait for the message to be sent. We offer with timeout to
      // prevent a race condition between checking the shouldCloseConnection
      // and the stopping of the polling thread
      while (!shouldCloseConnection.get()) {
        if (rpcRequestQueue.offer(Pair.of(call, buf), 1, TimeUnit.SECONDS)) {
          break;
        }
      }
    }

    /* Receive a response.
     * Because only one receiver, so no synchronization on in.
     */
    private void receiveRpcResponse() {
      if (shouldCloseConnection.get()) {
        return;
      }
      touch();
      
      try {
        ByteBuffer bb = ipcStreams.readResponse();
        RpcWritable.Buffer packet = RpcWritable.Buffer.wrap(bb);
        RpcResponseHeaderProto header =
            packet.getValue(RpcResponseHeaderProto.getDefaultInstance());
        checkResponse(header);

        int callId = header.getCallId();
        if (LOG.isDebugEnabled())
          LOG.debug(getName() + " got value #" + callId);

        RpcStatusProto status = header.getStatus();
        if (status == RpcStatusProto.SUCCESS) {
          Writable value = packet.newInstance(valueClass, conf);
          final Call call = calls.remove(callId);
          if (call.alignmentContext != null) {
            call.alignmentContext.receiveResponseState(header);
          }
          call.setRpcResponse(value);
        }
        // verify that packet length was correct
        if (packet.remaining() > 0) {
          throw new RpcClientException("RPC response length mismatch");
        }
        if (status != RpcStatusProto.SUCCESS) { // Rpc Request failed
          final String exceptionClassName = header.hasExceptionClassName() ?
                header.getExceptionClassName() : 
                  "ServerDidNotSetExceptionClassName";
          final String errorMsg = header.hasErrorMsg() ? 
                header.getErrorMsg() : "ServerDidNotSetErrorMsg" ;
          final RpcErrorCodeProto erCode = 
                    (header.hasErrorDetail() ? header.getErrorDetail() : null);
          if (erCode == null) {
             LOG.warn("Detailed error code not set by server on rpc error");
          }
          RemoteException re = new RemoteException(exceptionClassName, errorMsg, erCode);
          if (status == RpcStatusProto.ERROR) {
            final Call call = calls.remove(callId);
            call.setException(re);
          } else if (status == RpcStatusProto.FATAL) {
            // Close the connection
            markClosed(re);
          }
        }
      } catch (IOException e) {
        markClosed(e);
      }
    }
    
    private synchronized void markClosed(IOException e) {
      if (shouldCloseConnection.compareAndSet(false, true)) {
        closeException = e;
        notifyAll();
      }
    }

    private void interruptConnectingThread() {
      Thread connThread = connectingThread.get();
      if (connThread != null) {
        connThread.interrupt();
      }
    }
    
    /** Close the connection. */
    private synchronized void close() {
      if (!shouldCloseConnection.get()) {
        LOG.error("The connection is not in the closed state");
        return;
      }

      // We have marked this connection as closed. Other thread could have
      // already known it and replace this closedConnection with a new one.
      // We should only remove this closedConnection.
      removeMethod.accept(this);

      // close the streams and therefore the socket
      IOUtils.closeStream(ipcStreams);
      disposeSasl();

      // clean up all calls
      if (closeException == null) {
        if (!calls.isEmpty()) {
          LOG.warn(
              "A connection is closed for no cause and calls are not empty");

          // clean up calls anyway
          closeException = new IOException("Unexpected closed connection");
          cleanupCalls();
        }
      } else {
        // Log the newest server information if update address.
        if (LOG.isDebugEnabled()) {
          LOG.debug("closing ipc connection to " + server + ": " +
              closeException.getMessage(),closeException);
        }

        // cleanup calls
        cleanupCalls();
      }
      closeConnection();
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": closed");
    }
    
    /* Cleanup all calls and mark them as done */
    private void cleanupCalls() {
      Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator() ;
      while (itor.hasNext()) {
        Call c = itor.next().getValue(); 
        itor.remove();
        c.setException(closeException); // local exception
      }
    }
  }

  /**
   * Construct an IPC client whose values are of the given {@link Writable}
   * class.
   *
   * @param valueClass input valueClass.
   * @param conf input configuration.
   * @param factory input factory.
   */
  public Client(Class<? extends Writable> valueClass, Configuration conf, 
      SocketFactory factory) {
    this.valueClass = valueClass;
    this.conf = conf;
    this.socketFactory = factory;
    this.connectionTimeout = conf.getInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_KEY,
        CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_DEFAULT);
    this.fallbackAllowed = conf.getBoolean(CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
        CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT);
    this.bindToWildCardAddress = conf
        .getBoolean(CommonConfigurationKeys.IPC_CLIENT_BIND_WILDCARD_ADDR_KEY,
            CommonConfigurationKeys.IPC_CLIENT_BIND_WILDCARD_ADDR_DEFAULT);

    this.clientId = ClientId.getClientId();
    this.maxAsyncCalls = conf.getInt(
        CommonConfigurationKeys.IPC_CLIENT_ASYNC_CALLS_MAX_KEY,
        CommonConfigurationKeys.IPC_CLIENT_ASYNC_CALLS_MAX_DEFAULT);
  }

  /**
   * Construct an IPC client with the default SocketFactory.
   * @param valueClass input valueClass.
   * @param conf input Configuration.
   */
  public Client(Class<? extends Writable> valueClass, Configuration conf) {
    this(valueClass, conf, NetUtils.getDefaultSocketFactory(conf));
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "-"
        + StringUtils.byteToHexString(clientId);
  }

  /** Return the socket factory of this client
   *
   * @return this client's socket factory
   */
  SocketFactory getSocketFactory() {
    return socketFactory;
  }

  /** Stop all threads related to this client.  No further calls may be made
   * using this client. */
  public void stop() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping client");
    }
    synchronized (putLock) { // synchronized to avoid put after stop
      if (!running.compareAndSet(true, false)) {
        return;
      }
    }

    // wake up all connections
    for (Connection conn : connections.values()) {
      conn.interrupt();
      conn.rpcRequestThread.interrupt();
      conn.interruptConnectingThread();
    }
    
    // wait until all connections are closed
    synchronized (emptyCondition) {
      // synchronized the loop to guarantee wait must be notified.
      while (!connections.isEmpty()) {
        try {
          emptyCondition.wait();
        } catch (InterruptedException e) {
          LOG.trace(
              "Interrupted while waiting on all connections to be closed.");
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /** 
   * Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
   * <code>remoteId</code>, returning the rpc respond.
   *
   * @param rpcKind - input rpcKind.
   * @param rpcRequest -  contains serialized method and method parameters
   * @param remoteId - the target rpc server
   * @param fallbackToSimpleAuth - set to true or false during this method to
   *   indicate if a secure client falls back to simple auth
   * @return the rpc response
   * Throws exceptions if there are network problems or if the remote code
   * threw an exception.
   * @throws IOException raised on errors performing I/O.
   */
  public Writable call(RPC.RpcKind rpcKind, Writable rpcRequest,
      ConnectionId remoteId, AtomicBoolean fallbackToSimpleAuth)
      throws IOException {
    return call(rpcKind, rpcRequest, remoteId, RPC.RPC_SERVICE_CLASS_DEFAULT,
      fallbackToSimpleAuth, null);
  }

  public Writable call(RPC.RpcKind rpcKind, Writable rpcRequest,
      ConnectionId remoteId, AtomicBoolean fallbackToSimpleAuth,
      AlignmentContext alignmentContext)
      throws IOException {
    return call(rpcKind, rpcRequest, remoteId, RPC.RPC_SERVICE_CLASS_DEFAULT,
        fallbackToSimpleAuth, alignmentContext);
  }

  private void checkAsyncCall() throws IOException {
    if (isAsynchronousMode()) {
      if (asyncCallCounter.incrementAndGet() > maxAsyncCalls) {
        asyncCallCounter.decrementAndGet();
        String errMsg = String.format(
            "Exceeded limit of max asynchronous calls: %d, " +
            "please configure %s to adjust it.",
            maxAsyncCalls,
            CommonConfigurationKeys.IPC_CLIENT_ASYNC_CALLS_MAX_KEY);
        throw new AsyncCallLimitExceededException(errMsg);
      }
    }
  }

  Writable call(RPC.RpcKind rpcKind, Writable rpcRequest,
                ConnectionId remoteId, int serviceClass,
                AtomicBoolean fallbackToSimpleAuth)
      throws IOException {
    return call(rpcKind, rpcRequest, remoteId, serviceClass,
        fallbackToSimpleAuth, null);
  }

  /**
   * Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
   * <code>remoteId</code>, returning the rpc response.
   *
   * @param rpcKind
   * @param rpcRequest -  contains serialized method and method parameters
   * @param remoteId - the target rpc server
   * @param serviceClass - service class for RPC
   * @param fallbackToSimpleAuth - set to true or false during this method to
   *   indicate if a secure client falls back to simple auth
   * @param alignmentContext - state alignment context
   * @return the rpc response
   * Throws exceptions if there are network problems or if the remote code
   * threw an exception.
   */
  Writable call(RPC.RpcKind rpcKind, Writable rpcRequest,
      ConnectionId remoteId, int serviceClass,
      AtomicBoolean fallbackToSimpleAuth, AlignmentContext alignmentContext)
      throws IOException {
    final Call call = createCall(rpcKind, rpcRequest);
    call.setAlignmentContext(alignmentContext);
    final Connection connection = getConnection(remoteId, call, serviceClass,
        fallbackToSimpleAuth);

    try {
      checkAsyncCall();
      try {
        connection.sendRpcRequest(call);                 // send the rpc request
      } catch (RejectedExecutionException e) {
        throw new IOException("connection has been closed", e);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        IOException ioe = new InterruptedIOException(
            "Interrupted waiting to send RPC request to server");
        ioe.initCause(ie);
        throw ioe;
      }
    } catch(Exception e) {
      if (isAsynchronousMode()) {
        releaseAsyncCall();
      }
      throw e;
    }

    if (isAsynchronousMode()) {
      CompletableFuture<Writable> result = call.rpcResponseFuture.handle(
          (rpcResponse, e) -> {
            releaseAsyncCall();
            if (e != null) {
              IOException ioe = (IOException) e;
              throw new CompletionException(warpIOException(ioe, connection));
            }
            return rpcResponse;
          });
      ASYNC_RPC_RESPONSE.set(result);
      return null;
    } else {
      return getRpcResponse(call, connection);
    }
  }

  /**
   * Check if RPC is in asynchronous mode or not.
   *
   * @return true, if RPC is in asynchronous mode, otherwise false for
   *          synchronous mode.
   */
  @Unstable
  public static boolean isAsynchronousMode() {
    return asynchronousMode.get();
  }

  /**
   * Set RPC to asynchronous or synchronous mode.
   *
   * @param async
   *          true, RPC will be in asynchronous mode, otherwise false for
   *          synchronous mode
   */
  @Unstable
  public static void setAsynchronousMode(boolean async) {
    asynchronousMode.set(async);
  }

  private void releaseAsyncCall() {
    asyncCallCounter.decrementAndGet();
  }

  @VisibleForTesting
  int getAsyncCallCount() {
    return asyncCallCounter.get();
  }

  /** @return the rpc response or, in case of timeout, null. */
  private Writable getRpcResponse(final Call call, final Connection connection)
      throws IOException {
    try {
      return call.rpcResponseFuture.get();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new InterruptedIOException("Call interrupted");
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw warpIOException((IOException) cause, connection);
      }
      throw new IllegalStateException(e);
    }
  }

  private IOException warpIOException(IOException ioe, Connection connection) {
    if (ioe instanceof RemoteException ||
        ioe instanceof SaslException) {
      ioe.fillInStackTrace();
      return ioe;
    } else { // local exception
      InetSocketAddress address = connection.getRemoteAddress();
      return NetUtils.wrapException(address.getHostName(),
          address.getPort(),
          NetUtils.getHostname(),
          0,
          ioe);
    }
  }

  // for unit testing only
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  Set<ConnectionId> getConnectionIds() {
    return connections.keySet();
  }
  
  /** Get a connection from the pool, or create a new one and add it to the
   * pool.  Connections to a given ConnectionId are reused. */
  private Connection getConnection(ConnectionId remoteId,
      Call call, int serviceClass, AtomicBoolean fallbackToSimpleAuth)
      throws IOException {
    final Consumer<Connection> removeMethod = c -> {
      final boolean removed = connections.remove(remoteId, c);
      if (removed && connections.isEmpty()) {
        synchronized (emptyCondition) {
          emptyCondition.notify();
        }
      }
    };

    Connection connection;
    /* we could avoid this allocation for each RPC by having a  
     * connectionsId object and with set() method. We need to manage the
     * refs for keys in HashMap properly. For now its ok.
     */
    while (true) {
      synchronized (putLock) { // synchronized to avoid put after stop
        if (!running.get()) {
          throw new IOException("Failed to get connection for " + remoteId
              + ", " + call + ": " + this + " is already stopped");
        }
        connection = connections.computeIfAbsent(remoteId,
            id -> new Connection(id, serviceClass, removeMethod));
      }

      if (connection.addCall(call)) {
        break;
      } else {
        // This connection is closed, should be removed. But other thread could
        // have already known this closedConnection, and replace it with a new
        // connection. So we should call conditional remove to make sure we only
        // remove this closedConnection.
        removeMethod.accept(connection);
      }
    }

    // If the server happens to be slow, the method below will take longer to
    // establish a connection.
    connection.setupIOstreams(fallbackToSimpleAuth);
    return connection;
  }
  
  /**
   * This class holds the address and the user ticket. The client connections
   * to servers are uniquely identified by {@literal <}remoteAddress, protocol,
   * ticket{@literal >}
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  @InterfaceStability.Evolving
  public static class ConnectionId {
    private InetSocketAddress address;
    private final UserGroupInformation ticket;
    private final Class<?> protocol;
    private static final int PRIME = 16777619;
    private final int rpcTimeout;
    private final int maxIdleTime; //connections will be culled if it was idle for 
    //maxIdleTime msecs
    private final RetryPolicy connectionRetryPolicy;
    private final int maxRetriesOnSasl;
    // the max. no. of retries for socket connections on time out exceptions
    private final int maxRetriesOnSocketTimeouts;
    private final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
    private final boolean tcpLowLatency; // if T then use low-delay QoS
    private final boolean doPing; //do we need to send ping message
    private final int pingInterval; // how often sends ping to the server in msecs
    private String saslQop; // here for testing
    private final Configuration conf; // used to get the expected kerberos principal name

    public ConnectionId(InetSocketAddress address, Class<?> protocol,
                 UserGroupInformation ticket, int rpcTimeout,
                 RetryPolicy connectionRetryPolicy, Configuration conf) {
      this.protocol = protocol;
      this.address = address;
      this.ticket = ticket;
      this.rpcTimeout = rpcTimeout;
      this.connectionRetryPolicy = connectionRetryPolicy;

      this.maxIdleTime = conf.getInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);
      this.maxRetriesOnSasl = conf.getInt(
          CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY,
          CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_DEFAULT);
      this.maxRetriesOnSocketTimeouts = conf.getInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT);
      this.tcpNoDelay = conf.getBoolean(
          CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_KEY,
          CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_DEFAULT);
      this.tcpLowLatency = conf.getBoolean(
          CommonConfigurationKeysPublic.IPC_CLIENT_LOW_LATENCY,
          CommonConfigurationKeysPublic.IPC_CLIENT_LOW_LATENCY_DEFAULT
          );
      this.doPing = conf.getBoolean(
          CommonConfigurationKeys.IPC_CLIENT_PING_KEY,
          CommonConfigurationKeys.IPC_CLIENT_PING_DEFAULT);
      this.pingInterval = (doPing ? Client.getPingInterval(conf) : 0);
      this.conf = conf;
    }
    
    InetSocketAddress getAddress() {
      return address;
    }

    /**
     * This is used to update the remote address when an address change is detected.  This method
     * ensures that the {@link #hashCode()} won't change.
     *
     * @param address the updated address
     * @throws IllegalArgumentException if the hostname or port doesn't match
     * @see Connection#updateAddress()
     */
    void setAddress(InetSocketAddress address) {
      if (!Objects.equals(this.address.getHostName(), address.getHostName())) {
        throw new IllegalArgumentException("Hostname must match: " + this.address + " vs "
            + address);
      }
      if (this.address.getPort() != address.getPort()) {
        throw new IllegalArgumentException("Port must match: " + this.address + " vs " + address);
      }

      this.address = address;
    }


    Class<?> getProtocol() {
      return protocol;
    }
    
    UserGroupInformation getTicket() {
      return ticket;
    }
    
    int getRpcTimeout() {
      return rpcTimeout;
    }
    
    int getMaxIdleTime() {
      return maxIdleTime;
    }
    
    public int getMaxRetriesOnSasl() {
      return maxRetriesOnSasl;
    }

    /** @return max connection retries on socket time outs */
    public int getMaxRetriesOnSocketTimeouts() {
      return maxRetriesOnSocketTimeouts;
    }

    /** disable nagle's algorithm */
    boolean getTcpNoDelay() {
      return tcpNoDelay;
    }

    /** use low-latency QoS bits over TCP */
    boolean getTcpLowLatency() {
      return tcpLowLatency;
    }

    boolean getDoPing() {
      return doPing;
    }
    
    int getPingInterval() {
      return pingInterval;
    }

    RetryPolicy getRetryPolicy() {
      return connectionRetryPolicy;
    }
    
    @VisibleForTesting
    String getSaslQop() {
      return saslQop;
    }
    
    /**
     * Returns a ConnectionId object. 
     * @param addr Remote address for the connection.
     * @param protocol Protocol for RPC.
     * @param ticket UGI
     * @param rpcTimeout timeout
     * @param conf Configuration object
     * @return A ConnectionId instance
     * @throws IOException
     */
    static ConnectionId getConnectionId(InetSocketAddress addr,
        Class<?> protocol, UserGroupInformation ticket, int rpcTimeout,
        RetryPolicy connectionRetryPolicy, Configuration conf) throws IOException {

      if (connectionRetryPolicy == null) {
        final int max = conf.getInt(
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT);
        final int retryInterval = conf.getInt(
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY,
            CommonConfigurationKeysPublic
                .IPC_CLIENT_CONNECT_RETRY_INTERVAL_DEFAULT);

        connectionRetryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
            max, retryInterval, TimeUnit.MILLISECONDS);
      }

      return new ConnectionId(addr, protocol, ticket, rpcTimeout,
          connectionRetryPolicy, conf);
    }
    
    static boolean isEqual(Object a, Object b) {
      return a == null ? b == null : a.equals(b);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj instanceof ConnectionId) {
        ConnectionId that = (ConnectionId) obj;
        return isEqual(this.address, that.address)
            && this.doPing == that.doPing
            && this.maxIdleTime == that.maxIdleTime
            && isEqual(this.connectionRetryPolicy, that.connectionRetryPolicy)
            && this.pingInterval == that.pingInterval
            && isEqual(this.protocol, that.protocol)
            && this.rpcTimeout == that.rpcTimeout
            && this.tcpNoDelay == that.tcpNoDelay
            && isEqual(this.ticket, that.ticket);
      }
      return false;
    }
    
    @Override
    public int hashCode() {
      int result = connectionRetryPolicy.hashCode();
      // We calculate based on the host name and port without the IP address, since the hashCode
      // must be stable even if the IP address is updated.
      result = PRIME * result + ((address == null || address.getHostName() == null) ? 0 :
          address.getHostName().hashCode());
      result = PRIME * result + ((address == null) ? 0 : address.getPort());
      result = PRIME * result + (doPing ? 1231 : 1237);
      result = PRIME * result + maxIdleTime;
      result = PRIME * result + pingInterval;
      result = PRIME * result + ((protocol == null) ? 0 : protocol.hashCode());
      result = PRIME * result + rpcTimeout;
      result = PRIME * result + (tcpNoDelay ? 1231 : 1237);
      result = PRIME * result + ((ticket == null) ? 0 : ticket.hashCode());
      return result;
    }
    
    @Override
    public String toString() {
      return address.toString();
    }
  }  

  /**
   * Returns the next valid sequential call ID by incrementing an atomic counter
   * and masking off the sign bit.  Valid call IDs are non-negative integers in
   * the range [ 0, 2^31 - 1 ].  Negative numbers are reserved for special
   * purposes.  The values can overflow back to 0 and be reused.  Note that prior
   * versions of the client did not mask off the sign bit, so a server may still
   * see a negative call ID if it receives connections from an old client.
   * 
   * @return next call ID
   */
  public static int nextCallId() {
    return callIdCounter.getAndIncrement() & 0x7FFFFFFF;
  }

  @Override
  @Unstable
  public void close() throws Exception {
    stop();
  }

  /** Manages the input and output streams for an IPC connection.
   *  Only exposed for use by SaslRpcClient.
   */
  @InterfaceAudience.Private
  public static class IpcStreams implements Closeable, Flushable {
    private DataInputStream in;
    public DataOutputStream out;
    private int maxResponseLength;
    private boolean firstResponse = true;

    IpcStreams(Socket socket, int maxResponseLength) throws IOException {
      this.maxResponseLength = maxResponseLength;
      setInputStream(
          new BufferedInputStream(NetUtils.getInputStream(socket)));
      setOutputStream(
          new BufferedOutputStream(NetUtils.getOutputStream(socket)));
    }

    void setSaslClient(SaslRpcClient client) throws IOException {
      // Wrap the input stream in a BufferedInputStream to fill the buffer
      // before reading its length (HADOOP-14062).
      setInputStream(new BufferedInputStream(client.getInputStream(in)));
      setOutputStream(client.getOutputStream(out));
    }

    private void setInputStream(InputStream is) {
      this.in = (is instanceof DataInputStream)
          ? (DataInputStream)is : new DataInputStream(is);
    }

    private void setOutputStream(OutputStream os) {
      this.out = (os instanceof DataOutputStream)
          ? (DataOutputStream)os : new DataOutputStream(os);
    }

    public ByteBuffer readResponse() throws IOException {
      int length = in.readInt();
      if (firstResponse) {
        firstResponse = false;
        // pre-rpcv9 exception, almost certainly a version mismatch.
        if (length == -1) {
          in.readInt(); // ignore fatal/error status, it's fatal for us.
          throw new RemoteException(WritableUtils.readString(in),
                                    WritableUtils.readString(in));
        }
      }
      if (length <= 0) {
        throw new RpcException(String.format("RPC response has " +
            "invalid length of %d", length));
      }
      if (maxResponseLength > 0 && length > maxResponseLength) {
        throw new RpcException(String.format("RPC response has a " +
            "length of %d exceeds maximum data length", length));
      }
      ByteBuffer bb = ByteBuffer.allocate(length);
      in.readFully(bb.array());
      return bb;
    }

    public void sendRequest(byte[] buf) throws IOException {
      out.write(buf);
    }

    @Override
    public void flush() throws IOException {
      out.flush();
    }

    @Override
    public void close() {
      IOUtils.closeStream(out);
      IOUtils.closeStream(in);
    }
  }
}
