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

package org.apache.hadoop.ipc.netty.server;

import com.sun.xml.bind.v2.TODO;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.IpcException;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.ipc.RpcConstants;
import org.apache.hadoop.ipc.RpcServerException;
import org.apache.hadoop.ipc.RpcWritable;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.Message;
import org.apache.hadoop.tracing.Span;
import org.apache.hadoop.tracing.SpanContext;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.tracing.Tracer;
import org.apache.hadoop.util.ProtoUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.ipc.RpcConstants.AUTHORIZATION_FAILED_CALL_ID;
import static org.apache.hadoop.ipc.RpcConstants.CONNECTION_CONTEXT_CALL_ID;
import static org.apache.hadoop.ipc.RpcConstants.CURRENT_VERSION;
import static org.apache.hadoop.ipc.RpcConstants.HEADER_LEN_AFTER_HRPC_PART;
import static org.apache.hadoop.ipc.RpcConstants.PING_CALL_ID;

/**
 * Reads calls from a connection and queues them for handling.
 */
public abstract class Connection<T> {
  private final Server server;
  private boolean connectionHeaderRead = false; // connection  header is read?
  public boolean connectionContextRead = false; //if connection context that
  //follows connection header is read

  public final T channel;
  private ByteBuffer data;
  private ByteBuffer dataLengthBuffer;
  // number of outstanding rpcs
  private final AtomicInteger rpcCount = new AtomicInteger();
  private long lastContact;
  private int dataLength;
  // Cache the remote host & port info so that even if the socket is
  // disconnected, we can say where it used to connect to.
  private final String hostAddress;
  private final int remotePort;
  private final InetAddress addr;

  IpcConnectionContextProtos.IpcConnectionContextProto connectionContext;
  public String protocolName;
  public SaslServer saslServer;
  private String establishedQOP;
  private SaslRpcServer.AuthMethod authMethod;
  private Server.AuthProtocol authProtocol;
  private boolean saslContextEstablished;
  private ByteBuffer connectionHeaderBuf = null;
  private ByteBuffer unwrappedData;
  private final ByteBuffer unwrappedDataLengthBuffer;
  private int serviceClass;
  private boolean shouldClose = false;
  private final int ingressPort;
  private boolean isOnAuxiliaryPort;

  public UserGroupInformation user = null;
  public UserGroupInformation attemptingUser = null; // user name before auth

  // Fake 'call' for failed authorization response
  private final Server.RpcCall authFailedCall;

  private boolean sentNegotiate = false;
  public boolean useWrap = false;

  public abstract boolean isOpen();

  protected abstract void setSendBufferSize(T channel, int size) throws
      IOException;

  protected abstract int bufferRead(Object in, ByteBuffer buf)
      throws IOException;

  public Connection(Server server, T channel, InetSocketAddress localAddr,
                    InetSocketAddress remoteAddr) {
    this.server = server;
    this.channel = channel;
    this.lastContact = Time.now();
    this.data = null;

    authFailedCall = server.getRpcCall(this,
        AUTHORIZATION_FAILED_CALL_ID);

    // the buffer is initialized to read the "hrpc" and after that to read
    // the length of the Rpc-packet (i.e 4 bytes)
    this.dataLengthBuffer = ByteBuffer.allocate(4);
    this.unwrappedData = null;
    this.unwrappedDataLengthBuffer = ByteBuffer.allocate(4);
    this.addr = remoteAddr.getAddress();
    this.ingressPort = localAddr.getPort();
    // TODO: This variable is being initialized to itself. Why ?
    this.isOnAuxiliaryPort = isOnAuxiliaryPort;
    if (addr == null) {
      this.hostAddress = "*Unknown*";
    } else {
      this.hostAddress = addr.getHostAddress();
    }
    this.remotePort = remoteAddr.getPort();
    if (server.socketSendBufferSize != 0) {
      try {
        setSendBufferSize(channel, server.socketSendBufferSize);
      } catch (IOException e) {
        Server.LOG.warn(
            "Connection: unable to set socket send buffer size to " +
                server.socketSendBufferSize);
      }
    }
  }

  @Override
  public String toString() {
    return getHostAddress() + ":" + remotePort;
  }

  public boolean setShouldClose() {
    return shouldClose = true;
  }

  public boolean shouldClose() {
    return shouldClose;
  }

  public String getHostAddress() {
    return hostAddress;
  }

  public int getIngressPort() {
    return ingressPort;
  }

  public int getRemotePort() {
    return remotePort;
  }

  public InetAddress getHostInetAddress() {
    return addr;
  }

  public String getEstablishedQOP() {
    return establishedQOP;
  }

  public boolean isOnAuxiliaryPort() {
    return isOnAuxiliaryPort;
  }

  public void setLastContact(long lastContact) {
    this.lastContact = lastContact;
  }

  public long getLastContact() {
    return lastContact;
  }

  public Server getServer() {
    return server;
  }

  /* Return true if the connection has no outstanding rpc */
  public boolean isIdle() {
    return rpcCount.get() == 0;
  }

  /* Decrement the outstanding RPC count */
  public void decRpcCount() {
    rpcCount.decrementAndGet();
  }

  /* Increment the outstanding RPC count */
  private void incRpcCount() {
    rpcCount.incrementAndGet();
  }

  private UserGroupInformation getAuthorizedUgi(String authorizedId)
      throws SecretManager.InvalidToken, AccessControlException {
    if (authMethod == SaslRpcServer.AuthMethod.TOKEN) {
      TokenIdentifier tokenId = SaslRpcServer.getIdentifier(authorizedId,
          server.secretManager);
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
    final RpcHeaderProtos.RpcSaslProto saslMessage =
        getMessage(RpcHeaderProtos.RpcSaslProto.getDefaultInstance(), buffer);
    switch (saslMessage.getState()) {
      case WRAP: {
        if (!saslContextEstablished || !useWrap) {
          throw new Server.FatalRpcServerException(
              RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
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
   * {@link SecretManager.InvalidToken} we go one level deeper to get the true cause.
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
      } else if (cause instanceof SecretManager.InvalidToken) {
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
   *
   * @param saslMessage received SASL message
   * @throws RpcServerException   setup failed due to SASL negotiation
   *                              failure, premature or invalid connection context, or other state
   *                              errors. This exception needs to be sent to the client. This
   *                              exception will wrap {@link RetriableException},
   *                              {@link SecretManager.InvalidToken}, {@link StandbyException} or
   *                              {@link SaslException}.
   * @throws IOException          if sending reply fails
   * @throws InterruptedException
   */
  private void saslProcess(RpcHeaderProtos.RpcSaslProto saslMessage)
      throws RpcServerException, IOException, InterruptedException {
    if (saslContextEstablished) {
      throw new Server.FatalRpcServerException(
          RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
          new SaslException("Negotiation is already complete"));
    }
    RpcHeaderProtos.RpcSaslProto saslResponse = null;
    try {
      try {
        saslResponse = processSaslMessage(saslMessage);
      } catch (IOException e) {
        server.rpcMetrics.incrAuthenticationFailures();
        if (Server.LOG.isDebugEnabled()) {
          Server.LOG.debug(StringUtils.stringifyException(e));
        }
        // attempting user could be null
        IOException tce = (IOException) getTrueCause(e);
        Server.AUDITLOG.warn(Server.AUTH_FAILED_FOR + this.toString() + ":"
            + attemptingUser + " (" + e.getLocalizedMessage()
            + ") with true cause: (" + tce.getLocalizedMessage() + ")");
        throw tce;
      }

      if (saslServer != null && saslServer.isComplete()) {
        if (Server.LOG.isDebugEnabled()) {
          Server.LOG.debug("SASL server context established. Negotiated QoP is "
              + saslServer.getNegotiatedProperty(Sasl.QOP));
        }
        user = getAuthorizedUgi(saslServer.getAuthorizationID());
        if (Server.LOG.isDebugEnabled()) {
          Server.LOG.debug(
              "SASL server successfully authenticated client: " + user);
        }
        server.rpcMetrics.incrAuthenticationSuccesses();
        Server.AUDITLOG.info(
            Server.AUTH_SUCCESSFUL_FOR + user + " from " + toString());
        saslContextEstablished = true;
      }
    } catch (RpcServerException rse) { // don't re-wrap
      throw rse;
    } catch (IOException ioe) {
      throw new Server.FatalRpcServerException(
          RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_UNAUTHORIZED,
          ioe);
    }
    // send back response if any, may throw IOException
    if (saslResponse != null) {
      doSaslReply(saslResponse);
    }
    // do NOT enable wrapping until the last auth response is sent
    if (saslContextEstablished) {
      String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
      establishedQOP = qop;
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
   *
   * @param saslMessage received SASL message
   * @return the sasl response to send back to client
   * @throws SaslException          if authentication or generating response fails,
   *                                or SASL protocol mixup
   * @throws IOException            if a SaslServer cannot be created
   * @throws AccessControlException if the requested authentication type
   *                                is not supported or trying to re-attempt negotiation.
   * @throws InterruptedException
   */
  private RpcHeaderProtos.RpcSaslProto processSaslMessage(
      RpcHeaderProtos.RpcSaslProto saslMessage)
      throws SaslException, IOException, AccessControlException,
      InterruptedException {
    final RpcHeaderProtos.RpcSaslProto saslResponse;
    final RpcHeaderProtos.RpcSaslProto.SaslState state =
        saslMessage.getState(); // required
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
        if (saslResponse.getState() ==
            RpcHeaderProtos.RpcSaslProto.SaslState.SUCCESS) {
          switchToSimple();
        }
        break;
      }
      case INITIATE: {
        if (saslMessage.getAuthsCount() != 1) {
          throw new SaslException("Client mechanism is malformed");
        }
        // verify the client requested an advertised authType
        RpcHeaderProtos.RpcSaslProto.SaslAuth clientSaslAuth =
            saslMessage.getAuths(0);
        if (!server.negotiateResponse.getAuthsList().contains(clientSaslAuth)) {
          if (sentNegotiate) {
            throw new AccessControlException(
                clientSaslAuth.getMethod() + " authentication is not enabled."
                    + "  Available:" + server.enabledAuthMethods);
          }
          saslResponse = buildSaslNegotiateResponse();
          break;
        }
        authMethod =
            SaslRpcServer.AuthMethod.valueOf(clientSaslAuth.getMethod());
        // abort SASL for SIMPLE auth, server has already ensured that
        // SIMPLE is a legit option above.  we will send no response
        if (authMethod == SaslRpcServer.AuthMethod.SIMPLE) {
          switchToSimple();
          saslResponse = null;
          break;
        }
        // sasl server for tokens may already be instantiated
        if (saslServer == null ||
            authMethod != SaslRpcServer.AuthMethod.TOKEN) {
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

  private RpcHeaderProtos.RpcSaslProto processSaslToken(
      RpcHeaderProtos.RpcSaslProto saslMessage)
      throws SaslException {
    if (!saslMessage.hasToken()) {
      throw new SaslException("Client did not send a token");
    }
    byte[] saslToken = saslMessage.getToken().toByteArray();
    if (Server.LOG.isDebugEnabled()) {
      Server.LOG.debug("Have read input token of size " + saslToken.length
          + " for processing by saslServer.evaluateResponse()");
    }
    saslToken = saslServer.evaluateResponse(saslToken);
    return buildSaslResponse(
        saslServer.isComplete() ?
            RpcHeaderProtos.RpcSaslProto.SaslState.SUCCESS :
            RpcHeaderProtos.RpcSaslProto.SaslState.CHALLENGE,
        saslToken);
  }

  private void switchToSimple() {
    // disable SASL and blank out any SASL server
    authProtocol = Server.AuthProtocol.NONE;
    disposeSasl();
  }

  private RpcHeaderProtos.RpcSaslProto buildSaslResponse(
      RpcHeaderProtos.RpcSaslProto.SaslState state, byte[] replyToken) {
    if (Server.LOG.isDebugEnabled()) {
      Server.LOG.debug("Will send " + state + " token of size "
          + ((replyToken != null) ? replyToken.length : null)
          + " from saslServer.");
    }
    RpcHeaderProtos.RpcSaslProto.Builder response =
        RpcHeaderProtos.RpcSaslProto.newBuilder();
    response.setState(state);
    if (replyToken != null) {
      response.setToken(ByteString.copyFrom(replyToken));
    }
    return response.build();
  }

  private void doSaslReply(Message message) throws IOException {
    final Server.RpcCall
        saslCall = server.getRpcCall(this,
        Server.AuthProtocol.SASL.callId);
    server.setupResponse(saslCall,
        RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.SUCCESS, null,
        RpcWritable.wrap(message), null, null);
    sendResponse(saslCall);
  }

  private void doSaslReply(Exception ioe) throws IOException {
    server.setupResponse(authFailedCall,
        RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.FATAL,
        RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_UNAUTHORIZED,
        null, ioe.getClass().getName(), ioe.getMessage());
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
      Server.LOG.warn(error);
      throw new IOException(error);
    } else if (dataLength > server.maxDataLength) {
      String error = "Requested data length " + dataLength +
          " is longer than maximum configured RPC length " +
          server.maxDataLength + ".  RPC came from " + getHostAddress();
      Server.LOG.warn(error);
      throw new IOException(error);
    }
  }

  /**
   * This method reads in a non-blocking fashion from the channel:
   * this method is called repeatedly when data is present in the channel;
   * when it has enough data to process one rpc it processes that rpc.
   * <p>
   * On the first pass, it processes the connectionHeader,
   * connectionContext (an outOfBand RPC) and at most one RPC request that
   * follows that. On future passes it will process at most one RPC request.
   * <p>
   * Quirky things: dataLengthBuffer (4 bytes) is used to read "hrpc" OR
   * rpc request length.
   *
   * @return -1 in case of error, else num bytes read so far
   * @throws IOException          - internal error that should not be returned to
   *                              client, typically failure to respond to client
   * @throws InterruptedException - if the thread is interrupted.
   */
  public int doRead(Object in) throws InterruptedException {
    setLastContact(Time.now());

    int count;
    try {
      count = readAndProcess(in);
    } catch (InterruptedException ie) {
      Server.LOG.info(Thread.currentThread().getName() +
          ": readAndProcess caught InterruptedException", ie);
      throw ie;
    } catch (Exception e) {
      // Any exceptions that reach here are fatal unexpected internal errors
      // that could not be sent to the client.
      Server.LOG.info(Thread.currentThread().getName() +
          ": readAndProcess from client " + this +
          " threw exception [" + e + "]", e);
      count = -1; //so that the (count < 0) block is executed
    }
    // setupResponse will signal the connection should be closed when a
    // fatal response is sent.
    if (count < 0 || shouldClose()) {
      server.closeConnection(this);
    } else {
      setLastContact(Time.now());
    }
    return count;
  }

  private int readAndProcess(Object in)
      throws IOException, InterruptedException {
    while (server.running &&
        !shouldClose()) { // stop if a fatal response has been sent.
      // dataLengthBuffer is used to read "hrpc" or the rpc-packet length
      int count = -1;
      if (dataLengthBuffer.remaining() > 0) {
        count = bufferRead(in, dataLengthBuffer);
        if (count < 0 || dataLengthBuffer.remaining() > 0) {
          return count;
        }
      }

      if (!connectionHeaderRead) {
        // Every connection is expected to send the header;
        // so far we read "hrpc" of the connection header.
        if (connectionHeaderBuf == null) {
          // for the bytes that follow "hrpc", in the connection header
          connectionHeaderBuf = ByteBuffer.allocate(HEADER_LEN_AFTER_HRPC_PART);
        }
        count = bufferRead(in, connectionHeaderBuf);
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
        if (Server.HTTP_GET_BYTES.equals(dataLengthBuffer)) {
          setupHttpRequestOnIpcPortResponse();
          return -1;
        }

        if (!RpcConstants.HEADER.equals(dataLengthBuffer)) {
          Server.LOG.warn("Incorrect RPC Header length from {}:{} "
                  + "expected length: {} got length: {}",
              hostAddress, remotePort, RpcConstants.HEADER, dataLengthBuffer);
          setupBadVersionResponse(version);
          return -1;
        }
        if (version != CURRENT_VERSION) {
          //Warning is ok since this is not supposed to happen.
          Server.LOG.warn("Version mismatch from " +
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
      count = bufferRead(in, data);

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

  private Server.AuthProtocol initializeAuthContext(int authType)
      throws IOException {
    Server.AuthProtocol authProtocol = Server.AuthProtocol.valueOf(authType);
    if (authProtocol == null) {
      IOException ioe = new IpcException("Unknown auth protocol:" + authType);
      doSaslReply(ioe);
      throw ioe;
    }
    boolean isSimpleEnabled = server.enabledAuthMethods.contains(
        SaslRpcServer.AuthMethod.SIMPLE);
    switch (authProtocol) {
      case NONE: {
        // don't reply if client is simple and server is insecure
        if (!isSimpleEnabled) {
          IOException ioe = new AccessControlException(
              "SIMPLE authentication is not enabled."
                  + "  Available:" + server.enabledAuthMethods);
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
   *
   * @return the response to Negotiate request - the list of enabled
   * authMethods and challenge if the TOKENS are supported.
   * @throws SaslException - if attempt to generate challenge fails.
   * @throws IOException   - if it fails to create the SASL server for Tokens
   */
  private RpcHeaderProtos.RpcSaslProto buildSaslNegotiateResponse()
      throws InterruptedException, SaslException, IOException {
    RpcHeaderProtos.RpcSaslProto negotiateMessage = server.negotiateResponse;
    // accelerate token negotiation by sending initial challenge
    // in the negotiation response
    if (server.enabledAuthMethods.contains(SaslRpcServer.AuthMethod.TOKEN)) {
      saslServer = createSaslServer(SaslRpcServer.AuthMethod.TOKEN);
      byte[] challenge = saslServer.evaluateResponse(new byte[0]);
      RpcHeaderProtos.RpcSaslProto.Builder negotiateBuilder =
          RpcHeaderProtos.RpcSaslProto.newBuilder(server.negotiateResponse);
      negotiateBuilder.getAuthsBuilder(0)  // TOKEN is always first
          .setChallenge(ByteString.copyFrom(challenge));
      negotiateMessage = negotiateBuilder.build();
    }
    sentNegotiate = true;
    return negotiateMessage;
  }

  private SaslServer createSaslServer(SaslRpcServer.AuthMethod authMethod)
      throws IOException, InterruptedException {
    final Map<String, ?> saslProps =
        server.saslPropsResolver.getServerProperties(addr, ingressPort);
    return new SaslRpcServer(authMethod).create(this, saslProps,
        server.secretManager);
  }

  /**
   * Try to set up the response to indicate that the client version
   * is incompatible with the server. This can contain special-case
   * code to speak enough of past IPC protocols to pass back
   * an exception to the caller.
   *
   * @param clientVersion the version the caller is using
   * @throws IOException
   */
  private void setupBadVersionResponse(int clientVersion) throws IOException {
    String errMsg = "Server IPC version " + CURRENT_VERSION +
        " cannot communicate with client version " + clientVersion;
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    if (clientVersion >= 9) {
      // Versions >>9  understand the normal response
      Server.RpcCall fakeCall = server.getRpcCall(this, -1);
      server.setupResponse(fakeCall,
          RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.FATAL,
          RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_VERSION_MISMATCH,
          null, RPC.VersionMismatch.class.getName(), errMsg);
      sendResponse(fakeCall);
    } else if (clientVersion >= 3) {
      Server.RpcCall fakeCall = server.getRpcCall(this, -1);
      // Versions 3 to 8 use older response
      server.setupResponseOldVersionFatal(buffer, fakeCall,
          null, RPC.VersionMismatch.class.getName(), errMsg);

      sendResponse(fakeCall);
    } else if (clientVersion == 2) { // Hadoop 0.18.3
      Server.RpcCall fakeCall = server.getRpcCall(this, 0);
      DataOutputStream out = new DataOutputStream(buffer);
      out.writeInt(0); // call ID
      out.writeBoolean(true); // error
      WritableUtils.writeString(out, RPC.VersionMismatch.class.getName());
      WritableUtils.writeString(out, errMsg);
      fakeCall.setResponse(ByteBuffer.wrap(buffer.toByteArray()));
      sendResponse(fakeCall);
    }
  }

  private void setupHttpRequestOnIpcPortResponse() throws IOException {
    Server.RpcCall fakeCall = server.getRpcCall(this, 0);
    fakeCall.setResponse(ByteBuffer.wrap(
        Server.RECEIVED_HTTP_REQ_RESPONSE.getBytes(StandardCharsets.UTF_8)));
    sendResponse(fakeCall);
  }

  /**
   * Reads the connection context following the connection header
   *
   * @throws RpcServerException - if the header cannot be
   *                            deserialized, or the user is not authorized
   */
  private void processConnectionContext(RpcWritable.Buffer buffer)
      throws RpcServerException {
    // allow only one connection context during a session
    if (connectionContextRead) {
      throw new Server.FatalRpcServerException(
          RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
          "Connection context already processed");
    }
    connectionContext = getMessage(
        IpcConnectionContextProtos.IpcConnectionContextProto.getDefaultInstance(),
        buffer);
    protocolName = connectionContext.hasProtocol() ? connectionContext
        .getProtocol() : null;

    UserGroupInformation protocolUser = ProtoUtil.getUgi(connectionContext);
    if (authProtocol == Server.AuthProtocol.NONE) {
      user = protocolUser;
    } else {
      // user is authenticated
      user.setAuthenticationMethod(authMethod);
      //Now we check if this is a proxy user case. If the protocol user is
      //different from the 'user', it is a proxy user scenario. However,
      //this is not allowed if user authenticated with DIGEST.
      if ((protocolUser != null)
          && (!protocolUser.getUserName().equals(user.getUserName()))) {
        if (authMethod == SaslRpcServer.AuthMethod.TOKEN) {
          // Not allowed to doAs if token authentication is used
          throw new Server.FatalRpcServerException(
              RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_UNAUTHORIZED,
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
      server.connectionManager.incrUserConnections(user.getShortUserName());
    }
  }

  /**
   * Process a wrapped RPC Request - unwrap the SASL packet and process
   * each embedded RPC request
   *
   * @param inBuf - SASL wrapped request of one or more RPCs
   * @throws IOException          - SASL packet cannot be unwrapped
   * @throws InterruptedException
   */
  private void unwrapPacketAndProcessRpcs(byte[] inBuf)
      throws IOException, InterruptedException {
    if (Server.LOG.isDebugEnabled()) {
      Server.LOG.debug("Have read input token of size " + inBuf.length
          + " for processing by saslServer.unwrap()");
    }
    inBuf = saslServer.unwrap(inBuf, 0, inBuf.length);
    ReadableByteChannel ch = Channels.newChannel(new ByteArrayInputStream(
        inBuf));
    // Read all RPCs contained in the inBuf, even partial ones
    while (!shouldClose()) { // stop if a fatal response has been sent.
      int count = -1;
      if (unwrappedDataLengthBuffer.remaining() > 0) {
        count = server.channelRead(ch, unwrappedDataLengthBuffer);
        if (count <= 0 || unwrappedDataLengthBuffer.remaining() > 0) {
          return;
        }
      }

      if (unwrappedData == null) {
        unwrappedDataLengthBuffer.flip();
        int unwrappedDataLength = unwrappedDataLengthBuffer.getInt();
        unwrappedData = ByteBuffer.allocate(unwrappedDataLength);
      }

      count = server.channelRead(ch, unwrappedData);
      if (count <= 0 || unwrappedData.remaining() > 0) {
        return;
      }

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
   * - decode rpc in a rpc-Call
   * - handle out-of-band RPC requests such as the initial connectionContext
   * - A successfully decoded RpcCall will be deposited in RPC-Q and
   * its response will be sent later when the request is processed.
   * <p>
   * Prior to this call the connectionHeader ("hrpc...") has been handled and
   * if SASL then SASL has been established and the buf we are passed
   * has been unwrapped from SASL.
   *
   * @param bb - contains the RPC request header and the rpc request
   * @throws IOException          - internal error that should not be returned to
   *                              client, typically failure to respond to client
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
      final RpcHeaderProtos.RpcRequestHeaderProto header =
          getMessage(RpcHeaderProtos.RpcRequestHeaderProto.getDefaultInstance(),
              buffer);
      callId = header.getCallId();
      retry = header.getRetryCount();
      if (Server.LOG.isDebugEnabled()) {
        Server.LOG.debug(" got #" + callId);
      }
      checkRpcHeaders(header);

      if (callId < 0) { // callIds typically used during connection setup
        processRpcOutOfBandRequest(header, buffer);
      } else if (!connectionContextRead) {
        throw new Server.FatalRpcServerException(
            RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
            "Connection context not established");
      } else {
        processRpcRequest(header, buffer);
      }
    } catch (RpcServerException rse) {
      // inform client of error, but do not rethrow else non-fatal
      // exceptions will close connection!
      if (Server.LOG.isDebugEnabled()) {
        Server.LOG.debug(Thread.currentThread().getName() +
            ": processOneRpc from client " + this +
            " threw exception [" + rse + "]");
      }
      // use the wrapped exception if there is one.
      Throwable t = (rse.getCause() != null) ? rse.getCause() : rse;
      final Server.RpcCall call = server.getRpcCall(this, callId, retry);
      server.setupResponse(call,
          rse.getRpcStatusProto(), rse.getRpcErrorCodeProto(), null,
          t.getClass().getName(), t.getMessage());
      sendResponse(call);
    }
  }

  /**
   * Verify RPC header is valid
   *
   * @param header - RPC request header
   * @throws RpcServerException - header contains invalid values
   */
  private void checkRpcHeaders(RpcHeaderProtos.RpcRequestHeaderProto header)
      throws RpcServerException {
    if (!header.hasRpcOp()) {
      String err = " IPC Server: No rpc op in rpcRequestHeader";
      throw new Server.FatalRpcServerException(
          RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
          err);
    }
    if (header.getRpcOp() !=
        RpcHeaderProtos.RpcRequestHeaderProto.OperationProto.RPC_FINAL_PACKET) {
      String err = "IPC Server does not implement rpc header operation" +
          header.getRpcOp();
      throw new Server.FatalRpcServerException(
          RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
          err);
    }
    // If we know the rpc kind, get its class so that we can deserialize
    // (Note it would make more sense to have the handler deserialize but
    // we continue with this original design.
    if (!header.hasRpcKind()) {
      String err = " IPC Server: No rpc kind in rpcRequestHeader";
      throw new Server.FatalRpcServerException(
          RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
          err);
    }
  }

  /**
   * Process an RPC Request
   * - the connection headers and context must have been already read.
   * - Based on the rpcKind, decode the rpcRequest.
   * - A successfully decoded RpcCall will be deposited in RPC-Q and
   * its response will be sent later when the request is processed.
   *
   * @param header - RPC request header
   * @param buffer - stream to request payload
   * @throws RpcServerException   - generally due to fatal rpc layer issues
   *                              such as invalid header or deserialization error.  The call queue
   *                              may also throw a fatal or non-fatal exception on overflow.
   * @throws IOException          - fatal internal error that should/could not
   *                              be sent to client.
   * @throws InterruptedException
   */
  private void processRpcRequest(RpcHeaderProtos.RpcRequestHeaderProto header,
                                 RpcWritable.Buffer buffer)
      throws RpcServerException,
      InterruptedException {
    Class<? extends Writable> rpcRequestClass =
        server.getRpcRequestWrapper(header.getRpcKind());
    if (rpcRequestClass == null) {
      Server.LOG.warn("Unknown rpc kind " + header.getRpcKind() +
          " from client " + getHostAddress());
      final String err = "Unknown rpc kind in rpc header" +
          header.getRpcKind();
      throw new Server.FatalRpcServerException(
          RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
          err);
    }
    Writable rpcRequest;
    try { //Read the rpc request
      rpcRequest = buffer.newInstance(rpcRequestClass, server.conf);
    } catch (RpcServerException rse) { // lets tests inject failures.
      throw rse;
    } catch (Throwable t) { // includes runtime exception from newInstance
      Server.LOG.warn("Unable to read call parameters for client " +
          getHostAddress() + "on connection protocol " +
          this.protocolName + " for rpcKind " + header.getRpcKind(), t);
      String err =
          "IPC server unable to read call parameters: " + t.getMessage();
      throw new Server.FatalRpcServerException(
          RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_DESERIALIZING_REQUEST,
          err);
    }

    Span span = null;
    if (header.hasTraceInfo()) {
      RpcHeaderProtos.RPCTraceInfoProto traceInfoProto = header.getTraceInfo();
      if (traceInfoProto.hasSpanContext()) {
        if (server.tracer == null) {
          server.setTracer(Tracer.curThreadTracer());
        }
        if (server.tracer != null) {
          // If the incoming RPC included tracing info, always continue the
          // trace
          SpanContext spanCtx = TraceUtils.byteStringToSpanContext(
              traceInfoProto.getSpanContext());
          if (spanCtx != null) {
            span = server.tracer.newSpan(
                RpcClientUtil.toTraceName(rpcRequest.toString()), spanCtx);
          }
        }
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

    Server.RpcCall call = server.getRpcCall(this, header.getCallId(),
        header.getRetryCount(), rpcRequest,
        ProtoUtil.convert(header.getRpcKind()),
        header.getClientId().toByteArray(), span, callerContext);

    // Save the priority level assignment by the scheduler
    call.setPriorityLevel(server.callQueue.getPriorityLevel(call));
    call.markCallCoordinated(false);
    if (server.alignmentContext != null && call.rpcRequest != null &&
        (call.rpcRequest instanceof ProtobufRpcEngine2.RpcProtobufRequest)) {
      // if call.rpcRequest is not RpcProtobufRequest, will skip the following
      // step and treat the call as uncoordinated. As currently only certain
      // ClientProtocol methods request made through RPC protobuf needs to be
      // coordinated.
      String methodName;
      String protoName;
      ProtobufRpcEngine2.RpcProtobufRequest req =
          (ProtobufRpcEngine2.RpcProtobufRequest) call.rpcRequest;
      try {
        methodName = req.getRequestHeader().getMethodName();
        protoName = req.getRequestHeader().getDeclaringClassProtocolName();
        if (server.alignmentContext.isCoordinatedCall(protoName, methodName)) {
          call.markCallCoordinated(true);
          long stateId;
          stateId = server.alignmentContext.receiveRequestState(
              header, server.getMaxIdleTime());
          call.setClientStateId(stateId);
        }
      } catch (IOException ioe) {
        throw new RpcServerException("Processing RPC request caught ", ioe);
      }
    }

    try {
      server.internalQueueCall(call);
    } catch (RpcServerException rse) {
      throw rse;
    } catch (IOException ioe) {
      throw new Server.FatalRpcServerException(
          RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.ERROR_RPC_SERVER,
          ioe);
    }
    incRpcCount();  // Increment the rpc count
  }

  /**
   * Establish RPC connection setup by negotiating SASL if required, then
   * reading and authorizing the connection header
   *
   * @param header - RPC header
   * @param buffer - stream to request payload
   * @throws RpcServerException   - setup failed due to SASL
   *                              negotiation failure, premature or invalid connection context,
   *                              or other state errors. This exception needs to be sent to the
   *                              client.
   * @throws IOException          - failed to send a response back to the client
   * @throws InterruptedException
   */
  private void processRpcOutOfBandRequest(
      RpcHeaderProtos.RpcRequestHeaderProto header,
      RpcWritable.Buffer buffer) throws RpcServerException,
      IOException, InterruptedException {
    final int callId = header.getCallId();
    if (callId == CONNECTION_CONTEXT_CALL_ID) {
      // SASL must be established prior to connection context
      if (authProtocol == Server.AuthProtocol.SASL && !saslContextEstablished) {
        throw new Server.FatalRpcServerException(
            RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
            "Connection header sent during SASL negotiation");
      }
      // read and authorize the user
      processConnectionContext(buffer);
    } else if (callId == Server.AuthProtocol.SASL.callId) {
      // if client was switched to simple, ignore first SASL message
      if (authProtocol != Server.AuthProtocol.SASL) {
        throw new Server.FatalRpcServerException(
            RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
            "SASL protocol not requested by client");
      }
      saslReadAndProcess(buffer);
    } else if (callId == PING_CALL_ID) {
      Server.LOG.debug("Received ping message");
    } else {
      throw new Server.FatalRpcServerException(
          RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
          "Unknown out of band call #" + callId);
    }
  }

  /**
   * Authorize proxy users to access this server
   *
   * @throws RpcServerException - user is not allowed to proxy
   */
  private void authorizeConnection() throws RpcServerException {
    try {
      // If auth method is TOKEN, the token was obtained by the
      // real user for the effective user, therefore not required to
      // authorize real user. doAs is allowed only for simple or kerberos
      // authentication
      if (user != null && user.getRealUser() != null
          && (authMethod != SaslRpcServer.AuthMethod.TOKEN)) {
        ProxyUsers.authorize(user, this.getHostAddress());
      }
      server.authorize(user, protocolName, getHostInetAddress());
      if (Server.LOG.isDebugEnabled()) {
        Server.LOG.debug("Successfully authorized " + connectionContext);
      }
      server.rpcMetrics.incrAuthorizationSuccesses();
    } catch (AuthorizationException ae) {
      Server.LOG.info("Connection from " + this
          + " for protocol " + connectionContext.getProtocol()
          + " is unauthorized for user " + user);
      server.rpcMetrics.incrAuthorizationFailures();
      throw new Server.FatalRpcServerException(
          RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_UNAUTHORIZED,
          ae);
    }
  }

  /**
   * Decode the a protobuf from the given input stream
   *
   * @return Message - decoded protobuf
   * @throws RpcServerException - deserialization failed
   */
  @SuppressWarnings("unchecked")
  <T extends Message> T getMessage(Message message,
                                   RpcWritable.Buffer buffer)
      throws RpcServerException {
    try {
      return (T) buffer.getValue(message);
    } catch (Exception ioe) {
      Class<?> protoClass = message.getClass();
      throw new Server.FatalRpcServerException(
          RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_DESERIALIZING_REQUEST,
          "Error decoding " + protoClass.getSimpleName() + ": " + ioe);
    }
  }

  // ipc reader threads should invoke this directly, whereas handlers
  // must invoke call.sendResponse to allow lifecycle management of
  // external, postponed, deferred calls, etc.
  public void sendResponse(Server.RpcCall call) throws IOException {
    server.responder.doRespond(call);
  }

  /**
   * Get service class for connection
   *
   * @return the serviceClass
   */
  public int getServiceClass() {
    return serviceClass;
  }

  /**
   * Set service class for connection
   *
   * @param serviceClass the serviceClass to set
   */
  public void setServiceClass(int serviceClass) {
    this.serviceClass = serviceClass;
  }

  synchronized public void close() {
    setShouldClose(); // avoid race with reader reading after close.
    disposeSasl();
    data = null;
    dataLengthBuffer = null;
  }
}
