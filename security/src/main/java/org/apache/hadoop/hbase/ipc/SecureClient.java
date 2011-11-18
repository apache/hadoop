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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.HBaseSaslRpcClient;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer.AuthMethod;
import org.apache.hadoop.hbase.security.KerberosInfo;
import org.apache.hadoop.hbase.security.TokenInfo;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenSelector;
import org.apache.hadoop.hbase.util.PoolMap;
import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hadoop.util.ReflectionUtils;

import javax.net.SocketFactory;
import java.io.*;
import java.net.*;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A client for an IPC service, which support SASL authentication of connections
 * using either GSSAPI for Kerberos authentication or DIGEST-MD5 for
 * authentication using signed tokens.
 *
 * <p>
 * This is a copy of org.apache.hadoop.ipc.Client from secure Hadoop,
 * reworked to remove code duplicated with
 * {@link org.apache.hadoop.hbase.HBaseClient}.  This is part of the loadable
 * {@link SecureRpcEngine}, and only functions in connection with a
 * {@link SecureServer} instance.
 * </p>
 */
public class SecureClient extends HBaseClient {

  private static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.ipc.SecureClient");

  protected static Map<String,TokenSelector<? extends TokenIdentifier>> tokenHandlers =
      new HashMap<String,TokenSelector<? extends TokenIdentifier>>();
  static {
    tokenHandlers.put(AuthenticationTokenIdentifier.AUTH_TOKEN_TYPE.toString(),
        new AuthenticationTokenSelector());
  }

  /** Thread that reads responses and notifies callers.  Each connection owns a
   * socket connected to a remote address.  Calls are multiplexed through this
   * socket: responses may be delivered out of order. */
  protected class SecureConnection extends Connection {
    private InetSocketAddress server;             // server ip:port
    private String serverPrincipal;  // server's krb5 principal name
    private SecureConnectionHeader header;              // connection header
    private AuthMethod authMethod; // authentication method
    private boolean useSasl;
    private Token<? extends TokenIdentifier> token;
    private HBaseSaslRpcClient saslRpcClient;
    private int reloginMaxBackoff; // max pause before relogin on sasl failure

    public SecureConnection(ConnectionId remoteId) throws IOException {
      super(remoteId);
      this.server = remoteId.getAddress();

      User ticket = remoteId.getTicket();
      Class<?> protocol = remoteId.getProtocol();
      this.useSasl = User.isSecurityEnabled();
      if (useSasl && protocol != null) {
        TokenInfo tokenInfo = protocol.getAnnotation(TokenInfo.class);
        if (tokenInfo != null) {
          TokenSelector<? extends TokenIdentifier> tokenSelector =
              tokenHandlers.get(tokenInfo.value());
          if (tokenSelector != null) {
            token = tokenSelector.selectToken(new Text(clusterId),
                ticket.getUGI().getTokens());
          } else if (LOG.isDebugEnabled()) {
            LOG.debug("No token selector found for type "+tokenInfo.value());
          }
        }
        KerberosInfo krbInfo = protocol.getAnnotation(KerberosInfo.class);
        if (krbInfo != null) {
          String serverKey = krbInfo.serverPrincipal();
          if (serverKey == null) {
            throw new IOException(
                "Can't obtain server Kerberos config key from KerberosInfo");
          }
          serverPrincipal = SecurityUtil.getServerPrincipal(
              conf.get(serverKey), server.getAddress().getCanonicalHostName().toLowerCase());
          if (LOG.isDebugEnabled()) {
            LOG.debug("RPC Server Kerberos principal name for protocol="
                + protocol.getCanonicalName() + " is " + serverPrincipal);
          }
        }
      }

      if (!useSasl) {
        authMethod = AuthMethod.SIMPLE;
      } else if (token != null) {
        authMethod = AuthMethod.DIGEST;
      } else {
        authMethod = AuthMethod.KERBEROS;
      }

      header = new SecureConnectionHeader(
          protocol == null ? null : protocol.getName(), ticket, authMethod);

      if (LOG.isDebugEnabled())
        LOG.debug("Use " + authMethod + " authentication for protocol "
            + protocol.getSimpleName());

      reloginMaxBackoff = conf.getInt("hbase.security.relogin.maxbackoff", 5000);
    }

    private synchronized void disposeSasl() {
      if (saslRpcClient != null) {
        try {
          saslRpcClient.dispose();
          saslRpcClient = null;
        } catch (IOException ioe) {
          LOG.info("Error disposing of SASL client", ioe);
        }
      }
    }

    private synchronized boolean shouldAuthenticateOverKrb() throws IOException {
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      UserGroupInformation currentUser =
        UserGroupInformation.getCurrentUser();
      UserGroupInformation realUser = currentUser.getRealUser();
      return authMethod == AuthMethod.KERBEROS &&
          loginUser != null &&
          //Make sure user logged in using Kerberos either keytab or TGT
          loginUser.hasKerberosCredentials() &&
          // relogin only in case it is the login user (e.g. JT)
          // or superuser (like oozie).
          (loginUser.equals(currentUser) || loginUser.equals(realUser));
    }

    private synchronized boolean setupSaslConnection(final InputStream in2,
        final OutputStream out2)
        throws IOException {
      saslRpcClient = new HBaseSaslRpcClient(authMethod, token, serverPrincipal);
      return saslRpcClient.saslConnect(in2, out2);
    }

    /**
     * If multiple clients with the same principal try to connect
     * to the same server at the same time, the server assumes a
     * replay attack is in progress. This is a feature of kerberos.
     * In order to work around this, what is done is that the client
     * backs off randomly and tries to initiate the connection
     * again.
     * The other problem is to do with ticket expiry. To handle that,
     * a relogin is attempted.
     */
    private synchronized void handleSaslConnectionFailure(
        final int currRetries,
        final int maxRetries, final Exception ex, final Random rand,
        final User user)
    throws IOException, InterruptedException{
      user.runAs(new PrivilegedExceptionAction<Object>() {
        public Object run() throws IOException, InterruptedException {
          closeConnection();
          if (shouldAuthenticateOverKrb()) {
            if (currRetries < maxRetries) {
              LOG.debug("Exception encountered while connecting to " +
                  "the server : " + ex);
              //try re-login
              if (UserGroupInformation.isLoginKeytabBased()) {
                UserGroupInformation.getLoginUser().reloginFromKeytab();
              } else {
                UserGroupInformation.getLoginUser().reloginFromTicketCache();
              }
              disposeSasl();
              //have granularity of milliseconds
              //we are sleeping with the Connection lock held but since this
              //connection instance is being used for connecting to the server
              //in question, it is okay
              Thread.sleep((rand.nextInt(reloginMaxBackoff) + 1));
              return null;
            } else {
              String msg = "Couldn't setup connection for " +
              UserGroupInformation.getLoginUser().getUserName() +
              " to " + serverPrincipal;
              LOG.warn(msg);
              throw (IOException) new IOException(msg).initCause(ex);
            }
          } else {
            LOG.warn("Exception encountered while connecting to " +
                "the server : " + ex);
          }
          if (ex instanceof RemoteException)
            throw (RemoteException)ex;
          throw new IOException(ex);
        }
      });
    }

    @Override
    protected synchronized void setupIOstreams()
        throws IOException, InterruptedException {
      if (socket != null || shouldCloseConnection.get()) {
        return;
      }

      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connecting to "+server);
        }
        short numRetries = 0;
        final short MAX_RETRIES = 5;
        Random rand = null;
        while (true) {
          setupConnection();
          InputStream inStream = NetUtils.getInputStream(socket);
          OutputStream outStream = NetUtils.getOutputStream(socket);
          writeRpcHeader(outStream);
          if (useSasl) {
            final InputStream in2 = inStream;
            final OutputStream out2 = outStream;
            User ticket = remoteId.getTicket();
            if (authMethod == AuthMethod.KERBEROS) {
              UserGroupInformation ugi = ticket.getUGI();
              if (ugi != null && ugi.getRealUser() != null) {
                ticket = User.create(ugi.getRealUser());
              }
            }
            boolean continueSasl = false;
            try {
              continueSasl =
                ticket.runAs(new PrivilegedExceptionAction<Boolean>() {
                  @Override
                  public Boolean run() throws IOException {
                    return setupSaslConnection(in2, out2);
                  }
                });
            } catch (Exception ex) {
              if (rand == null) {
                rand = new Random();
              }
              handleSaslConnectionFailure(numRetries++, MAX_RETRIES, ex, rand,
                   ticket);
              continue;
            }
            if (continueSasl) {
              // Sasl connect is successful. Let's set up Sasl i/o streams.
              inStream = saslRpcClient.getInputStream(inStream);
              outStream = saslRpcClient.getOutputStream(outStream);
            } else {
              // fall back to simple auth because server told us so.
              authMethod = AuthMethod.SIMPLE;
              header = new SecureConnectionHeader(header.getProtocol(),
                  header.getUser(), authMethod);
              useSasl = false;
            }
          }
          this.in = new DataInputStream(new BufferedInputStream
              (new PingInputStream(inStream)));
          this.out = new DataOutputStream
          (new BufferedOutputStream(outStream));
          writeHeader();

          // update last activity time
          touch();

          // start the receiver thread after the socket connection has been set up
          start();
          return;
        }
      } catch (IOException e) {
        markClosed(e);
        close();

        throw e;
      }
    }

    /* Write the RPC header */
    private void writeRpcHeader(OutputStream outStream) throws IOException {
      DataOutputStream out = new DataOutputStream(new BufferedOutputStream(outStream));
      // Write out the header, version and authentication method
      out.write(SecureServer.HEADER.array());
      out.write(SecureServer.CURRENT_VERSION);
      authMethod.write(out);
      out.flush();
    }

    /**
     * Write the protocol header for each connection
     * Out is not synchronized because only the first thread does this.
     */
    private void writeHeader() throws IOException {
      // Write out the ConnectionHeader
      DataOutputBuffer buf = new DataOutputBuffer();
      header.write(buf);

      // Write out the payload length
      int bufLen = buf.getLength();
      out.writeInt(bufLen);
      out.write(buf.getData(), 0, bufLen);
    }

    @Override
    protected void receiveResponse() {
      if (shouldCloseConnection.get()) {
        return;
      }
      touch();

      try {
        int id = in.readInt();                    // try to read an id

        if (LOG.isDebugEnabled())
          LOG.debug(getName() + " got value #" + id);

        Call call = calls.remove(id);

        int state = in.readInt();     // read call status
        if (LOG.isDebugEnabled()) {
          LOG.debug("call #"+id+" state is " + state);
        }
        if (state == Status.SUCCESS.state) {
          Writable value = ReflectionUtils.newInstance(valueClass, conf);
          value.readFields(in);                 // read value
          if (LOG.isDebugEnabled()) {
            LOG.debug("call #"+id+", response is:\n"+value.toString());
          }
          call.setValue(value);
        } else if (state == Status.ERROR.state) {
          call.setException(new RemoteException(WritableUtils.readString(in),
                                                WritableUtils.readString(in)));
        } else if (state == Status.FATAL.state) {
          // Close the connection
          markClosed(new RemoteException(WritableUtils.readString(in),
                                         WritableUtils.readString(in)));
        }
      } catch (IOException e) {
        if (e instanceof SocketTimeoutException && remoteId.rpcTimeout > 0) {
          // Clean up open calls but don't treat this as a fatal condition,
          // since we expect certain responses to not make it by the specified
          // {@link ConnectionId#rpcTimeout}.
          closeException = e;
        } else {
          // Since the server did not respond within the default ping interval
          // time, treat this as a fatal condition and close this connection
          markClosed(e);
        }
      } finally {
        if (remoteId.rpcTimeout > 0) {
          cleanupCalls(remoteId.rpcTimeout);
        }
      }
    }

    /** Close the connection. */
    protected synchronized void close() {
      if (!shouldCloseConnection.get()) {
        LOG.error("The connection is not in the closed state");
        return;
      }

      // release the resources
      // first thing to do;take the connection out of the connection list
      synchronized (connections) {
        if (connections.get(remoteId) == this) {
          connections.remove(remoteId);
        }
      }

      // close the streams and therefore the socket
      IOUtils.closeStream(out);
      IOUtils.closeStream(in);
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
        // log the info
        if (LOG.isDebugEnabled()) {
          LOG.debug("closing ipc connection to " + server + ": " +
              closeException.getMessage(),closeException);
        }

        // cleanup calls
        cleanupCalls();
      }
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": closed");
    }
  }

  /**
   * Construct an IPC client whose values are of the given {@link org.apache.hadoop.io.Writable}
   * class.
   * @param valueClass value class
   * @param conf configuration
   * @param factory socket factory
   */
  public SecureClient(Class<? extends Writable> valueClass, Configuration conf,
      SocketFactory factory) {
    super(valueClass, conf, factory);
  }

  /**
   * Construct an IPC client with the default SocketFactory
   * @param valueClass value class
   * @param conf configuration
   */
  public SecureClient(Class<? extends Writable> valueClass, Configuration conf) {
    this(valueClass, conf, NetUtils.getDefaultSocketFactory(conf));
  }

  @Override
  protected SecureConnection getConnection(InetSocketAddress addr,
                                   Class<? extends VersionedProtocol> protocol,
                                   User ticket,
                                   int rpcTimeout,
                                   Call call)
                                   throws IOException, InterruptedException {
    if (!running.get()) {
      // the client is stopped
      throw new IOException("The client is stopped");
    }
    SecureConnection connection;
    /* we could avoid this allocation for each RPC by having a
     * connectionsId object and with set() method. We need to manage the
     * refs for keys in HashMap properly. For now its ok.
     */
    ConnectionId remoteId = new ConnectionId(addr, protocol, ticket, rpcTimeout);
    do {
      synchronized (connections) {
        connection = (SecureConnection)connections.get(remoteId);
        if (connection == null) {
          connection = new SecureConnection(remoteId);
          connections.put(remoteId, connection);
        }
      }
    } while (!connection.addCall(call));

    //we don't invoke the method below inside "synchronized (connections)"
    //block above. The reason for that is if the server happens to be slow,
    //it will take longer to establish a connection and that will slow the
    //entire system down.
    connection.setupIOstreams();
    return connection;
  }
}