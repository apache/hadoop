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

import java.net.Socket;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.ConnectException;

import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FilterInputStream;
import java.io.InputStream;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.SocketFactory;

import org.apache.commons.logging.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;

/** A client for an IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @see Server
 */
public class Client {
  
  public static final Log LOG =
    LogFactory.getLog(Client.class);
  private Hashtable<ConnectionId, Connection> connections =
    new Hashtable<ConnectionId, Connection>();

  private Class<? extends Writable> valueClass;   // class of call values
  private int counter;                            // counter for call ids
  private AtomicBoolean running = new AtomicBoolean(true); // if client runs
  final private Configuration conf;
  final private int maxIdleTime; //connections will be culled if it was idle for 
                           //maxIdleTime msecs
  final private int maxRetries; //the max. no. of retries for socket connections
  private boolean tcpNoDelay; // if T then disable Nagle's Algorithm
  private int pingInterval; // how often sends ping to the server in msecs

  private SocketFactory socketFactory;           // how to create sockets
  private int refCount = 1;
  
  final private static String PING_INTERVAL_NAME = "ipc.ping.interval";
  final static int DEFAULT_PING_INTERVAL = 60000; // 1 min
  final static int PING_CALL_ID = -1;
  
  /**
   * set the ping interval value in configuration
   * 
   * @param conf Configuration
   * @param pingInterval the ping interval
   */
  final public static void setPingInterval(Configuration conf, int pingInterval) {
    conf.setInt(PING_INTERVAL_NAME, pingInterval);
  }

  /**
   * Get the ping interval from configuration;
   * If not set in the configuration, return the default value.
   * 
   * @param conf Configuration
   * @return the ping interval
   */
  final static int getPingInterval(Configuration conf) {
    return conf.getInt(PING_INTERVAL_NAME, DEFAULT_PING_INTERVAL);
  }
  
  /**
   * Increment this client's reference count
   *
   */
  synchronized void incCount() {
    refCount++;
  }
  
  /**
   * Decrement this client's reference count
   *
   */
  synchronized void decCount() {
    refCount--;
  }
  
  /**
   * Return if this client has no reference
   * 
   * @return true if this client has no reference; false otherwise
   */
  synchronized boolean isZeroReference() {
    return refCount==0;
  }

  /** A call waiting for a value. */
  private class Call {
    int id;                                       // call id
    Writable param;                               // parameter
    Writable value;                               // value, null if error
    IOException error;                            // exception, null if value
    boolean done;                                 // true when call is done

    protected Call(Writable param) {
      this.param = param;
      synchronized (Client.this) {
        this.id = counter++;
      }
    }

    /** Indicate when the call is complete and the
     * value or error are available.  Notifies by default.  */
    protected synchronized void callComplete() {
      this.done = true;
      notify();                                 // notify caller
    }

    /** Set the exception when there is an error.
     * Notify the caller the call is done.
     * 
     * @param error exception thrown by the call; either local or remote
     */
    public synchronized void setException(IOException error) {
      this.error = error;
      callComplete();
    }
    
    /** Set the return value when there is no error. 
     * Notify the caller the call is done.
     * 
     * @param value return value of the call.
     */
    public synchronized void setValue(Writable value) {
      this.value = value;
      callComplete();
    }
  }

  /** Thread that reads responses and notifies callers.  Each connection owns a
   * socket connected to a remote address.  Calls are multiplexed through this
   * socket: responses may be delivered out of order. */
  private class Connection extends Thread {
    private InetSocketAddress server;             // server ip:port
    private ConnectionHeader header;              // connection header
    private ConnectionId remoteId;                // connection id
    
    private Socket socket = null;                 // connected socket
    private DataInputStream in;
    private DataOutputStream out;
    
    // currently active calls
    private Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>();
    private AtomicLong lastActivity = new AtomicLong();// last I/O activity time
    private AtomicBoolean shouldCloseConnection = new AtomicBoolean();  // indicate if the connection is closed
    private IOException closeException; // close reason

    public Connection(ConnectionId remoteId) throws IOException {
      this.remoteId = remoteId;
      this.server = remoteId.getAddress();
      if (server.isUnresolved()) {
        throw new UnknownHostException("unknown host: " + 
                                       remoteId.getAddress().getHostName());
      }
      
      UserGroupInformation ticket = remoteId.getTicket();
      Class<?> protocol = remoteId.getProtocol();
      header = 
        new ConnectionHeader(protocol == null ? null : protocol.getName(), ticket);
      
      this.setName("IPC Client (" + socketFactory.hashCode() +") connection to " +
          remoteId.getAddress().toString() +
          " from " + ((ticket==null)?"an unknown user":ticket.getUserName()));
      this.setDaemon(true);
    }

    /** Update lastActivity with the current time. */
    private void touch() {
      lastActivity.set(System.currentTimeMillis());
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
       * if the connection is not going to be closed, send a ping.
       * otherwise, throw the timeout exception.
       */
      private void handleTimeout(SocketTimeoutException e) throws IOException {
        if (shouldCloseConnection.get() || !running.get()) {
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
      public int read() throws IOException {
        do {
          try {
            return super.read();
          } catch (SocketTimeoutException e) {
            handleTimeout(e);
          }
        } while (true);
      }

      /** Read bytes into a buffer starting from offset <code>off</code>
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       * 
       * @return the total number of bytes read; -1 if the connection is closed.
       */
      public int read(byte[] buf, int off, int len) throws IOException {
        do {
          try {
            return super.read(buf, off, len);
          } catch (SocketTimeoutException e) {
            handleTimeout(e);
          }
        } while (true);
      }
    }
    
    /** Connect to the server and set up the I/O streams. It then sends
     * a header to the server and starts
     * the connection thread that waits for responses.
     * @throws IOException if the connection attempt was unsuccessful
     */
    private synchronized void setupIOstreams() throws IOException {
      if (socket != null || shouldCloseConnection.get()) {
        return;
      }
      
      short ioFailures = 0;
      short timeoutFailures = 0;
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connecting to "+server);
        }
        while (true) {
          try {
            this.socket = socketFactory.createSocket();
            this.socket.setTcpNoDelay(tcpNoDelay);
            // connection time out is 20s
            NetUtils.connect(this.socket, remoteId.getAddress(), 20000);
            this.socket.setSoTimeout(pingInterval);
            break;
          } catch (SocketTimeoutException toe) {
            /* The max number of retries is 45,
             * which amounts to 20s*45 = 15 minutes retries.
             */
            handleConnectionFailure(timeoutFailures++, maxRetries, toe);
          } catch (IOException ie) {
            handleConnectionFailure(ioFailures++, maxRetries, ie);
          }
        }
        this.in = new DataInputStream(new BufferedInputStream
            (new PingInputStream(NetUtils.getInputStream(socket))));
        this.out = new DataOutputStream
            (new BufferedOutputStream(NetUtils.getOutputStream(socket)));
        writeHeader();

        // update last activity time
        touch();

        // start the receiver thread after the socket connection has been set up
        start();
      } catch (IOException e) {
        markClosed(e);
        close();
        throw e;
      }
    }

    /* Handle connection failures
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
    private void handleConnectionFailure(
        int curRetries, int maxRetries, IOException ioe) throws IOException {
      // close the current connection
      try {
        socket.close();
      } catch (IOException e) {
        LOG.warn("Not able to close a socket", e);
      }
      // set socket to null so that the next call to setupIOstreams
      // can start the process of connect all over again.
      socket = null;

      // throw the exception if the maximum number of retries is reached
      if (curRetries >= maxRetries) {
        throw wrapException(remoteId.getAddress(), ioe);
      }

      // otherwise back off and retry
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignored) {}
      
      LOG.info("Retrying connect to server: " + server + 
          ". Already tried " + curRetries + " time(s) out of "+ maxRetries);
    }

    /* Write the header for each connection
     * Out is not synchronized because only the first thread does this.
     */
    private void writeHeader() throws IOException {
      // Write out the header and version
      out.write(Server.HEADER.array());
      out.write(Server.CURRENT_VERSION);

      // Write out the ConnectionHeader
      DataOutputBuffer buf = new DataOutputBuffer();
      header.write(buf);
      
      // Write out the payload length
      int bufLen = buf.getLength();
      out.writeInt(bufLen);
      out.write(buf.getData(), 0, bufLen);
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
              (System.currentTimeMillis()-lastActivity.get());
        if (timeout>0) {
          try {
            wait(timeout);
          } catch (InterruptedException e) {}
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
      long curTime = System.currentTimeMillis();
      if ( curTime - lastActivity.get() >= pingInterval) {
        lastActivity.set(curTime);
        synchronized (out) {
          out.writeInt(PING_CALL_ID);
          out.flush();
        }
      }
    }

    public void run() {
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": starting, having connections " 
            + connections.size());

      while (waitForWork()) {//wait here for work - read or close connection
        receiveResponse();
      }
      
      close();
      
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": stopped, remaining connections "
            + connections.size());
    }

    /** Initiates a call by sending the parameter to the remote server.
     * Note: this is not called from the Connection thread, but by other
     * threads.
     */
    public void sendParam(Call call) {
      if (shouldCloseConnection.get()) {
        return;
      }

      DataOutputBuffer d=null;
      try {
        synchronized (this.out) {
          if (LOG.isDebugEnabled())
            LOG.debug(getName() + " sending #" + call.id);
          
          //for serializing the
          //data to be written
          d = new DataOutputBuffer();
          d.writeInt(call.id);
          call.param.write(d);
          byte[] data = d.getData();
          int dataLength = d.getLength();
          out.writeInt(dataLength);      //first put the data length
          out.write(data, 0, dataLength);//write the data
          out.flush();
        }
      } catch(IOException e) {
        markClosed(e);
      } finally {
        //the buffer is just an in-memory buffer, but it is still polite to
        // close early
        IOUtils.closeStream(d);
      }
    }  

    /* Receive a response.
     * Because only one receiver, so no synchronization on in.
     */
    private void receiveResponse() {
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
        if (state == Status.SUCCESS.state) {
          Writable value = ReflectionUtils.newInstance(valueClass, conf);
          value.readFields(in);                 // read value
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
        markClosed(e);
      }
    }
    
    private synchronized void markClosed(IOException e) {
      if (shouldCloseConnection.compareAndSet(false, true)) {
        closeException = e;
        notifyAll();
      }
    }
    
    /** Close the connection. */
    private synchronized void close() {
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
    
    /* Cleanup all calls and mark them as done */
    private void cleanupCalls() {
      Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator() ;
      while (itor.hasNext()) {
        Call c = itor.next().getValue(); 
        c.setException(closeException); // local exception
        itor.remove();         
      }
    }
  }

  /** Call implementation used for parallel calls. */
  private class ParallelCall extends Call {
    private ParallelResults results;
    private int index;
    
    public ParallelCall(Writable param, ParallelResults results, int index) {
      super(param);
      this.results = results;
      this.index = index;
    }

    /** Deliver result to result collector. */
    protected void callComplete() {
      results.callComplete(this);
    }
  }

  /** Result collector for parallel calls. */
  private static class ParallelResults {
    private Writable[] values;
    private int size;
    private int count;

    public ParallelResults(int size) {
      this.values = new Writable[size];
      this.size = size;
    }

    /** Collect a result. */
    public synchronized void callComplete(ParallelCall call) {
      values[call.index] = call.value;            // store the value
      count++;                                    // count it
      if (count == size)                          // if all values are in
        notify();                                 // then notify waiting caller
    }
  }

  /** Construct an IPC client whose values are of the given {@link Writable}
   * class. */
  public Client(Class<? extends Writable> valueClass, Configuration conf, 
      SocketFactory factory) {
    this.valueClass = valueClass;
    this.maxIdleTime = 
      conf.getInt("ipc.client.connection.maxidletime", 10000); //10s
    this.maxRetries = conf.getInt("ipc.client.connect.max.retries", 10);
    this.tcpNoDelay = conf.getBoolean("ipc.client.tcpnodelay", false);
    this.pingInterval = getPingInterval(conf);
    if (LOG.isDebugEnabled()) {
      LOG.debug("The ping interval is" + this.pingInterval + "ms.");
    }
    this.conf = conf;
    this.socketFactory = factory;
  }

  /**
   * Construct an IPC client with the default SocketFactory
   * @param valueClass
   * @param conf
   */
  public Client(Class<? extends Writable> valueClass, Configuration conf) {
    this(valueClass, conf, NetUtils.getDefaultSocketFactory(conf));
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

    if (!running.compareAndSet(true, false)) {
      return;
    }
    
    // wake up all connections
    synchronized (connections) {
      for (Connection conn : connections.values()) {
        conn.interrupt();
      }
    }
    
    // wait until all connections are closed
    while (!connections.isEmpty()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
    }
  }

  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code>, returning the value.  Throws exceptions if there are
   * network problems or if the remote code threw an exception.
   * @deprecated Use {@link #call(Writable, InetSocketAddress, Class, UserGroupInformation)} instead 
   */
  @Deprecated
  public Writable call(Writable param, InetSocketAddress address)
  throws InterruptedException, IOException {
      return call(param, address, null);
  }
  
  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> with the <code>ticket</code> credentials, returning 
   * the value.  
   * Throws exceptions if there are network problems or if the remote code 
   * threw an exception.
   * @deprecated Use {@link #call(Writable, InetSocketAddress, Class, UserGroupInformation)} instead 
   */
  @Deprecated
  public Writable call(Writable param, InetSocketAddress addr, 
      UserGroupInformation ticket)  
      throws InterruptedException, IOException {
    return call(param, addr, null, ticket);
  }
  
  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> which is servicing the <code>protocol</code> protocol, 
   * with the <code>ticket</code> credentials, returning the value.  
   * Throws exceptions if there are network problems or if the remote code 
   * threw an exception. */
  public Writable call(Writable param, InetSocketAddress addr, 
                       Class<?> protocol, UserGroupInformation ticket)  
                       throws InterruptedException, IOException {
    Call call = new Call(param);
    Connection connection = getConnection(addr, protocol, ticket, call);
    connection.sendParam(call);                 // send the parameter
    synchronized (call) {
      while (!call.done) {
        try {
          call.wait();                           // wait for the result
        } catch (InterruptedException ignored) {}
      }

      if (call.error != null) {
        if (call.error instanceof RemoteException) {
          call.error.fillInStackTrace();
          throw call.error;
        } else { // local exception
          throw wrapException(addr, call.error);
        }
      } else {
        return call.value;
      }
    }
  }

  /**
   * Take an IOException and the address we were trying to connect to
   * and return an IOException with the input exception as the cause.
   * The new exception provides the stack trace of the place where 
   * the exception is thrown and some extra diagnostics information.
   * If the exception is ConnectException or SocketTimeoutException, 
   * return a new one of the same type; Otherwise return an IOException.
   * 
   * @param addr target address
   * @param exception the relevant exception
   * @return an exception to throw
   */
  private IOException wrapException(InetSocketAddress addr,
                                         IOException exception) {
    if (exception instanceof ConnectException) {
      //connection refused; include the host:port in the error
      return (ConnectException)new ConnectException(
           "Call to " + addr + " failed on connection exception: " + exception)
                    .initCause(exception);
    } else if (exception instanceof SocketTimeoutException) {
      return (SocketTimeoutException)new SocketTimeoutException(
           "Call to " + addr + " failed on socket timeout exception: "
                      + exception).initCause(exception);
    } else {
      return (IOException)new IOException(
           "Call to " + addr + " failed on local exception: " + exception)
                                 .initCause(exception);

    }
  }

  /** 
   * Makes a set of calls in parallel.  Each parameter is sent to the
   * corresponding address.  When all values are available, or have timed out
   * or errored, the collected results are returned in an array.  The array
   * contains nulls for calls that timed out or errored.
   * @deprecated Use {@link #call(Writable[], InetSocketAddress[], Class, UserGroupInformation)} instead 
   */
  @Deprecated
  public Writable[] call(Writable[] params, InetSocketAddress[] addresses)
    throws IOException {
    return call(params, addresses, null, null);
  }
  
  /** Makes a set of calls in parallel.  Each parameter is sent to the
   * corresponding address.  When all values are available, or have timed out
   * or errored, the collected results are returned in an array.  The array
   * contains nulls for calls that timed out or errored.  */
  public Writable[] call(Writable[] params, InetSocketAddress[] addresses, 
                         Class<?> protocol, UserGroupInformation ticket)
    throws IOException {
    if (addresses.length == 0) return new Writable[0];

    ParallelResults results = new ParallelResults(params.length);
    synchronized (results) {
      for (int i = 0; i < params.length; i++) {
        ParallelCall call = new ParallelCall(params[i], results, i);
        try {
          Connection connection = 
            getConnection(addresses[i], protocol, ticket, call);
          connection.sendParam(call);             // send each parameter
        } catch (IOException e) {
          // log errors
          LOG.info("Calling "+addresses[i]+" caught: " + 
                   e.getMessage(),e);
          results.size--;                         //  wait for one fewer result
        }
      }
      while (results.count != results.size) {
        try {
          results.wait();                    // wait for all results
        } catch (InterruptedException e) {}
      }

      return results.values;
    }
  }

  /** Get a connection from the pool, or create a new one and add it to the
   * pool.  Connections to a given host/port are reused. */
  private Connection getConnection(InetSocketAddress addr,
                                   Class<?> protocol,
                                   UserGroupInformation ticket,
                                   Call call)
                                   throws IOException {
    if (!running.get()) {
      // the client is stopped
      throw new IOException("The client is stopped");
    }
    Connection connection;
    /* we could avoid this allocation for each RPC by having a  
     * connectionsId object and with set() method. We need to manage the
     * refs for keys in HashMap properly. For now its ok.
     */
    ConnectionId remoteId = new ConnectionId(addr, protocol, ticket);
    do {
      synchronized (connections) {
        connection = connections.get(remoteId);
        if (connection == null) {
          connection = new Connection(remoteId);
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

  /**
   * This class holds the address and the user ticket. The client connections
   * to servers are uniquely identified by <remoteAddress, protocol, ticket>
   */
  private static class ConnectionId {
    InetSocketAddress address;
    UserGroupInformation ticket;
    Class<?> protocol;
    private static final int PRIME = 16777619;
    
    ConnectionId(InetSocketAddress address, Class<?> protocol, 
                 UserGroupInformation ticket) {
      this.protocol = protocol;
      this.address = address;
      this.ticket = ticket;
    }
    
    InetSocketAddress getAddress() {
      return address;
    }
    
    Class<?> getProtocol() {
      return protocol;
    }
    
    UserGroupInformation getTicket() {
      return ticket;
    }
    
    
    @Override
    public boolean equals(Object obj) {
     if (obj instanceof ConnectionId) {
       ConnectionId id = (ConnectionId) obj;
       return address.equals(id.address) && protocol == id.protocol && 
              ticket == id.ticket;
       //Note : ticket is a ref comparision.
     }
     return false;
    }
    
    @Override
    public int hashCode() {
      return (address.hashCode() + PRIME * System.identityHashCode(protocol)) ^ 
             System.identityHashCode(ticket);
    }
  }  
}
