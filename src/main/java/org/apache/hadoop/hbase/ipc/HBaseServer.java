/**
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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.io.WritableWithSize;
import org.apache.hadoop.hbase.util.ByteBufferOutputStream;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.hbase.ipc.VersionedProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/** An abstract IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 *
 *
 * <p>Copied local so can fix HBASE-900.
 *
 * @see HBaseClient
 */
public abstract class HBaseServer implements RpcServer {

  /**
   * The first four bytes of Hadoop RPC connections
   */
  public static final ByteBuffer HEADER = ByteBuffer.wrap("hrpc".getBytes());
  public static final byte CURRENT_VERSION = 3;

  /**
   * How many calls/handler are allowed in the queue.
   */
  private static final int DEFAULT_MAX_QUEUE_SIZE_PER_HANDLER = 10;

  static final int BUFFER_INITIAL_SIZE = 1024;

  private static final String WARN_DELAYED_CALLS =
      "hbase.ipc.warn.delayedrpc.number";

  private static final int DEFAULT_WARN_DELAYED_CALLS = 1000;

  private final int warnDelayedCalls;

  private AtomicInteger delayedCalls;

  public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.ipc.HBaseServer");
  protected static final Log TRACELOG =
      LogFactory.getLog("org.apache.hadoop.ipc.HBaseServer.trace");

  protected static final ThreadLocal<RpcServer> SERVER =
    new ThreadLocal<RpcServer>();
  private volatile boolean started = false;

  private static final Map<String, Class<? extends VersionedProtocol>>
      PROTOCOL_CACHE =
      new ConcurrentHashMap<String, Class<? extends VersionedProtocol>>();

  static Class<? extends VersionedProtocol> getProtocolClass(
      String protocolName, Configuration conf)
  throws ClassNotFoundException {
    Class<? extends VersionedProtocol> protocol =
        PROTOCOL_CACHE.get(protocolName);

    if (protocol == null) {
      protocol = (Class<? extends VersionedProtocol>)
          conf.getClassByName(protocolName);
      PROTOCOL_CACHE.put(protocolName, protocol);
    }
    return protocol;
  }

  /** Returns the server instance called under or null.  May be called under
   * {@link #call(Class, Writable, long)} implementations, and under {@link Writable}
   * methods of paramters and return values.  Permits applications to access
   * the server context.
   * @return HBaseServer
   */
  public static RpcServer get() {
    return SERVER.get();
  }

  /** This is set to Call object before Handler invokes an RPC and reset
   * after the call returns.
   */
  protected static final ThreadLocal<Call> CurCall = new ThreadLocal<Call>();

  /** Returns the remote side ip address when invoked inside an RPC
   *  Returns null incase of an error.
   *  @return InetAddress
   */
  public static InetAddress getRemoteIp() {
    Call call = CurCall.get();
    if (call != null) {
      return call.connection.socket.getInetAddress();
    }
    return null;
  }
  /** Returns remote address as a string when invoked inside an RPC.
   *  Returns null in case of an error.
   *  @return String
   */
  public static String getRemoteAddress() {
    InetAddress addr = getRemoteIp();
    return (addr == null) ? null : addr.getHostAddress();
  }

  protected String bindAddress;
  protected int port;                             // port we listen on
  private int handlerCount;                       // number of handler threads
  private int priorityHandlerCount;
  private int readThreads;                        // number of read threads
  protected Class<? extends Writable> paramClass; // class of call parameters
  protected int maxIdleTime;                      // the maximum idle time after
                                                  // which a client may be
                                                  // disconnected
  protected int thresholdIdleConnections;         // the number of idle
                                                  // connections after which we
                                                  // will start cleaning up idle
                                                  // connections
  int maxConnectionsToNuke;                       // the max number of
                                                  // connections to nuke
                                                  // during a cleanup

  protected HBaseRpcMetrics  rpcMetrics;

  protected Configuration conf;

  private int maxQueueSize;
  protected int socketSendBufferSize;
  protected final boolean tcpNoDelay;   // if T then disable Nagle's Algorithm
  protected final boolean tcpKeepAlive; // if T then use keepalives

  volatile protected boolean running = true;         // true while server runs
  protected BlockingQueue<Call> callQueue; // queued calls
  protected BlockingQueue<Call> priorityCallQueue;

  private int highPriorityLevel;  // what level a high priority call is at

  protected final List<Connection> connectionList =
    Collections.synchronizedList(new LinkedList<Connection>());
  //maintain a list
  //of client connections
  private Listener listener = null;
  protected Responder responder = null;
  protected int numConnections = 0;
  private Handler[] handlers = null;
  private Handler[] priorityHandlers = null;
  protected HBaseRPCErrorHandler errorHandler = null;

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
    try {
      socket.bind(address, backlog);
    } catch (BindException e) {
      BindException bindException =
        new BindException("Problem binding to " + address + " : " +
            e.getMessage());
      bindException.initCause(e);
      throw bindException;
    } catch (SocketException e) {
      // If they try to bind to a different host's address, give a better
      // error message.
      if ("Unresolved address".equals(e.getMessage())) {
        throw new UnknownHostException("Invalid hostname for server: " +
                                       address.getHostName());
      }
      throw e;
    }
  }

  /** A call queued for handling. */
  protected class Call implements Delayable {
    protected int id;                             // the client's call id
    protected Writable param;                     // the parameter passed
    protected Connection connection;              // connection to client
    protected long timestamp;      // the time received when response is null
                                   // the time served when response is not null
    protected ByteBuffer response;                // the response for this call
    protected boolean delayResponse;
    protected Responder responder;
    protected boolean delayReturnValue;           // if the return value should be
                                                  // set at call completion
    protected boolean isError;

    public Call(int id, Writable param, Connection connection,
        Responder responder) {
      this.id = id;
      this.param = param;
      this.connection = connection;
      this.timestamp = System.currentTimeMillis();
      this.response = null;
      this.delayResponse = false;
      this.responder = responder;
      this.isError = false;
    }

    @Override
    public String toString() {
      return param.toString() + " from " + connection.toString();
    }

    private synchronized void setResponse(Object value, String errorClass,
        String error) {
      // Avoid overwriting an error value in the response.  This can happen if
      // endDelayThrowing is called by another thread before the actual call
      // returning.
      if (this.isError)
        return;
      if (errorClass != null) {
        this.isError = true;
      }
      Writable result = null;
      if (value instanceof Writable) {
        result = (Writable) value;
      } else {
        /* We might have a null value and errors. Avoid creating a
         * HbaseObjectWritable, because the constructor fails on null. */
        if (value != null) {
          result = new HbaseObjectWritable(value);
        }
      }

      int size = BUFFER_INITIAL_SIZE;
      if (result instanceof WritableWithSize) {
        // get the size hint.
        WritableWithSize ohint = (WritableWithSize) result;
        long hint = ohint.getWritableSize() + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT;
        if (hint > Integer.MAX_VALUE) {
          // oops, new problem.
          IOException ioe =
            new IOException("Result buffer size too large: " + hint);
          errorClass = ioe.getClass().getName();
          error = StringUtils.stringifyException(ioe);
        } else {
          size = (int)hint;
        }
      }

      ByteBufferOutputStream buf = new ByteBufferOutputStream(size);
      DataOutputStream out = new DataOutputStream(buf);
      try {
        out.writeInt(this.id);                // write call id
        out.writeBoolean(error != null);      // write error flag
      } catch (IOException e) {
        errorClass = e.getClass().getName();
        error = StringUtils.stringifyException(e);
      }

      try {
        if (error == null) {
          result.write(out);
        } else {
          WritableUtils.writeString(out, errorClass);
          WritableUtils.writeString(out, error);
        }
      } catch (IOException e) {
        LOG.warn("Error sending response to call: ", e);
      }

      this.response = buf.getByteBuffer();
    }

    @Override
    public synchronized void endDelay(Object result) throws IOException {
      assert this.delayResponse;
      assert this.delayReturnValue || result == null;
      this.delayResponse = false;
      delayedCalls.decrementAndGet();
      if (this.delayReturnValue)
        this.setResponse(result, null, null);
      this.responder.doRespond(this);
    }

    @Override
    public synchronized void endDelay() throws IOException {
      this.endDelay(null);
    }

    @Override
    public synchronized void startDelay(boolean delayReturnValue) {
      assert !this.delayResponse;
      this.delayResponse = true;
      this.delayReturnValue = delayReturnValue;
      int numDelayed = delayedCalls.incrementAndGet();
      if (numDelayed > warnDelayedCalls) {
        LOG.warn("Too many delayed calls: limit " + warnDelayedCalls +
            " current " + numDelayed);
      }
    }

    @Override
    public synchronized void endDelayThrowing(Throwable t) throws IOException {
      this.setResponse(null, t.getClass().toString(),
          StringUtils.stringifyException(t));
      this.delayResponse = false;
      this.sendResponseIfReady();
    }

    @Override
    public synchronized boolean isDelayed() {
      return this.delayResponse;
    }

    @Override
    public synchronized boolean isReturnValueDelayed() {
      return this.delayReturnValue;
    }

    /**
     * If we have a response, and delay is not set, then respond
     * immediately.  Otherwise, do not respond to client.  This is
     * called the by the RPC code in the context of the Handler thread.
     */
    public synchronized void sendResponseIfReady() throws IOException {
      if (!this.delayResponse) {
        this.responder.doRespond(this);
      }
    }
  }

  /** Listens on the socket. Creates jobs for the handler threads*/
  private class Listener extends Thread {

    private ServerSocketChannel acceptChannel = null; //the accept channel
    private Selector selector = null; //the selector that we use for the server
    private Reader[] readers = null;
    private int currentReader = 0;
    private InetSocketAddress address; //the address we bind at
    private Random rand = new Random();
    private long lastCleanupRunTime = 0; //the last time when a cleanup connec-
                                         //-tion (for idle connections) ran
    private long cleanupInterval = 10000; //the minimum interval between
                                          //two cleanup runs
    private int backlogLength = conf.getInt("ipc.server.listen.queue.size", 128);

    private ExecutorService readPool;

    public Listener() throws IOException {
      address = new InetSocketAddress(bindAddress, port);
      // Create a new server socket and set to non blocking mode
      acceptChannel = ServerSocketChannel.open();
      acceptChannel.configureBlocking(false);

      // Bind the server socket to the local host and port
      bind(acceptChannel.socket(), address, backlogLength);
      port = acceptChannel.socket().getLocalPort(); //Could be an ephemeral port
      // create a selector;
      selector= Selector.open();

      readers = new Reader[readThreads];
      readPool = Executors.newFixedThreadPool(readThreads,
        new ThreadFactoryBuilder().setNameFormat(
          "IPC Reader %d on port " + port).setDaemon(true).build());
      for (int i = 0; i < readThreads; ++i) {
        Selector readSelector = Selector.open();
        Reader reader = new Reader(readSelector);
        readers[i] = reader;
        readPool.execute(reader);
      }

      // Register accepts on the server socket with the selector.
      acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
      this.setName("IPC Server listener on " + port);
      this.setDaemon(true);
    }


    private class Reader implements Runnable {
      private volatile boolean adding = false;
      private Selector readSelector = null;

      Reader(Selector readSelector) {
        this.readSelector = readSelector;
      }
      public void run() {
        synchronized(this) {
          while (running) {
            SelectionKey key = null;
            try {
              readSelector.select();
              while (adding) {
                this.wait(1000);
              }

              Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
              while (iter.hasNext()) {
                key = iter.next();
                iter.remove();
                if (key.isValid()) {
                  if (key.isReadable()) {
                    doRead(key);
                  }
                }
                key = null;
              }
            } catch (InterruptedException e) {
              if (running) {                     // unexpected -- log it
                LOG.info(getName() + "caught: " +
                    StringUtils.stringifyException(e));
              }
            } catch (IOException ex) {
               LOG.error("Error in Reader", ex);
            }
          }
        }
      }

      /**
       * This gets reader into the state that waits for the new channel
       * to be registered with readSelector. If it was waiting in select()
       * the thread will be woken up, otherwise whenever select() is called
       * it will return even if there is nothing to read and wait
       * in while(adding) for finishAdd call
       */
      public void startAdd() {
        adding = true;
        readSelector.wakeup();
      }

      public synchronized SelectionKey registerChannel(SocketChannel channel)
        throws IOException {
        return channel.register(readSelector, SelectionKey.OP_READ);
      }

      public synchronized void finishAdd() {
        adding = false;
        this.notify();
      }
    }

    /** cleanup connections from connectionList. Choose a random range
     * to scan and also have a limit on the number of the connections
     * that will be cleanedup per run. The criteria for cleanup is the time
     * for which the connection was idle. If 'force' is true then all
     * connections will be looked at for the cleanup.
     * @param force all connections will be looked at for cleanup
     */
    private void cleanupConnections(boolean force) {
      if (force || numConnections > thresholdIdleConnections) {
        long currentTime = System.currentTimeMillis();
        if (!force && (currentTime - lastCleanupRunTime) < cleanupInterval) {
          return;
        }
        int start = 0;
        int end = numConnections - 1;
        if (!force) {
          start = rand.nextInt() % numConnections;
          end = rand.nextInt() % numConnections;
          int temp;
          if (end < start) {
            temp = start;
            start = end;
            end = temp;
          }
        }
        int i = start;
        int numNuked = 0;
        while (i <= end) {
          Connection c;
          synchronized (connectionList) {
            try {
              c = connectionList.get(i);
            } catch (Exception e) {return;}
          }
          if (c.timedOut(currentTime)) {
            if (LOG.isDebugEnabled())
              LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
            closeConnection(c);
            numNuked++;
            end--;
            //noinspection UnusedAssignment
            c = null;
            if (!force && numNuked == maxConnectionsToNuke) break;
          }
          else i++;
        }
        lastCleanupRunTime = System.currentTimeMillis();
      }
    }

    @Override
    public void run() {
      LOG.info(getName() + ": starting");
      SERVER.set(HBaseServer.this);

      while (running) {
        SelectionKey key = null;
        try {
          selector.select(); // FindBugs IS2_INCONSISTENT_SYNC
          Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
          while (iter.hasNext()) {
            key = iter.next();
            iter.remove();
            try {
              if (key.isValid()) {
                if (key.isAcceptable())
                  doAccept(key);
              }
            } catch (IOException ignored) {
            }
            key = null;
          }
        } catch (OutOfMemoryError e) {
          if (errorHandler != null) {
            if (errorHandler.checkOOME(e)) {
              LOG.info(getName() + ": exiting on OOME");
              closeCurrentConnection(key);
              cleanupConnections(true);
              return;
            }
          } else {
            // we can run out of memory if we have too many threads
            // log the event and sleep for a minute and give
            // some thread(s) a chance to finish
            LOG.warn("Out of Memory in server select", e);
            closeCurrentConnection(key);
            cleanupConnections(true);
            try { Thread.sleep(60000); } catch (Exception ignored) {}
      }
        } catch (Exception e) {
          closeCurrentConnection(key);
        }
        cleanupConnections(false);
      }
      LOG.info("Stopping " + this.getName());

      synchronized (this) {
        try {
          acceptChannel.close();
          selector.close();
        } catch (IOException ignored) { }

        selector= null;
        acceptChannel= null;

        // clean up all connections
        while (!connectionList.isEmpty()) {
          closeConnection(connectionList.remove(0));
        }
      }
    }

    private void closeCurrentConnection(SelectionKey key) {
      if (key != null) {
        Connection c = (Connection)key.attachment();
        if (c != null) {
          if (LOG.isDebugEnabled())
            LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
          closeConnection(c);
        }
      }
    }

    InetSocketAddress getAddress() {
      return (InetSocketAddress)acceptChannel.socket().getLocalSocketAddress();
    }

    void doAccept(SelectionKey key) throws IOException, OutOfMemoryError {
      Connection c;
      ServerSocketChannel server = (ServerSocketChannel) key.channel();

      SocketChannel channel;
      while ((channel = server.accept()) != null) {
        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(tcpNoDelay);
        channel.socket().setKeepAlive(tcpKeepAlive);

        Reader reader = getReader();
        try {
          reader.startAdd();
          SelectionKey readKey = reader.registerChannel(channel);
          c = new Connection(channel, System.currentTimeMillis());
          readKey.attach(c);
          synchronized (connectionList) {
            connectionList.add(numConnections, c);
            numConnections++;
          }
          if (LOG.isDebugEnabled())
            LOG.debug("Server connection from " + c.toString() +
                "; # active connections: " + numConnections +
                "; # queued calls: " + callQueue.size());
        } finally {
          reader.finishAdd();
        }
      }
    }

    void doRead(SelectionKey key) throws InterruptedException {
      int count = 0;
      Connection c = (Connection)key.attachment();
      if (c == null) {
        return;
      }
      c.setLastContact(System.currentTimeMillis());

      try {
        count = c.readAndProcess();
      } catch (InterruptedException ieo) {
        throw ieo;
      } catch (Exception e) {
        LOG.warn(getName() + ": readAndProcess threw exception " + e + ". Count of bytes read: " + count, e);
        count = -1; //so that the (count < 0) block is executed
      }
      if (count < 0) {
        if (LOG.isDebugEnabled())
          LOG.debug(getName() + ": disconnecting client " +
                    c.getHostAddress() + ". Number of active connections: "+
                    numConnections);
        closeConnection(c);
        // c = null;
      }
      else {
        c.setLastContact(System.currentTimeMillis());
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
          LOG.info(getName() + ":Exception in closing listener socket. " + e);
        }
      }
      readPool.shutdownNow();
    }

    // The method that will return the next reader to work with
    // Simplistic implementation of round robin for now
    Reader getReader() {
      currentReader = (currentReader + 1) % readers.length;
      return readers[currentReader];
    }
  }

  // Sends responses of RPC back to clients.
  private class Responder extends Thread {
    private Selector writeSelector;
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
      LOG.info(getName() + ": starting");
      SERVER.set(HBaseServer.this);
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
              if (key.isValid() && key.isWritable()) {
                  doAsyncWrite(key);
              }
            } catch (IOException e) {
              LOG.info(getName() + ": doAsyncWrite threw exception " + e);
            }
          }
          long now = System.currentTimeMillis();
          if (now < lastPurgeTime + PURGE_INTERVAL) {
            continue;
          }
          lastPurgeTime = now;
          //
          // If there were some calls that have not been sent out for a
          // long time, discard them.
          //
          LOG.debug("Checking for old call responses.");
          ArrayList<Call> calls;

          // get the list of channels from list of keys.
          synchronized (writeSelector.keys()) {
            calls = new ArrayList<Call>(writeSelector.keys().size());
            iter = writeSelector.keys().iterator();
            while (iter.hasNext()) {
              SelectionKey key = iter.next();
              Call call = (Call)key.attachment();
              if (call != null && key.channel() == call.connection.channel) {
                calls.add(call);
              }
            }
          }

          for(Call call : calls) {
            doPurge(call, now);
          }
        } catch (OutOfMemoryError e) {
          if (errorHandler != null) {
            if (errorHandler.checkOOME(e)) {
              LOG.info(getName() + ": exiting on OOME");
              return;
            }
          } else {
            //
            // we can run out of memory if we have too many threads
            // log the event and sleep for a minute and give
            // some thread(s) a chance to finish
            //
            LOG.warn("Out of Memory in server select", e);
            try { Thread.sleep(60000); } catch (Exception ignored) {}
      }
        } catch (Exception e) {
          LOG.warn("Exception in Responder " +
                   StringUtils.stringifyException(e));
        }
      }
      LOG.info("Stopping " + this.getName());
    }

    private void doAsyncWrite(SelectionKey key) throws IOException {
      Call call = (Call)key.attachment();
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
    private void doPurge(Call call, long now) {
      synchronized (call.connection.responseQueue) {
        Iterator<Call> iter = call.connection.responseQueue.listIterator(0);
        while (iter.hasNext()) {
          Call nextCall = iter.next();
          if (now > nextCall.timestamp + PURGE_INTERVAL) {
            closeConnection(nextCall.connection);
            break;
          }
        }
      }
    }

    // Processes one response. Returns true if there are no more pending
    // data for this channel.
    //
    @SuppressWarnings({"ConstantConditions"})
    private boolean processResponse(final LinkedList<Call> responseQueue,
                                    boolean inHandler) throws IOException {
      boolean error = true;
      boolean done = false;       // there is more data for this channel.
      int numElements;
      Call call = null;
      try {
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
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
            LOG.debug(getName() + ": responding to #" + call.id + " from " +
                      call.connection);
          }
          //
          // Send as much data as we can in the non-blocking fashion
          //
          int numBytes = channelWrite(channel, call.response);
          if (numBytes < 0) {
            return true;
          }
          if (!call.response.hasRemaining()) {
            call.connection.decRpcCount();
            //noinspection RedundantIfStatement
            if (numElements == 1) {    // last call fully processes.
              done = true;             // no more data for this channel.
            } else {
              done = false;            // more calls pending to be sent.
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(getName() + ": responding to #" + call.id + " from " +
                        call.connection + " Wrote " + numBytes + " bytes.");
            }
          } else {
            //
            // If we were unable to write the entire response out, then
            // insert in Selector queue.
            //
            call.connection.responseQueue.addFirst(call);

            if (inHandler) {
              // set the serve time when the response has to be sent later
              call.timestamp = System.currentTimeMillis();
              if (enqueueInSelector(call))
                done = true;
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(getName() + ": responding to #" + call.id + " from " +
                        call.connection + " Wrote partial " + numBytes +
                        " bytes.");
            }
          }
          error = false;              // everything went off well
        }
      } finally {
        if (error && call != null) {
          LOG.warn(getName()+", call " + call + ": output error");
          done = true;               // error. no more data for this channel.
          closeConnection(call.connection);
        }
      }
      return done;
    }

    //
    // Enqueue for background thread to send responses out later.
    //
    private boolean enqueueInSelector(Call call) throws IOException {
      boolean done = false;
      incPending();
      try {
        // Wake up the thread blocked on select, only then can the call
        // to channel.register() complete.
        SocketChannel channel = call.connection.channel;
        writeSelector.wakeup();
        channel.register(writeSelector, SelectionKey.OP_WRITE, call);
      } catch (ClosedChannelException e) {
        //It's OK.  Channel might be closed else where.
        done = true;
      } finally {
        decPending();
      }
      return done;
    }

    //
    // Enqueue a response from the application.
    //
    void doRespond(Call call) throws IOException {
      // set the serve time when the response has to be sent later
      call.timestamp = System.currentTimeMillis();

      boolean doRegister = false;
      synchronized (call.connection.responseQueue) {
        call.connection.responseQueue.addLast(call);
        if (call.connection.responseQueue.size() == 1) {
          doRegister = !processResponse(call.connection.responseQueue, false);
        }
      }
      if (doRegister) {
        enqueueInSelector(call);
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

  /** Reads calls from a connection and queues them for handling. */
  private class Connection {
    private boolean versionRead = false; //if initial signature and
                                         //version are read
    private boolean headerRead = false;  //if the connection header that
                                         //follows version is read.
    protected SocketChannel channel;
    private ByteBuffer data;
    private ByteBuffer dataLengthBuffer;
    protected final LinkedList<Call> responseQueue;
    private volatile int rpcCount = 0; // number of outstanding rpcs
    private long lastContact;
    private int dataLength;
    protected Socket socket;
    // Cache the remote host & port info so that even if the socket is
    // disconnected, we can say where it used to connect to.
    private String hostAddress;
    private int remotePort;
    ConnectionHeader header = new ConnectionHeader();
    Class<? extends VersionedProtocol> protocol;
    protected UserGroupInformation ticket = null;

    public Connection(SocketChannel channel, long lastContact) {
      this.channel = channel;
      this.lastContact = lastContact;
      this.data = null;
      this.dataLengthBuffer = ByteBuffer.allocate(4);
      this.socket = channel.socket();
      InetAddress addr = socket.getInetAddress();
      if (addr == null) {
        this.hostAddress = "*Unknown*";
      } else {
        this.hostAddress = addr.getHostAddress();
      }
      this.remotePort = socket.getPort();
      this.responseQueue = new LinkedList<Call>();
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

    public String getHostAddress() {
      return hostAddress;
    }

    public void setLastContact(long lastContact) {
      this.lastContact = lastContact;
    }

    public long getLastContact() {
      return lastContact;
    }

    /* Return true if the connection has no outstanding rpc */
    private boolean isIdle() {
      return rpcCount == 0;
    }

    /* Decrement the outstanding RPC count */
    protected void decRpcCount() {
      rpcCount--;
    }

    /* Increment the outstanding RPC count */
    private void incRpcCount() {
      rpcCount++;
    }

    protected boolean timedOut(long currentTime) {
      return isIdle() && currentTime - lastContact > maxIdleTime;
    }

    public int readAndProcess() throws IOException, InterruptedException {
      while (true) {
        /* Read at most one RPC. If the header is not read completely yet
         * then iterate until we read first RPC or until there is no data left.
         */
        int count;
        if (dataLengthBuffer.remaining() > 0) {
          count = channelRead(channel, dataLengthBuffer);
          if (count < 0 || dataLengthBuffer.remaining() > 0)
            return count;
        }

        if (!versionRead) {
          //Every connection is expected to send the header.
          ByteBuffer versionBuffer = ByteBuffer.allocate(1);
          count = channelRead(channel, versionBuffer);
          if (count <= 0) {
            return count;
          }
          int version = versionBuffer.get(0);

          dataLengthBuffer.flip();
          if (!HEADER.equals(dataLengthBuffer) || version != CURRENT_VERSION) {
            //Warning is ok since this is not supposed to happen.
            LOG.warn("Incorrect header or version mismatch from " +
                     hostAddress + ":" + remotePort +
                     " got version " + version +
                     " expected version " + CURRENT_VERSION);
            return -1;
          }
          dataLengthBuffer.clear();
          versionRead = true;
          continue;
        }

        if (data == null) {
          dataLengthBuffer.flip();
          dataLength = dataLengthBuffer.getInt();

          if (dataLength == HBaseClient.PING_CALL_ID) {
            dataLengthBuffer.clear();
            return 0;  //ping message
          }
          data = ByteBuffer.allocate(dataLength);
          incRpcCount();  // Increment the rpc count
        }

        count = channelRead(channel, data);

        if (data.remaining() == 0) {
          dataLengthBuffer.clear();
          data.flip();
          if (headerRead) {
            processData();
            data = null;
            return count;
          }
          processHeader();
          headerRead = true;
          data = null;
          continue;
        }
        return count;
      }
    }

    /// Reads the connection header following version
    private void processHeader() throws IOException {
      DataInputStream in =
        new DataInputStream(new ByteArrayInputStream(data.array()));
      header.readFields(in);
      try {
        String protocolClassName = header.getProtocol();
        if (protocolClassName != null) {
          protocol = getProtocolClass(header.getProtocol(), conf);
        }
      } catch (ClassNotFoundException cnfe) {
        throw new IOException("Unknown protocol: " + header.getProtocol());
      }

      ticket = header.getUgi();
    }

    private void processData() throws  IOException, InterruptedException {
      byte[] array = data.array();
      DataInputStream dis =
        new DataInputStream(new ByteArrayInputStream(array));
      int id = dis.readInt();                    // try to read an id

      if (LOG.isDebugEnabled())
        LOG.debug(" got call #" + id + ", " + array.length + " bytes");

      Writable param = ReflectionUtils.newInstance(paramClass, conf);           // read param
      param.readFields(dis);

      Call call = new Call(id, param, this, responder);

      if (priorityCallQueue != null && getQosLevel(param) > highPriorityLevel) {
        priorityCallQueue.put(call);
      } else {
        callQueue.put(call);              // queue the call; maybe blocked here
      }
    }

    protected synchronized void close() {
      data = null;
      dataLengthBuffer = null;
      if (!channel.isOpen())
        return;
      try {socket.shutdownOutput();} catch(Exception ignored) {} // FindBugs DE_MIGHT_IGNORE
      if (channel.isOpen()) {
        try {channel.close();} catch(Exception ignored) {}
      }
      try {socket.close();} catch(Exception ignored) {}
    }
  }

  /** Handles queued calls . */
  private class Handler extends Thread {
    private final BlockingQueue<Call> myCallQueue;

    public Handler(final BlockingQueue<Call> cq, int instanceNumber) {
      this.myCallQueue = cq;
      this.setDaemon(true);

      String threadName = "IPC Server handler " + instanceNumber + " on " + port;
      if (cq == priorityCallQueue) {
        // this is just an amazing hack, but it works.
        threadName = "PRI " + threadName;
      }
      this.setName(threadName);
    }

    @Override
    public void run() {
      LOG.info(getName() + ": starting");
      SERVER.set(HBaseServer.this);
      while (running) {
        try {
          Call call = myCallQueue.take(); // pop the queue; maybe blocked here

          if (LOG.isDebugEnabled())
            LOG.debug(getName() + ": has #" + call.id + " from " +
                      call.connection);

          String errorClass = null;
          String error = null;
          Writable value = null;

          CurCall.set(call);
          try {
            if (!started)
              throw new ServerNotRunningYetException("Server is not running yet");
            value = call(call.connection.protocol, call.param, call.timestamp);             // make the call
          } catch (Throwable e) {
            LOG.debug(getName()+", call "+call+": error: " + e, e);
            errorClass = e.getClass().getName();
            error = StringUtils.stringifyException(e);
          }
          CurCall.set(null);

          // Set the response for undelayed calls and delayed calls with
          // undelayed responses.
          if (!call.isDelayed() || !call.isReturnValueDelayed()) {
            call.setResponse(value, errorClass, error);
          }
          call.sendResponseIfReady();
        } catch (InterruptedException e) {
          if (running) {                          // unexpected -- log it
            LOG.info(getName() + " caught: " +
                     StringUtils.stringifyException(e));
          }
        } catch (OutOfMemoryError e) {
          if (errorHandler != null) {
            if (errorHandler.checkOOME(e)) {
              LOG.info(getName() + ": exiting on OOME");
              return;
            }
          } else {
            // rethrow if no handler
            throw e;
          }
        } catch (Exception e) {
          LOG.warn(getName() + " caught: " +
                   StringUtils.stringifyException(e));
        }
      }
      LOG.info(getName() + ": exiting");
    }

  }

  /**
   * Gets the QOS level for this call.  If it is higher than the highPriorityLevel and there
   * are priorityHandlers available it will be processed in it's own thread set.
   *
   * @param param
   * @return priority, higher is better
   */
  private Function<Writable,Integer> qosFunction = null;
  @Override
  public void setQosFunction(Function<Writable, Integer> newFunc) {
    qosFunction = newFunc;
  }

  protected int getQosLevel(Writable param) {
    if (qosFunction == null) {
      return 0;
    }

    Integer res = qosFunction.apply(param);
    if (res == null) {
      return 0;
    }
    return res;
  }

  /* Constructs a server listening on the named port and address.  Parameters passed must
   * be of the named class.  The <code>handlerCount</handlerCount> determines
   * the number of handler threads that will be used to process calls.
   *
   */
  protected HBaseServer(String bindAddress, int port,
                        Class<? extends Writable> paramClass, int handlerCount,
                        int priorityHandlerCount, Configuration conf, String serverName,
                        int highPriorityLevel)
    throws IOException {
    this.bindAddress = bindAddress;
    this.conf = conf;
    this.port = port;
    this.paramClass = paramClass;
    this.handlerCount = handlerCount;
    this.priorityHandlerCount = priorityHandlerCount;
    this.socketSendBufferSize = 0;
    this.maxQueueSize =
      this.conf.getInt("ipc.server.max.queue.size",
        handlerCount * DEFAULT_MAX_QUEUE_SIZE_PER_HANDLER);
     this.readThreads = conf.getInt(
        "ipc.server.read.threadpool.size",
        10);
    this.callQueue  = new LinkedBlockingQueue<Call>(maxQueueSize);
    if (priorityHandlerCount > 0) {
      this.priorityCallQueue = new LinkedBlockingQueue<Call>(maxQueueSize); // TODO hack on size
    } else {
      this.priorityCallQueue = null;
    }
    this.highPriorityLevel = highPriorityLevel;
    this.maxIdleTime = 2*conf.getInt("ipc.client.connection.maxidletime", 1000);
    this.maxConnectionsToNuke = conf.getInt("ipc.client.kill.max", 10);
    this.thresholdIdleConnections = conf.getInt("ipc.client.idlethreshold", 4000);

    // Start the listener here and let it bind to the port
    listener = new Listener();
    this.port = listener.getAddress().getPort();
    this.rpcMetrics = new HBaseRpcMetrics(serverName,
                          Integer.toString(this.port));
    this.tcpNoDelay = conf.getBoolean("ipc.server.tcpnodelay", false);
    this.tcpKeepAlive = conf.getBoolean("ipc.server.tcpkeepalive", true);

    this.warnDelayedCalls = conf.getInt(WARN_DELAYED_CALLS,
                                        DEFAULT_WARN_DELAYED_CALLS);
    this.delayedCalls = new AtomicInteger(0);


    // Create the responder here
    responder = new Responder();
  }

  protected void closeConnection(Connection connection) {
    synchronized (connectionList) {
      if (connectionList.remove(connection))
        numConnections--;
    }
    connection.close();
  }

  /** Sets the socket buffer size used for responding to RPCs.
   * @param size send size
   */
  @Override
  public void setSocketSendBufSize(int size) { this.socketSendBufferSize = size; }

  /** Starts the service.  Must be called before any calls will be handled. */
  @Override
  public void start() {
    startThreads();
    openServer();
  }

  /**
   * Open a previously started server.
   */
  @Override
  public void openServer() {
    started = true;
  }

  /**
   * Starts the service threads but does not allow requests to be responded yet.
   * Client will get {@link ServerNotRunningYetException} instead.
   */
  @Override
  public synchronized void startThreads() {
    responder.start();
    listener.start();
    handlers = new Handler[handlerCount];

    for (int i = 0; i < handlerCount; i++) {
      handlers[i] = new Handler(callQueue, i);
      handlers[i].start();
    }

    if (priorityHandlerCount > 0) {
      priorityHandlers = new Handler[priorityHandlerCount];
      for (int i = 0 ; i < priorityHandlerCount; i++) {
        priorityHandlers[i] = new Handler(priorityCallQueue, i);
        priorityHandlers[i].start();
      }
    }
  }

  /** Stops the service.  No new calls will be handled after this is called. */
  @Override
  public synchronized void stop() {
    LOG.info("Stopping server on " + port);
    running = false;
    if (handlers != null) {
      for (Handler handler : handlers) {
        if (handler != null) {
          handler.interrupt();
        }
      }
    }
    if (priorityHandlers != null) {
      for (Handler handler : priorityHandlers) {
        if (handler != null) {
          handler.interrupt();
        }
      }
    }
    listener.interrupt();
    listener.doStop();
    responder.interrupt();
    notifyAll();
    if (this.rpcMetrics != null) {
      this.rpcMetrics.shutdown();
    }
  }

  /** Wait for the server to be stopped.
   * Does not wait for all subthreads to finish.
   *  See {@link #stop()}.
   * @throws InterruptedException e
   */
  @Override
  public synchronized void join() throws InterruptedException {
    while (running) {
      wait();
    }
  }

  /**
   * Return the socket (ip+port) on which the RPC server is listening to.
   * @return the socket (ip+port) on which the RPC server is listening to.
   */
  @Override
  public synchronized InetSocketAddress getListenerAddress() {
    return listener.getAddress();
  }

  /**
   * The number of open RPC conections
   * @return the number of open rpc connections
   */
  @Override
  public int getNumOpenConnections() {
    return numConnections;
  }

  /**
   * The number of rpc calls in the queue.
   * @return The number of rpc calls in the queue.
   */
  @Override
  public int getCallQueueLen() {
    return callQueue.size();
  }

  /**
   * Set the handler for calling out of RPC for error conditions.
   * @param handler the handler implementation
   */
  @Override
  public void setErrorHandler(HBaseRPCErrorHandler handler) {
    this.errorHandler = handler;
  }

  /**
   * Returns the metrics instance for reporting RPC call statistics
   */
  public HBaseRpcMetrics getRpcMetrics() {
    return rpcMetrics;
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
   * @param channel writable byte channel to write to
   * @param buffer buffer to write
   * @return number of bytes written
   * @throws java.io.IOException e
   * @see WritableByteChannel#write(ByteBuffer)
   */
  protected static int channelWrite(WritableByteChannel channel,
                                    ByteBuffer buffer) throws IOException {
    return (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
           channel.write(buffer) : channelIO(null, channel, buffer);
  }

  /**
   * This is a wrapper around {@link ReadableByteChannel#read(ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks.
   * This is to avoid jdk from creating many direct buffers as the size of
   * ByteBuffer increases. There should not be any performance degredation.
   *
   * @param channel writable byte channel to write on
   * @param buffer buffer to write
   * @return number of bytes written
   * @throws java.io.IOException e
   * @see ReadableByteChannel#read(ByteBuffer)
   */
  protected static int channelRead(ReadableByteChannel channel,
                                   ByteBuffer buffer) throws IOException {
    return (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
           channel.read(buffer) : channelIO(channel, null, buffer);
  }

  /**
   * Helper for {@link #channelRead(ReadableByteChannel, ByteBuffer)}
   * and {@link #channelWrite(WritableByteChannel, ByteBuffer)}. Only
   * one of readCh or writeCh should be non-null.
   *
   * @param readCh read channel
   * @param writeCh write channel
   * @param buf buffer to read or write into/out of
   * @return bytes written
   * @throws java.io.IOException e
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

  public Delayable getCurrentCall() {
    return CurCall.get();
  }
}
