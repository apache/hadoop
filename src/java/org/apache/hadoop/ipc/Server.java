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

import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.BufferedOutputStream;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.io.ByteArrayInputStream;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.*;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.ipc.SocketChannelOutputStream;
import org.apache.hadoop.util.*;

/** An abstract IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @author Doug Cutting
 * @see Client
 */
public abstract class Server {
  
  /**
   * The first four bytes of Hadoop RPC connections
   */
  public static final ByteBuffer HEADER = ByteBuffer.wrap("hrpc".getBytes());
  
  /**
   * How much time should be allocated for actually running the handler?
   * Calls that are older than ipc.timeout * MAX_CALL_QUEUE_TIME
   * are ignored when the handler takes them off the queue.
   */
  private static final float MAX_CALL_QUEUE_TIME = 0.6f;
  
  /**
   * How many calls/handler are allowed in the queue.
   */
  private static final int MAX_QUEUE_SIZE_PER_HANDLER = 100;
  
  public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.ipc.Server");

  private static final ThreadLocal SERVER = new ThreadLocal();

  /** Returns the server instance called under or null.  May be called under
   * {@link #call(Writable)} implementations, and under {@link Writable}
   * methods of paramters and return values.  Permits applications to access
   * the server context.*/
  public static Server get() {
    return (Server)SERVER.get();
  }
  private String bindAddress; 
  private int port;                               // port we listen on
  private int handlerCount;                       // number of handler threads
  private Class paramClass;                       // class of call parameters
  private int maxIdleTime;                        // the maximum idle time after 
                                                  // which a client may be disconnected
  private int thresholdIdleConnections;           // the number of idle connections
                                                  // after which we will start
                                                  // cleaning up idle 
                                                  // connections
  int maxConnectionsToNuke;                       // the max number of 
                                                  // connections to nuke
                                                  //during a cleanup
  
  private Configuration conf;

  private int timeout;
  private long maxCallStartAge;
  private int maxQueueSize;

  private boolean running = true;                 // true while server runs
  private LinkedList callQueue = new LinkedList(); // queued calls
  private Object callDequeued = new Object();     // used by wait/notify

  private List connectionList = 
       Collections.synchronizedList(new LinkedList()); //maintain a list
                                                       //of client connectionss
  private Listener listener;
  private int numConnections = 0;
  
  /** A call queued for handling. */
  private static class Call {
    private int id;                               // the client's call id
    private Writable param;                       // the parameter passed
    private Connection connection;                // connection to client
    private long receivedTime;                    // the time received

    public Call(int id, Writable param, Connection connection) {
      this.id = id;
      this.param = param;
      this.connection = connection;
      this.receivedTime = System.currentTimeMillis();
    }
    
    public String toString() {
      return param.toString() + " from " + connection.toString();
    }
  }

  /** Listens on the socket. Creates jobs for the handler threads*/
  private class Listener extends Thread {
    
    private ServerSocketChannel acceptChannel = null; //the accept channel
    private Selector selector = null; //the selector that we use for the server
    private InetSocketAddress address; //the address we bind at
    private Random rand = new Random();
    private long lastCleanupRunTime = 0; //the last time when a cleanup connec-
                                         //-tion (for idle connections) ran
    private long cleanupInterval = 10000; //the minimum interval between 
                                          //two cleanup runs
    private int backlogLength = conf.getInt("ipc.server.listen.queue.size", 128);
    
    public Listener() throws IOException {
      address = new InetSocketAddress(bindAddress,port);
      // Create a new server socket and set to non blocking mode
      acceptChannel = ServerSocketChannel.open();
      acceptChannel.configureBlocking(false);

      // Bind the server socket to the local host and port
      acceptChannel.socket().bind(address, backlogLength);
      // create a selector;
      selector= Selector.open();

      // Register accepts on the server socket with the selector.
      acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
      this.setName("IPC Server listener on " + port);
      this.setDaemon(true);
    }
    /** cleanup connections from connectionList. Choose a random range
     * to scan and also have a limit on the number of the connections
     * that will be cleanedup per run. The criteria for cleanup is the time
     * for which the connection was idle. If 'force' is true then all 
     * connections will be looked at for the cleanup.
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
              c = (Connection)connectionList.get(i);
            } catch (Exception e) {return;}
          }
          if (c.timedOut(currentTime)) {
            synchronized (connectionList) {
              if (connectionList.remove(c))
                numConnections--;
            }
            try {
              if (LOG.isDebugEnabled())
                LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
              c.close();
            } catch (Exception e) {}
            numNuked++;
            end--;
            c = null;
            if (!force && numNuked == maxConnectionsToNuke) break;
          }
          else i++;
        }
        lastCleanupRunTime = System.currentTimeMillis();
      }
    }

    public void run() {
      LOG.info(getName() + ": starting");
      SERVER.set(Server.this);
      while (running) {
        SelectionKey key = null;
        try {
          selector.select();
          Iterator iter = selector.selectedKeys().iterator();
          
          while (iter.hasNext()) {
            key = (SelectionKey)iter.next();
            iter.remove();
            try {
              if (key.isValid()) {
                if (key.isAcceptable())
                  doAccept(key);
                else if (key.isReadable())
                  doRead(key);
              }
            } catch (IOException e) {
              key.cancel();
            }
            key = null;
          }
        } catch (OutOfMemoryError e) {
          // we can run out of memory if we have too many threads
          // log the event and sleep for a minute and give 
          // some thread(s) a chance to finish
          LOG.warn("Out of Memory in server select", e);
          closeCurrentConnection(key, e);
          cleanupConnections(true);
          try { Thread.sleep(60000); } catch (Exception ie) {}
        } catch (Exception e) {
          closeCurrentConnection(key, e);
        }
        cleanupConnections(false);
      }
      LOG.info("Stopping " + this.getName());

      try {
        acceptChannel.close();
        selector.close();
      } catch (IOException e) { }

      synchronized (this) {
        selector= null;
        acceptChannel= null;
        connectionList = null;
      }
    }

    private void closeCurrentConnection(SelectionKey key, Throwable e) {
      if (key != null) {
        Connection c = (Connection)key.attachment();
        if (c != null) {
          synchronized (connectionList) {
            if (connectionList.remove(c))
              numConnections--;
          }
          try {
            if (LOG.isDebugEnabled())
              LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
            c.close();
          } catch (Exception ex) {}
          c = null;
        }
      }
    }

    void doAccept(SelectionKey key) throws IOException,  OutOfMemoryError {
      Connection c = null;
      ServerSocketChannel server = (ServerSocketChannel) key.channel();
      SocketChannel channel = server.accept();
      channel.configureBlocking(false);
      SelectionKey readKey = channel.register(selector, SelectionKey.OP_READ);
      c = new Connection(readKey, channel, System.currentTimeMillis());
      readKey.attach(c);
      synchronized (connectionList) {
        connectionList.add(numConnections, c);
        numConnections++;
      }
      if (LOG.isDebugEnabled())
        LOG.debug("Server connection from " + c.toString() +
                "; # active connections: " + numConnections +
                "; # queued calls: " + callQueue.size() );
    }

    void doRead(SelectionKey key) {
      int count = 0;
      Connection c = (Connection)key.attachment();
      if (c == null) {
        return;  
      }
      c.setLastContact(System.currentTimeMillis());
      
      try {
        count = c.readAndProcess();
      } catch (Exception e) {
        key.cancel();
        LOG.debug(getName() + ": readAndProcess threw exception " + e + ". Count of bytes read: " + count, e);
        count = -1; //so that the (count < 0) block is executed
      }
      if (count < 0) {
        synchronized (connectionList) {
          if (connectionList.remove(c))
            numConnections--;
        }
        try {
          if (LOG.isDebugEnabled())
            LOG.debug(getName() + ": disconnecting client " + 
                  c.getHostAddress() + ". Number of active connections: "+
                  numConnections);
          c.close();
        } catch (Exception e) {}
        c = null;
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
    }
  }

  /** Reads calls from a connection and queues them for handling. */
  private class Connection {
    private boolean firstData = true;
    private SocketChannel channel;
    private SelectionKey key;
    private ByteBuffer data;
    private ByteBuffer dataLengthBuffer;
    private DataOutputStream out;
    private SocketChannelOutputStream channelOut;
    private long lastContact;
    private int dataLength;
    private Socket socket;
    // Cache the remote host & port info so that even if the socket is 
    // disconnected, we can say where it used to connect to.
    private String hostAddress;
    private int remotePort;

    public Connection(SelectionKey key, SocketChannel channel, 
    long lastContact) {
      this.key = key;
      this.channel = channel;
      this.lastContact = lastContact;
      this.data = null;
      this.dataLengthBuffer = ByteBuffer.allocate(4);
      this.socket = channel.socket();
      this.out = new DataOutputStream
        (new BufferedOutputStream(
         this.channelOut = new SocketChannelOutputStream( channel )));
      InetAddress addr = socket.getInetAddress();
      if (addr == null) {
        this.hostAddress = "*Unknown*";
      } else {
        this.hostAddress = addr.getHostAddress();
      }
      this.remotePort = socket.getPort();
    }   

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

    private boolean timedOut() {
      if(System.currentTimeMillis() -  lastContact > maxIdleTime)
        return true;
      return false;
    }

    private boolean timedOut(long currentTime) {
        if(currentTime -  lastContact > maxIdleTime)
          return true;
        return false;
    }

    public int readAndProcess() throws IOException, InterruptedException {
      int count = -1;
      if (dataLengthBuffer.remaining() > 0) {
        count = channel.read(dataLengthBuffer);       
        if ( count < 0 || dataLengthBuffer.remaining() > 0 ) 
          return count;        
        dataLengthBuffer.flip(); 
        // Is this a new style header?
        if (firstData && HEADER.equals(dataLengthBuffer)) {
          // If so, read the version
          ByteBuffer versionBuffer = ByteBuffer.allocate(1);
          count = channel.read(versionBuffer);
          if (count < 0) {
            return count;
          }
          // read the first length
          dataLengthBuffer.clear();
          count = channel.read(dataLengthBuffer);
          if (count < 0 || dataLengthBuffer.remaining() > 0) {
            return count;
          }
          dataLengthBuffer.flip();
          firstData = false;
        }
        dataLength = dataLengthBuffer.getInt();
        data = ByteBuffer.allocate(dataLength);
      }
      count = channel.read(data);
      if (data.remaining() == 0) {
        data.flip();
        processData();
        dataLengthBuffer.flip();
        data = null; 
      }
      return count;
    }

    private void processData() throws  IOException, InterruptedException {
      DataInputStream dis =
          new DataInputStream(new ByteArrayInputStream( data.array() ));
      int id = dis.readInt();                    // try to read an id
        
      if (LOG.isDebugEnabled())
        LOG.debug(" got #" + id);
            
      Writable param = (Writable)ReflectionUtils.newInstance(paramClass, conf);           // read param
      param.readFields(dis);        
        
      Call call = new Call(id, param, this);
      synchronized (callQueue) {
        if (callQueue.size() >= maxQueueSize) {
          Call oldCall = (Call) callQueue.removeFirst();
          LOG.warn("Call queue overflow discarding oldest call " + oldCall);
        }
        callQueue.addLast(call);              // queue the call
        callQueue.notify();                   // wake up a waiting handler
      }
        
    }

    private void close() throws IOException {
      data = null;
      dataLengthBuffer = null;
      if (!channel.isOpen())
        return;
      try {socket.shutdownOutput();} catch(Exception e) {}
      try {out.close();} catch(Exception e) {}
      try {channelOut.destroy();} catch(Exception e) {}
      if (channel.isOpen()) {
        try {channel.close();} catch(Exception e) {}
      }
      try {socket.close();} catch(Exception e) {}
      try {key.cancel();} catch(Exception e) {}
      key = null;
    }
  }

  /** Handles queued calls . */
  private class Handler extends Thread {
    public Handler(int instanceNumber) {
      this.setDaemon(true);
      this.setName("IPC Server handler "+ instanceNumber + " on " + port);
    }

    public void run() {
      LOG.info(getName() + ": starting");
      SERVER.set(Server.this);
      while (running) {
        try {
          Call call;
          synchronized (callQueue) {
            while (running && callQueue.size()==0) { // wait for a call
              callQueue.wait(timeout);
            }
            if (!running) break;
            call = (Call)callQueue.removeFirst(); // pop the queue
          }

          synchronized (callDequeued) {           // tell others we've dequeued
            callDequeued.notify();
          }

          // throw the message away if it is too old
          if (System.currentTimeMillis() - call.receivedTime > 
              maxCallStartAge) {
            ReflectionUtils.logThreadInfo(LOG, "Discarding call " + call, 30);
            LOG.warn("Call " + call.toString() + 
                     " discarded for being too old (" +
                     (System.currentTimeMillis() - call.receivedTime) + ")");
            continue;
          }
          
          if (LOG.isDebugEnabled())
            LOG.debug(getName() + ": has #" + call.id + " from " +
                     call.connection);
          
          String errorClass = null;
          String error = null;
          Writable value = null;
          try {
            value = call(call.param);             // make the call
          } catch (Throwable e) {
            LOG.info(getName() + " call error: " + e, e);
            errorClass = e.getClass().getName();
            error = getStackTrace(e);
          }
            
          DataOutputStream out = call.connection.out;
          synchronized (out) {
            try {
              out.writeInt(call.id);                // write call id
              out.writeBoolean(error!=null);        // write error flag
              if (error == null) {
                value.write(out);
              } else {
                WritableUtils.writeString(out, errorClass);
                WritableUtils.writeString(out, error);
              }
              out.flush();
            } catch (Exception e) {
              LOG.warn("handler output error", e);
              synchronized (connectionList) {
                if (connectionList.remove(call.connection))
                  numConnections--;
              }
              call.connection.close();
            }
          }

        } catch (Exception e) {
          LOG.info(getName() + " caught: " + e, e);
        }
      }
      LOG.info(getName() + ": exiting");
    }

    private String getStackTrace(Throwable throwable) {
      StringWriter stringWriter = new StringWriter();
      PrintWriter printWriter = new PrintWriter(stringWriter);
      throwable.printStackTrace(printWriter);
      printWriter.flush();
      return stringWriter.toString();
    }

  }
  /** Constructs a server listening on the named port and address.  Parameters passed must
   * be of the named class.  The <code>handlerCount</handlerCount> determines
   * the number of handler threads that will be used to process calls.
   * 
   */
  protected Server(String bindAddress, int port, Class paramClass, int handlerCount, Configuration conf) {
    this.bindAddress = bindAddress;
    this.conf = conf;
    this.port = port;
    this.paramClass = paramClass;
    this.handlerCount = handlerCount;
    this.timeout = conf.getInt("ipc.client.timeout",10000);
    maxCallStartAge = (long) (timeout * MAX_CALL_QUEUE_TIME);
    maxQueueSize = handlerCount * MAX_QUEUE_SIZE_PER_HANDLER;
    this.maxIdleTime = conf.getInt("ipc.client.maxidletime", 120000);
    this.maxConnectionsToNuke = conf.getInt("ipc.client.kill.max", 10);
    this.thresholdIdleConnections = conf.getInt("ipc.client.idlethreshold", 4000);
  }

  /** Sets the timeout used for network i/o. */
  public void setTimeout(int timeout) { this.timeout = timeout; }

  /** Starts the service.  Must be called before any calls will be handled. */
  public synchronized void start() throws IOException {
    listener = new Listener();
    listener.start();
    
    for (int i = 0; i < handlerCount; i++) {
      Handler handler = new Handler(i);
      handler.start();
    }
  }

  /** Stops the service.  No new calls will be handled after this is called. */
  public synchronized void stop() {
    LOG.info("Stopping server on " + port);
    running = false;
    listener.doStop();
    notifyAll();
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

  /** Called for each call. */
  public abstract Writable call(Writable param) throws IOException;
  
}
