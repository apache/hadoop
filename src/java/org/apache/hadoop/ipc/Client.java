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

import java.io.IOException;
import java.io.EOFException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;

import java.util.Hashtable;
import java.util.Iterator;

import javax.net.SocketFactory;

import org.apache.commons.logging.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.FSConstants;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/** A client for an IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @see Server
 */
public class Client {
  
  public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.ipc.Client");
  private Hashtable<ConnectionId, Connection> connections =
    new Hashtable<ConnectionId, Connection>();

  private Class valueClass;                       // class of call values
  private int timeout;// timeout for calls
  private int counter;                            // counter for call ids
  private boolean running = true;                 // true while client runs
  private Configuration conf;
  private int maxIdleTime; //connections will be culled if it was idle for 
                           //maxIdleTime msecs
  private int maxRetries; //the max. no. of retries for socket connections
  private boolean tcpNoDelay; // if T then disable Nagle's Algorithm
  private Thread connectionCullerThread;
  private SocketFactory socketFactory;           // how to create sockets
  
  private int refCount = 1;
  
  synchronized void incCount() {
	  refCount++;
  }
  
  synchronized void decCount() {
    refCount--;
  }
  
  synchronized boolean isZeroReference() {
    return refCount==0;
  }
  
  /** A call waiting for a value. */
  private class Call {
    int id;                                       // call id
    Writable param;                               // parameter
    Writable value;                               // value, null if error
    String error;                                 // exception, null if value
    String errorClass;                            // class of exception
    long lastActivity;                            // time of last i/o
    boolean done;                                 // true when call is done

    protected Call(Writable param) {
      this.param = param;
      synchronized (Client.this) {
        this.id = counter++;
      }
      touch();
    }

    /** Called by the connection thread when the call is complete and the
     * value or error string are available.  Notifies by default.  */
    public synchronized void callComplete() {
      notify();                                 // notify caller
    }

    /** Update lastActivity with the current time. */
    public synchronized void touch() {
      lastActivity = System.currentTimeMillis();
    }

    /** Update lastActivity with the current time. */
    public synchronized void setResult(Writable value, 
                                       String errorClass,
                                       String error) {
      this.value = value;
      this.error = error;
      this.errorClass =errorClass;
      this.done = true;
    }
    
  }

  /** Thread that reads responses and notifies callers.  Each connection owns a
   * socket connected to a remote address.  Calls are multiplexed through this
   * socket: responses may be delivered out of order. */
  private class Connection extends Thread {
    private ConnectionId remoteId;
    private Socket socket = null;                 // connected socket
    private DataInputStream in;                   
    private DataOutputStream out;
    // currently active calls
    private Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>();
    private Call readingCall;
    private Call writingCall;
    private int inUse = 0;
    private long lastActivity = 0;
    private boolean shouldCloseConnection = false;

    public Connection(InetSocketAddress address) throws IOException {
      this(new ConnectionId(address, null));
    }
    
    public Connection(ConnectionId remoteId) throws IOException {
      if (remoteId.getAddress().isUnresolved()) {
        throw new UnknownHostException("unknown host: " + 
                                       remoteId.getAddress().getHostName());
      }
      this.remoteId = remoteId;
      this.setName("IPC Client connection to " + 
                   remoteId.getAddress().toString());
      this.setDaemon(true);
    }

    public synchronized void setupIOstreams() throws IOException {
      if (socket != null) {
        notify();
        return;
      }
      short failures = 0;
      while (true) {
        try {
          this.socket = socketFactory.createSocket();
          this.socket.setTcpNoDelay(tcpNoDelay);
          this.socket.connect(remoteId.getAddress());
          break;
        } catch (IOException ie) { //SocketTimeoutException is also caught 
          if (failures == maxRetries) {
            //reset inUse so that the culler gets a chance to throw this
            //connection object out of the table. We don't want to increment
            //inUse to infinity (everytime getConnection is called inUse is
            //incremented)!
            inUse = 0;
            // set socket to null so that the next call to setupIOstreams
            // can start the process of connect all over again.
            socket.close();
            socket = null;
            throw ie;
          }
          failures++;
          LOG.info("Retrying connect to server: " + remoteId.getAddress() + 
                   ". Already tried " + failures + " time(s).");
          try { 
            Thread.sleep(1000);
          } catch (InterruptedException iex){
          }
        }
      }
      socket.setSoTimeout(timeout);
      this.in = new DataInputStream
        (new BufferedInputStream
         (new FilterInputStream(NetUtils.getInputStream(socket)) {
             public int read(byte[] buf, int off, int len) throws IOException {
               int value = super.read(buf, off, len);
               if (readingCall != null) {
                 readingCall.touch();
               }
               return value;
             }
           }));
      this.out = new DataOutputStream
        (new BufferedOutputStream
         (new FilterOutputStream(NetUtils.getOutputStream(socket)) {
             public void write(byte[] buf, int o, int len) throws IOException {
               out.write(buf, o, len);
               if (writingCall != null) {
                 writingCall.touch();
               }
             }
           }));
      writeHeader();
      notify();
    }

    private synchronized void writeHeader() throws IOException {
      out.write(Server.HEADER.array());
      out.write(Server.CURRENT_VERSION);
      //When there are more fields we can have ConnectionHeader Writable.
      DataOutputBuffer buf = new DataOutputBuffer();
      ObjectWritable.writeObject(buf, remoteId.getTicket(), 
                                 UserGroupInformation.class, conf);
      int bufLen = buf.getLength();
      out.writeInt(bufLen);
      out.write(buf.getData(), 0, bufLen);
    }
    
    private synchronized boolean waitForWork() {
      //wait till someone signals us to start reading RPC response or
      //close the connection. If we are idle long enough (blocked in wait),
      //the ConnectionCuller thread will wake us up and ask us to close the
      //connection. 
      //We need to wait when inUse is 0 or socket is null (it may be null if
      //the Connection object has been created but the socket connection
      //has not been setup yet). We stop waiting if we have been asked to close
      //connection
      while ((inUse == 0 || socket == null) && !shouldCloseConnection) {
        try {
          wait();
        } catch (InterruptedException e) {}
      }
      return !shouldCloseConnection;
    }

    private synchronized void incrementRef() {
      inUse++;
    }

    private synchronized void decrementRef() {
      lastActivity = System.currentTimeMillis();
      inUse--;
    }

    public synchronized boolean isIdle() {
      //check whether the connection is in use or just created
      if (inUse != 0) return false;
      long currTime = System.currentTimeMillis();
      if (currTime - lastActivity > maxIdleTime)
        return true;
      return false;
    }

    public InetSocketAddress getRemoteAddress() {
      return remoteId.getAddress();
    }

    public void setCloseConnection() {
      shouldCloseConnection = true;
    }

    public void run() {
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": starting");
      try {
        while (running) {
          int id;
          //wait here for work - read connection or close connection
          if (waitForWork() == false)
            break;
          try {
            id = in.readInt();                    // try to read an id
          } catch (SocketTimeoutException e) {
            continue;
          }

          if (LOG.isDebugEnabled())
            LOG.debug(getName() + " got value #" + id);

          Call call = calls.remove(id);
          boolean isError = in.readBoolean();     // read if error
          if (isError) {
            call.setResult(null, WritableUtils.readString(in),
                           WritableUtils.readString(in));
          } else {
            Writable value = (Writable)ReflectionUtils.newInstance(valueClass, conf);
            try {
              readingCall = call;
              value.readFields(in);                 // read value
            } finally {
              readingCall = null;
            }
            call.setResult(value, null, null);
          }
          call.callComplete();                   // deliver result to caller
          //received the response. So decrement the ref count
          decrementRef();
        }
      } catch (EOFException eof) {
        // This is what happens when the remote side goes down
      } catch (Exception e) {
        LOG.info(StringUtils.stringifyException(e));
      } finally {
        //If there was no exception thrown in this method, then the only
        //way we reached here is by breaking out of the while loop (after
        //waitForWork). And if we took that route to reach here, we have 
        //already removed the connection object in the ConnectionCuller thread.
        //We don't want to remove this again as some other thread might have
        //actually put a new Connection object in the table in the meantime.
        synchronized (connections) {
          if (connections.get(remoteId) == this) {
            connections.remove(remoteId);
          }
        }
        close();
      }
    }

    /** Initiates a call by sending the parameter to the remote server.
     * Note: this is not called from the Connection thread, but by other
     * threads.
     */
    public void sendParam(Call call) throws IOException {
      boolean error = true;
      try {
        calls.put(call.id, call);
        synchronized (out) {
          if (LOG.isDebugEnabled())
            LOG.debug(getName() + " sending #" + call.id);
          try {
            writingCall = call;
            DataOutputBuffer d = new DataOutputBuffer(); //for serializing the
                                                         //data to be written
            d.writeInt(call.id);
            call.param.write(d);
            byte[] data = d.getData();
            int dataLength = d.getLength();

            out.writeInt(dataLength);      //first put the data length
            out.write(data, 0, dataLength);//write the data
            out.flush();
          } finally {
            writingCall = null;
          }
        }
        error = false;
      } finally {
        if (error) {
          synchronized (connections) {
            if (connections.get(remoteId) == this)
              connections.remove(remoteId);
          }
          close();                                // close on error
        }
      }
    }  

    /** Close the connection. */
    public void close() {
      //socket may be null if the connection could not be established to the
      //server in question, and the culler asked us to close the connection
      if (socket == null) return;
      try {
        socket.close();                           // close socket
      } catch (IOException e) {}
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": closing");
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
    public void callComplete() {
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

  private class ConnectionCuller extends Thread {

    public static final int MIN_SLEEP_TIME = 1000;

    public void run() {

      LOG.debug(getName() + ": starting");

      while (running) {
        try {
          Thread.sleep(MIN_SLEEP_TIME);
        } catch (InterruptedException ie) {}

        synchronized (connections) {
          Iterator i = connections.values().iterator();
          while (i.hasNext()) {
            Connection c = (Connection)i.next();
            if (c.isIdle()) { 
              //We don't actually close the socket here (i.e., don't invoke
              //the close() method). We leave that work to the response receiver
              //thread. The reason for that is since we have taken a lock on the
              //connections table object, we don't want to slow down the entire
              //system if we happen to talk to a slow server.
              i.remove();
              synchronized (c) {
                c.setCloseConnection();
                c.notify();
              }
            }
          }
        }
      }
    }
  }

  /** Construct an IPC client whose values are of the given {@link Writable}
   * class. */
  public Client(Class valueClass, Configuration conf, 
      SocketFactory factory) {
    this.valueClass = valueClass;
    this.timeout = conf.getInt("ipc.client.timeout", 10000);
    this.maxIdleTime = conf.getInt("ipc.client.connection.maxidletime", 1000);
    this.maxRetries = conf.getInt("ipc.client.connect.max.retries", 10);
    this.tcpNoDelay = conf.getBoolean("ipc.client.tcpnodelay", false);
    this.conf = conf;
    this.socketFactory = factory;
    this.connectionCullerThread = new ConnectionCuller();
    connectionCullerThread.setDaemon(true);
    connectionCullerThread.setName(valueClass.getName() + " Connection Culler");
    LOG.debug(valueClass.getName() + 
              "Connection culler maxidletime= " + maxIdleTime + "ms");
    connectionCullerThread.start();
  }

  /**
   * Construct an IPC client with the default SocketFactory
   * @param valueClass
   * @param conf
   */
  public Client(Class<?> valueClass, Configuration conf) {
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
    
    if (running == false) {
      return;
    }
    running = false;

    connectionCullerThread.interrupt();
    try {
      connectionCullerThread.join();
    } catch(InterruptedException e) {}

    // close and wake up all connections
    synchronized (connections) {
      for (Connection conn : connections.values()) {
        synchronized (conn) {
          conn.setCloseConnection();
          conn.notifyAll();
        }
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

  /** Sets the timeout used for network i/o. */
  public void setTimeout(int timeout) { this.timeout = timeout; }

  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code>, returning the value.  Throws exceptions if there are
   * network problems or if the remote code threw an exception. */
  public Writable call(Writable param, InetSocketAddress address)
  throws InterruptedException, IOException {
      return call(param, address, null);
  }
  
  public Writable call(Writable param, InetSocketAddress addr, 
                       UserGroupInformation ticket)  
                       throws InterruptedException, IOException {
    Connection connection = getConnection(addr, ticket);
    Call call = new Call(param);
    synchronized (call) {
      connection.sendParam(call);                 // send the parameter
      long wait = timeout;
      do {
        call.wait(wait);                       // wait for the result
        wait = timeout - (System.currentTimeMillis() - call.lastActivity);
      } while (!call.done && wait > 0);

      if (call.error != null) {
        throw new RemoteException(call.errorClass, call.error);
      } else if (!call.done) {
        throw new SocketTimeoutException("timed out waiting for rpc response");
      } else {
        return call.value;
      }
    }
  }

  /** Makes a set of calls in parallel.  Each parameter is sent to the
   * corresponding address.  When all values are available, or have timed out
   * or errored, the collected results are returned in an array.  The array
   * contains nulls for calls that timed out or errored.  */
  public Writable[] call(Writable[] params, InetSocketAddress[] addresses)
    throws IOException {
    if (addresses.length == 0) return new Writable[0];

    ParallelResults results = new ParallelResults(params.length);
    synchronized (results) {
      for (int i = 0; i < params.length; i++) {
        ParallelCall call = new ParallelCall(params[i], results, i);
        try {
          Connection connection = getConnection(addresses[i], null);
          connection.sendParam(call);             // send each parameter
        } catch (IOException e) {
          LOG.info("Calling "+addresses[i]+" caught: " + 
                   StringUtils.stringifyException(e)); // log errors
          results.size--;                         //  wait for one fewer result
        }
      }
      try {
        results.wait(timeout);                    // wait for all results
      } catch (InterruptedException e) {}

      if (results.count == 0) {
        throw new IOException("no responses");
      } else {
        return results.values;
      }
    }
  }

  /** Get a connection from the pool, or create a new one and add it to the
   * pool.  Connections to a given host/port are reused. */
  private Connection getConnection(InetSocketAddress addr, 
                                   UserGroupInformation ticket)
                                   throws IOException {
    Connection connection;
    /* we could avoid this allocation for each RPC by having a  
     * connectionsId object and with set() method. We need to manage the
     * refs for keys in HashMap properly. For now its ok.
     */
    ConnectionId remoteId = new ConnectionId(addr, ticket);
    synchronized (connections) {
      connection = connections.get(remoteId);
      if (connection == null) {
        connection = new Connection(remoteId);
        connections.put(remoteId, connection);
        connection.start();
      }
      connection.incrementRef();
    }
    //we don't invoke the method below inside "synchronized (connections)"
    //block above. The reason for that is if the server happens to be slow,
    //it will take longer to establish a connection and that will slow the
    //entire system down.
    connection.setupIOstreams();
    return connection;
  }

  /**
   * This class holds the address and the user ticket. The client connections
   * to servers are uniquely identified by <remoteAddress, ticket>
   */
  private static class ConnectionId {
    InetSocketAddress address;
    UserGroupInformation ticket;
    
    ConnectionId(InetSocketAddress address, UserGroupInformation ticket) {
      this.address = address;
      this.ticket = ticket;
    }
    
    InetSocketAddress getAddress() {
      return address;
    }
    UserGroupInformation getTicket() {
      return ticket;
    }
    
    @Override
    public boolean equals(Object obj) {
     if (obj instanceof ConnectionId) {
       ConnectionId id = (ConnectionId) obj;
       return address.equals(id.address) && ticket == id.ticket;
       //Note : ticket is a ref comparision.
     }
     return false;
    }
    
    @Override
    public int hashCode() {
      return address.hashCode() ^ System.identityHashCode(ticket);
    }
  }  
}
