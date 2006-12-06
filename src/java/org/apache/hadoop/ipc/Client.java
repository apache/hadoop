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
import java.io.OutputStream;

import java.util.Hashtable;
import java.util.Iterator;

import org.apache.commons.logging.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.dfs.FSConstants;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/** A client for an IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @author Doug Cutting
 * @see Server
 */
public class Client {
  /** Should the client send the header on the connection? */
  private static final boolean SEND_HEADER = false;
  private static final byte CURRENT_VERSION = 0;
  
  public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.ipc.Client");
  private Hashtable connections = new Hashtable();

  private Class valueClass;                       // class of call values
  private int timeout ;// timeout for calls
  private int counter;                            // counter for call ids
  private boolean running = true;                 // true while client runs
  private Configuration conf;
  private int maxIdleTime; //connections will be culled if it was idle for 
                           //maxIdleTime msecs
  private int maxRetries; //the max. no. of retries for socket connections

  /** A call waiting for a value. */
  private class Call {
    int id;                                       // call id
    Writable param;                               // parameter
    Writable value;                               // value, null if error
    RemoteException error;                        // error, null if value
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
    public synchronized void setResult(Writable value, RemoteException error) {
      this.value = value;
      this.error = error;
      this.done = true;
    }
    
  }

  /** Thread that reads responses and notifies callers.  Each connection owns a
   * socket connected to a remote address.  Calls are multiplexed through this
   * socket: responses may be delivered out of order. */
  private class Connection extends Thread {
    private InetSocketAddress address;            // address of server
    private Socket socket = null;                 // connected socket
    private DataInputStream in;                   
    private DataOutputStream out;
    private Hashtable calls = new Hashtable();    // currently active calls
    private Call readingCall;
    private Call writingCall;
    private int inUse = 0;
    private long lastActivity = 0;
    private boolean shouldCloseConnection = false;

    public Connection(InetSocketAddress address) throws IOException {
      if (address.isUnresolved()) {
         throw new UnknownHostException("unknown host: " + address.getHostName());
      }
      this.address = address;
      this.setName("IPC Client connection to " + address.toString());
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
          this.socket = new Socket();
          this.socket.connect(address, FSConstants.READ_TIMEOUT);
          break;
        } catch (IOException ie) { //SocketTimeoutException is also caught 
          if (failures == maxRetries) {
            //reset inUse so that the culler gets a chance to throw this
            //connection object out of the table. We don't want to increment
            //inUse to infinity (everytime getConnection is called inUse is
            //incremented)!
            inUse = 0;
            throw ie;
          }
          failures++;
          LOG.info("Retrying connect to server: " + address + 
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
         (new FilterInputStream(socket.getInputStream()) {
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
         (new FilterOutputStream(socket.getOutputStream()) {
             public void write(byte[] buf, int o, int len) throws IOException {
               out.write(buf, o, len);
               if (writingCall != null) {
                 writingCall.touch();
               }
             }
           }));
      if (SEND_HEADER) {
        out.write(Server.HEADER.array());
        out.write(CURRENT_VERSION);
      }
      notify();
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
      return address;
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

          Call call = (Call)calls.remove(new Integer(id));
          boolean isError = in.readBoolean();     // read if error
          if (isError) {
            RemoteException ex = 
              new RemoteException(WritableUtils.readString(in),
                                  WritableUtils.readString(in));
            call.setResult(null, ex);
          } else {
            Writable value = (Writable)ReflectionUtils.newInstance(valueClass, conf);
            try {
              readingCall = call;
              value.readFields(in);                 // read value
            } finally {
              readingCall = null;
            }
            call.setResult(value, null);
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
          if (connections.get(address) == this) {
            connections.remove(address);
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
        calls.put(new Integer(call.id), call);
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
            if (connections.get(address) == this)
              connections.remove(address);
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

      LOG.info(getName() + ": starting");

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
  public Client(Class valueClass, Configuration conf) {
    this.valueClass = valueClass;
    this.timeout = conf.getInt("ipc.client.timeout",10000);
    this.maxIdleTime = conf.getInt("ipc.client.connection.maxidletime",1000);
    this.maxRetries = conf.getInt("ipc.client.connect.max.retries", 10);
    this.conf = conf;

    Thread t = new ConnectionCuller();
    t.setDaemon(true);
    t.setName(valueClass.getName() + " Connection Culler");
    LOG.info(valueClass.getName() + 
             "Connection culler maxidletime= " + maxIdleTime + "ms");
    t.start();
  }
 
  /** Stop all threads related to this client.  No further calls may be made
   * using this client. */
  public void stop() {
    LOG.info("Stopping client");
    running = false;
  }

  /** Sets the timeout used for network i/o. */
  public void setTimeout(int timeout) { this.timeout = timeout; }

  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code>, returning the value.  Throws exceptions if there are
   * network problems or if the remote code threw an exception. */
  public Writable call(Writable param, InetSocketAddress address)
    throws IOException {
    Connection connection = getConnection(address);
    Call call = new Call(param);
    synchronized (call) {
      connection.sendParam(call);                 // send the parameter
      long wait = timeout;
      do {
        try {
          call.wait(wait);                       // wait for the result
        } catch (InterruptedException e) {}
        wait = timeout - (System.currentTimeMillis() - call.lastActivity);
      } while (!call.done && wait > 0);

      if (call.error != null) {
        throw call.error;
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
          Connection connection = getConnection(addresses[i]);
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
  private Connection getConnection(InetSocketAddress address)
    throws IOException {
    Connection connection;
    synchronized (connections) {
      connection = (Connection)connections.get(address);
      if (connection == null) {
        connection = new Connection(address);
        connections.put(address, connection);
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

}
