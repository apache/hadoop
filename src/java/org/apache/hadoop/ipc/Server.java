/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.io.EOFException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.StringWriter;
import java.io.PrintWriter;

import java.net.Socket;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import java.util.LinkedList;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.apache.hadoop.util.LogFormatter;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.UTF8;

/** An abstract IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @author Doug Cutting
 * @see Client
 */
public abstract class Server {
  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.hadoop.ipc.Server");

  private static final ThreadLocal SERVER = new ThreadLocal();

  /** Returns the server instance called under or null.  May be called under
   * {@link #call(Writable)} implementations, and under {@link Writable}
   * methods of paramters and return values.  Permits applications to access
   * the server context.*/
  public static Server get() {
    return (Server)SERVER.get();
  }

  private int port;                               // port we listen on
  private int handlerCount;                       // number of handler threads
  private int maxQueuedCalls;                     // max number of queued calls
  private Class paramClass;                       // class of call parameters
  private Configuration conf;

  private int timeout;

  private boolean running = true;                 // true while server runs
  private LinkedList callQueue = new LinkedList(); // queued calls
  private Object callDequeued = new Object();     // used by wait/notify

  /** A call queued for handling. */
  private static class Call {
    private int id;                               // the client's call id
    private Writable param;                       // the parameter passed
    private Connection connection;                // connection to client

    public Call(int id, Writable param, Connection connection) {
      this.id = id;
      this.param = param;
      this.connection = connection;
    }
  }

  /** Listens on the socket, starting new connection threads. */
  private class Listener extends Thread {
    private ServerSocket socket;

    public Listener() throws IOException {
      this.socket = new ServerSocket(port);
      socket.setSoTimeout(timeout);
      this.setDaemon(true);
      this.setName("Server listener on port " + port);
    }

    public void run() {
      LOG.info(getName() + ": starting");
      while (running) {
        try {
          new Connection(socket.accept()).start(); // start a new connection
        } catch (SocketTimeoutException e) {      // ignore timeouts
        } catch (Exception e) {                   // log all other exceptions
          LOG.log(Level.INFO, getName() + " caught: " + e, e);
        }
      }
      try {
        socket.close();
      } catch (IOException e) {
        LOG.info(getName() + ": e=" + e);
      }
      LOG.info(getName() + ": exiting");
    }
  }

  /** Reads calls from a connection and queues them for handling. */
  private class Connection extends Thread {
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    public Connection(Socket socket) throws IOException {
      this.socket = socket;
      socket.setSoTimeout(timeout);
      this.in = new DataInputStream
        (new BufferedInputStream(socket.getInputStream()));
      this.out = new DataOutputStream
        (new BufferedOutputStream(socket.getOutputStream()));
      this.setDaemon(true);
      this.setName("Server connection on port " + port + " from "
                   + socket.getInetAddress().getHostAddress());
    }

    public void run() {
      LOG.info(getName() + ": starting");
      SERVER.set(Server.this);
      try {
        while (running) {
          int id;
          try {
            id = in.readInt();                    // try to read an id
          } catch (SocketTimeoutException e) {
            continue;
          }
        
          if (LOG.isLoggable(Level.FINE))
            LOG.fine(getName() + " got #" + id);
        
          Writable param = makeParam();           // read param
          param.readFields(in);        
        
          Call call = new Call(id, param, this);
        
          synchronized (callQueue) {
            callQueue.addLast(call);              // queue the call
            callQueue.notify();                   // wake up a waiting handler
          }
        
          while (running && callQueue.size() >= maxQueuedCalls) {
            synchronized (callDequeued) {         // queue is full
              callDequeued.wait(timeout);         // wait for a dequeue
            }
          }
        }
      } catch (EOFException eof) {
          // This is what happens on linux when the other side shuts down
      } catch (SocketException eof) {
          // This is what happens on Win32 when the other side shuts down
      } catch (Exception e) {
        LOG.log(Level.INFO, getName() + " caught: " + e, e);
      } finally {
        try {
          socket.close();
        } catch (IOException e) {}
        LOG.info(getName() + ": exiting");
      }
    }

  }

  /** Handles queued calls . */
  private class Handler extends Thread {
    public Handler(int instanceNumber) {
      this.setDaemon(true);
      this.setName("Server handler "+ instanceNumber + " on " + port);
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

          if (LOG.isLoggable(Level.FINE))
            LOG.fine(getName() + ": has #" + call.id + " from " +
                     call.connection.socket.getInetAddress().getHostAddress());
          
          String error = null;
          Writable value = null;
          try {
            value = call(call.param);             // make the call
          } catch (IOException e) {
            LOG.log(Level.INFO, getName() + " call error: " + e, e);
            error = getStackTrace(e);
          } catch (Exception e) {
            LOG.log(Level.INFO, getName() + " call error: " + e, e);
            error = getStackTrace(e);
          }
            
          DataOutputStream out = call.connection.out;
          synchronized (out) {
            out.writeInt(call.id);                // write call id
            out.writeBoolean(error!=null);        // write error flag
            if (error != null)
              value = new UTF8(error);
            value.write(out);                     // write value
            out.flush();
          }

        } catch (Exception e) {
          LOG.log(Level.INFO, getName() + " caught: " + e, e);
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
  
  /** Constructs a server listening on the named port.  Parameters passed must
   * be of the named class.  The <code>handlerCount</handlerCount> determines
   * the number of handler threads that will be used to process calls.
   */
  protected Server(int port, Class paramClass, int handlerCount, Configuration conf) {
    this.conf = conf;
    this.port = port;
    this.paramClass = paramClass;
    this.handlerCount = handlerCount;
    this.maxQueuedCalls = handlerCount;
    this.timeout = conf.getInt("ipc.client.timeout",10000); 
  }

  /** Sets the timeout used for network i/o. */
  public void setTimeout(int timeout) { this.timeout = timeout; }

  /** Starts the service.  Must be called before any calls will be handled. */
  public synchronized void start() throws IOException {
    Listener listener = new Listener();
    listener.start();
    
    for (int i = 0; i < handlerCount; i++) {
      Handler handler = new Handler(i);
      handler.start();
    }
  }

  /** Stops the service.  No new calls will be handled after this is called.  All
   * subthreads will likely be finished after this returns.
   */
  public synchronized void stop() {
    LOG.info("Stopping server on " + port);
    running = false;
    try {
      Thread.sleep(timeout);     //  inexactly wait for pending requests to finish
    } catch (InterruptedException e) {}
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

  
  private Writable makeParam() {
    Writable param;                               // construct param
    try {
      param = (Writable)paramClass.newInstance();
      if (param instanceof Configurable) {
        ((Configurable)param).setConf(conf);
      }
    } catch (InstantiationException e) {
      throw new RuntimeException(e.toString());
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e.toString());
    }
    return param;
  }

}
