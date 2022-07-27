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

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.util.ExitUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Listens on the socket. Creates jobs for the handler threads
 */
public class NioListener extends Thread
    implements Listener<ServerSocketChannel> {
  private final Server server;
  private ServerSocketChannel acceptChannel = null; //the accept channel
  private Selector selector = null; //the selector that we use for the server
  private Reader[] readers = null;
  private int currentReader = 0;
  private final InetSocketAddress address; //the address we bind at
  private final int listenPort; //the port we bind at
  private final int backlogLength;
  private final boolean reuseAddr;
  private boolean isOnAuxiliaryPort;

  public NioListener(Server server, int port) throws IOException {
    this.server = server;

    backlogLength = server.conf.getInt(
        CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_KEY,
        CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT);
    reuseAddr = server.conf.getBoolean(
        CommonConfigurationKeysPublic.IPC_SERVER_REUSEADDR_KEY,
        CommonConfigurationKeysPublic.IPC_SERVER_REUSEADDR_DEFAULT);

    address = new InetSocketAddress(server.bindAddress, port);
    // Create a new server socket and set to non blocking mode
    acceptChannel = ServerSocketChannel.open();
    acceptChannel.configureBlocking(false);
    acceptChannel.setOption(StandardSocketOptions.SO_REUSEADDR, reuseAddr);

    // Bind the server socket to the local host and port
    Server.bind(acceptChannel.socket(), address, backlogLength, server.conf,
        server.portRangeConfig);
    //Could be an ephemeral port
    this.listenPort = acceptChannel.socket().getLocalPort();
    Thread.currentThread().setName("Listener at " +
        server.bindAddress + "/" + this.listenPort);
    // create a selector;
    selector = Selector.open();
    readers = new Reader[server.readThreads];
    for (int i = 0; i < server.readThreads; i++) {
      Reader reader = new Reader(
          "Socket Reader #" + (i + 1) + " for port " + port);
      readers[i] = reader;
      reader.start();
    }

    // Register accepts on the server socket with the selector.
    registerAcceptChannel(acceptChannel);
    this.setName("IPC Server listener on " + port);
    this.setDaemon(true);
    this.isOnAuxiliaryPort = false;
  }

  void setIsAuxiliary() {
    this.isOnAuxiliaryPort = true;
  }


  @Override
  public void listen(InetSocketAddress addr) throws IOException {
    // Bind the server socket to the local host and port
    ServerSocketChannel acceptChannel = ServerSocketChannel.open();
    acceptChannel.configureBlocking(false);
    Server.bind(acceptChannel.socket(), addr, backlogLength);
    registerAcceptChannel(acceptChannel);
  }

  @Override
  public void registerAcceptChannel(ServerSocketChannel channel)
      throws IOException {
    channel.register(selector, SelectionKey.OP_ACCEPT);
  }

  @Override
  public void closeAcceptChannels() throws IOException {
    if (selector.isOpen()) {
      for (SelectionKey key : selector.keys()) {
        if (key.isValid()) {
          key.channel().close();
        }
      }
    }
  }

  private class Reader extends Thread {
    final private BlockingQueue<NioConnection> pendingConnections;
    private final Selector readSelector;

    Reader(String name) throws IOException {
      super(name);

      this.pendingConnections =
          new LinkedBlockingQueue<>(server.readerPendingConnectionQueue);
      this.readSelector = Selector.open();
    }

    @Override
    public void run() {
      Server.LOG.info("Starting " + Thread.currentThread().getName());
      try {
        doRunLoop();
      } finally {
        try {
          readSelector.close();
        } catch (IOException ioe) {
          Server.LOG.error("Error closing read selector in " +
              Thread.currentThread().getName(), ioe);
        }
      }
    }

    private synchronized void doRunLoop() {
      while (server.running) {
        SelectionKey key = null;
        try {
          // consume as many connections as currently queued to avoid
          // unbridled acceptance of connections that starves the select
          int size = pendingConnections.size();
          for (int i = size; i > 0; i--) {
            NioConnection conn = pendingConnections.take();
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
              Server.LOG.info(Thread.currentThread().getName() +
                  ": connection aborted from " + key.attachment());
            }
            key = null;
          }
        } catch (InterruptedException e) {
          if (server.running) {                      // unexpected -- log it
            Server.LOG.info(
                Thread.currentThread().getName() + " unexpectedly interrupted",
                e);
          }
        } catch (IOException ex) {
          Server.LOG.error("Error in Reader", ex);
        } catch (Throwable re) {
          Server.LOG.error("Bug in read selector!", re);
          ExitUtil.terminate(1, "Bug in read selector!");
        }
      }
    }

    /**
     * Updating the readSelector while it's being used is not thread-safe,
     * so the connection must be queued.  The reader will drain the queue
     * and update its readSelector before performing the next select
     */
    public void addConnection(NioConnection conn)
        throws InterruptedException {
      pendingConnections.put(conn);
      readSelector.wakeup();
    }

    void shutdown() {
      assert !server.running;
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
    Server.LOG.info(Thread.currentThread().getName() + ": starting");
    Server.SERVER.set(server);
    server.connectionManager.startIdleScan();
    while (server.running) {
      SelectionKey key = null;
      try {
        getSelector().select();
        Iterator<SelectionKey> iter = getSelector().selectedKeys().iterator();
        while (iter.hasNext()) {
          key = iter.next();
          iter.remove();
          try {
            if (key.isValid()) {
              if (key.isAcceptable()) {
                doAccept(key);
              }
            }
          } catch (IOException e) {
          }
          key = null;
        }
      } catch (OutOfMemoryError e) {
        // we can run out of memory if we have too many threads
        // log the event and sleep for a minute and give
        // some thread(s) a chance to finish
        Server.LOG.warn("Out of Memory in server select", e);
        closeCurrentConnection(key, e);
        server.connectionManager.closeIdle(true);
        try {
          Thread.sleep(60000);
        } catch (Exception ie) {
        }
      } catch (Exception e) {
        closeCurrentConnection(key, e);
      }
    }
    Server.LOG.info("Stopping " + Thread.currentThread().getName());

    synchronized (this) {
      try {
        closeAcceptChannels();
        selector.close();
      } catch (IOException e) {
      }

      selector = null;
      acceptChannel = null;

      // close all connections
      server.connectionManager.stopIdleScan();
      server.connectionManager.closeAll();
    }
  }

  private void closeCurrentConnection(SelectionKey key, Throwable e) {
    if (key != null) {
      Connection c = (Connection) key.attachment();
      if (c != null) {
        server.closeConnection(c);
        c = null;
      }
    }
  }

  @Override
  public InetSocketAddress getAddress() {
    return (InetSocketAddress) acceptChannel.socket().getLocalSocketAddress();
  }

  void doAccept(SelectionKey key)
      throws InterruptedException, IOException, OutOfMemoryError {
    ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
    SocketChannel channel;
    while ((channel = serverSocketChannel.accept()) != null) {

      channel.configureBlocking(false);
      channel.socket().setTcpNoDelay(server.tcpNoDelay);
      channel.socket().setKeepAlive(true);

      Reader reader = getReader();
      // If the connectionManager can't take it, it closes the connection.
      // TODO: How do we create a NioConnection Object without making the class
      //       static ?
      NioConnection c = new NioConnection(server, channel);
      if (!server.connectionManager.register(c)) {
        continue;
      }
      key.attach(c);  // so closeCurrentConnection can get the object
      reader.addConnection(c);
    }
  }

  void doRead(SelectionKey key) throws InterruptedException {
    Connection c = (Connection) key.attachment();
    if (c != null) {
      c.doRead(key.channel());
    }
  }

  @Override
  public synchronized void doStop() {
    if (selector != null) {
      selector.wakeup();
      Thread.yield();
    }
    if (acceptChannel != null) {
      try {
        closeAcceptChannels();
      } catch (IOException e) {
        Server.LOG.info(Thread.currentThread().getName() +
            ":Exception in closing listener socket. " + e);
      }
    }
    for (Reader r : readers) {
      r.shutdown();
    }
  }

  synchronized Selector getSelector() {
    return selector;
  }

  // The method that will return the next reader to work with
  // Simplistic implementation of round robin for now
  Reader getReader() {
    currentReader = (currentReader + 1) % readers.length;
    return readers[currentReader];
  }
}
