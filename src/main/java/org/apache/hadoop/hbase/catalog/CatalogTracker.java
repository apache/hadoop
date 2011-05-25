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
package org.apache.hadoop.hbase.catalog;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MetaNodeTracker;
import org.apache.hadoop.hbase.zookeeper.RootRegionTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.ipc.RemoteException;

/**
 * Tracks the availability of the catalog tables <code>-ROOT-</code> and
 * <code>.META.</code>.
 * 
 * This class is "read-only" in that the locations of the catalog tables cannot
 * be explicitly set.  Instead, ZooKeeper is used to learn of the availability
 * and location of <code>-ROOT-</code>.  <code>-ROOT-</code> is used to learn of
 * the location of <code>.META.</code>  If not available in <code>-ROOT-</code>,
 * ZooKeeper is used to monitor for a new location of <code>.META.</code>.
 *
 * <p>Call {@link #start()} to start up operation.  Call {@link #stop()}} to
 * interrupt waits and close up shop.
 */
public class CatalogTracker {
  private static final Log LOG = LogFactory.getLog(CatalogTracker.class);
  private final Configuration conf;
  private final HConnection connection;
  private final ZooKeeperWatcher zookeeper;
  private final RootRegionTracker rootRegionTracker;
  private final MetaNodeTracker metaNodeTracker;
  private final AtomicBoolean metaAvailable = new AtomicBoolean(false);
  /**
   * Do not clear this address once set.  Its needed when we do
   * server shutdown processing -- we need to know who had .META. last.  If you
   * want to know if the address is good, rely on {@link #metaAvailable} value.
   */
  private ServerName metaLocation;
  private final int defaultTimeout;
  private boolean stopped = false;

  public static final byte [] ROOT_REGION =
    HRegionInfo.ROOT_REGIONINFO.getRegionName();
  public static final byte [] META_REGION =
    HRegionInfo.FIRST_META_REGIONINFO.getRegionName();

  /**
   * Constructs a catalog tracker. Find current state of catalog tables and
   * begin active tracking by executing {@link #start()} post construction. Does
   * not timeout.
   *
   * @param conf
   *          the {@link Configuration} from which a {@link HConnection} will be
   *          obtained; if problem, this connections
   *          {@link HConnection#abort(String, Throwable)} will be called.
   * @throws IOException
   */
  public CatalogTracker(final Configuration conf) throws IOException {
    this(null, conf, null);
  }

  /**
   * Constructs the catalog tracker.  Find current state of catalog tables and
   * begin active tracking by executing {@link #start()} post construction.
   * Does not timeout.
   * @param zk
   * @param connection server connection
   * @param abortable if fatal exception
   * @throws IOException 
   */
  public CatalogTracker(final ZooKeeperWatcher zk, final Configuration conf,
      final Abortable abortable)
  throws IOException {
    this(zk, conf, abortable, 0);
  }

  /**
   * Constructs the catalog tracker.  Find current state of catalog tables and
   * begin active tracking by executing {@link #start()} post construction.
   * @param zk
   * @param connection server connection
   * @param abortable if fatal exception
   * @param defaultTimeout Timeout to use.  Pass zero for no timeout
   * ({@link Object#wait(long)} when passed a <code>0</code> waits for ever).
   * @throws IOException 
   */
  public CatalogTracker(final ZooKeeperWatcher zk, final Configuration conf,
      Abortable abortable, final int defaultTimeout)
  throws IOException {
    this(zk, conf, HConnectionManager.getConnection(conf), abortable, defaultTimeout);
  }

  CatalogTracker(final ZooKeeperWatcher zk, final Configuration conf,
      HConnection connection, Abortable abortable, final int defaultTimeout)
  throws IOException {
    this.conf = conf;
    this.connection = connection;
    this.zookeeper = (zk == null) ? this.connection.getZooKeeperWatcher() : zk;
    if (abortable == null) {
      abortable = this.connection;
    }
    this.rootRegionTracker = new RootRegionTracker(zookeeper, abortable);
    this.metaNodeTracker = new MetaNodeTracker(zookeeper, this, abortable);
    this.defaultTimeout = defaultTimeout;
  }

  /**
   * Starts the catalog tracker.
   * Determines current availability of catalog tables and ensures all further
   * transitions of either region are tracked.
   * @throws IOException
   * @throws InterruptedException 
   */
  public void start() throws IOException, InterruptedException {
    this.rootRegionTracker.start();
    this.metaNodeTracker.start();
    LOG.debug("Starting catalog tracker " + this);
  }

  /**
   * Stop working.
   * Interrupts any ongoing waits.
   */
  public void stop() {
    if (!this.stopped) {
      LOG.debug("Stopping catalog tracker " + this);
      this.stopped = true;
      this.rootRegionTracker.stop();
      this.metaNodeTracker.stop();
      try {
        if (this.connection != null) {
          this.connection.close();
        }
      } catch (IOException e) {
        // Although the {@link Closeable} interface throws an {@link
        // IOException}, in reality, the implementation would never do that.
        LOG.error("Attempt to close catalog tracker's connection failed.", e);
      }
      // Call this and it will interrupt any ongoing waits on meta.
      synchronized (this.metaAvailable) {
        this.metaAvailable.notifyAll();
      }
    }
  }

  /**
   * Gets the current location for <code>-ROOT-</code> or null if location is
   * not currently available.
   * @return server name
   * @throws InterruptedException 
   */
  public ServerName getRootLocation() throws InterruptedException {
    return this.rootRegionTracker.getRootRegionLocation();
  }

  /**
   * @return Location of server hosting meta region formatted as per
   * {@link ServerName}, or null if none available
   */
  public ServerName getMetaLocation() {
    return this.metaLocation;
  }

  /**
   * Waits indefinitely for availability of <code>-ROOT-</code>.  Used during
   * cluster startup.
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForRoot()
  throws InterruptedException {
    this.rootRegionTracker.blockUntilAvailable();
  }

  /**
   * Gets the current location for <code>-ROOT-</code> if available and waits
   * for up to the specified timeout if not immediately available.  Returns null
   * if the timeout elapses before root is available.
   * @param timeout maximum time to wait for root availability, in milliseconds
   * @return Location of server hosting root region,
   * or null if none available
   * @throws InterruptedException if interrupted while waiting
   * @throws NotAllMetaRegionsOnlineException if root not available before
   *                                          timeout
   */
  ServerName waitForRoot(final long timeout)
  throws InterruptedException, NotAllMetaRegionsOnlineException {
    ServerName sn = rootRegionTracker.waitRootRegionLocation(timeout);
    if (sn == null) {
      throw new NotAllMetaRegionsOnlineException("Timed out; " + timeout + "ms");
    }
    return sn;
  }

  /**
   * Gets a connection to the server hosting root, as reported by ZooKeeper,
   * waiting up to the specified timeout for availability.
   * @see #waitForRoot(long) for additional information
   * @return connection to server hosting root
   * @throws InterruptedException
   * @throws NotAllMetaRegionsOnlineException if timed out waiting
   * @throws IOException
   */
  public HRegionInterface waitForRootServerConnection(long timeout)
  throws InterruptedException, NotAllMetaRegionsOnlineException, IOException {
    return getCachedConnection(waitForRoot(timeout));
  }

  /**
   * Gets a connection to the server hosting root, as reported by ZooKeeper,
   * waiting for the default timeout specified on instantiation.
   * @see #waitForRoot(long) for additional information
   * @return connection to server hosting root
   * @throws NotAllMetaRegionsOnlineException if timed out waiting
   * @throws IOException
   */
  public HRegionInterface waitForRootServerConnectionDefault()
  throws NotAllMetaRegionsOnlineException, IOException {
    try {
      return getCachedConnection(waitForRoot(defaultTimeout));
    } catch (InterruptedException e) {
      throw new NotAllMetaRegionsOnlineException("Interrupted");
    }
  }

  /**
   * Gets a connection to the server hosting root, as reported by ZooKeeper,
   * if available.  Returns null if no location is immediately available.
   * @return connection to server hosting root, null if not available
   * @throws IOException
   * @throws InterruptedException 
   */
  private HRegionInterface getRootServerConnection()
  throws IOException, InterruptedException {
    ServerName sn = this.rootRegionTracker.getRootRegionLocation();
    if (sn == null) {
      return null;
    }
    return getCachedConnection(sn);
  }

  /**
   * Gets a connection to the server currently hosting <code>.META.</code> or
   * null if location is not currently available.
   * <p>
   * If a location is known, a connection to the cached location is returned.
   * If refresh is true, the cached connection is verified first before
   * returning.  If the connection is not valid, it is reset and rechecked.
   * <p>
   * If no location for meta is currently known, method checks ROOT for a new
   * location, verifies META is currently there, and returns a cached connection
   * to the server hosting META.
   *
   * @return connection to server hosting meta, null if location not available
   * @throws IOException
   * @throws InterruptedException 
   */
  private HRegionInterface getMetaServerConnection(boolean refresh)
  throws IOException, InterruptedException {
    synchronized (metaAvailable) {
      if (metaAvailable.get()) {
        HRegionInterface current = getCachedConnection(metaLocation);
        if (!refresh) {
          return current;
        }
        if (verifyRegionLocation(current, this.metaLocation, META_REGION)) {
          return current;
        }
        resetMetaLocation();
      }
      HRegionInterface rootConnection = getRootServerConnection();
      if (rootConnection == null) {
        return null;
      }
      ServerName newLocation = MetaReader.readMetaLocation(rootConnection);
      if (newLocation == null) {
        return null;
      }
      HRegionInterface newConnection = getCachedConnection(newLocation);
      if (verifyRegionLocation(newConnection, this.metaLocation, META_REGION)) {
        setMetaLocation(newLocation);
        return newConnection;
      }
      return null;
    }
  }

  /**
   * Waits indefinitely for availability of <code>.META.</code>.  Used during
   * cluster startup.
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForMeta() throws InterruptedException {
    synchronized (metaAvailable) {
      while (!stopped && !metaAvailable.get()) {
        metaAvailable.wait();
      }
    }
  }

  /**
   * Gets the current location for <code>.META.</code> if available and waits
   * for up to the specified timeout if not immediately available.  Throws an
   * exception if timed out waiting.  This method differs from {@link #waitForMeta()}
   * in that it will go ahead and verify the location gotten from ZooKeeper by
   * trying to use returned connection.
   * @param timeout maximum time to wait for meta availability, in milliseconds
   * @return location of meta
   * @throws InterruptedException if interrupted while waiting
   * @throws IOException unexpected exception connecting to meta server
   * @throws NotAllMetaRegionsOnlineException if meta not available before
   *                                          timeout
   */
  public ServerName waitForMeta(long timeout)
  throws InterruptedException, IOException, NotAllMetaRegionsOnlineException {
    long stop = System.currentTimeMillis() + timeout;
    synchronized (metaAvailable) {
      while(!stopped && !metaAvailable.get() &&
          (timeout == 0 || System.currentTimeMillis() < stop)) {
        if (getMetaServerConnection(true) != null) {
          return metaLocation;
        }
        metaAvailable.wait(timeout == 0 ? 50 : timeout);
      }
      if (getMetaServerConnection(true) == null) {
        throw new NotAllMetaRegionsOnlineException(
            "Timed out (" + timeout + "ms)");
      }
      return metaLocation;
    }
  }

  /**
   * Gets a connection to the server hosting meta, as reported by ZooKeeper,
   * waiting up to the specified timeout for availability.
   * @see #waitForMeta(long) for additional information
   * @return connection to server hosting meta
   * @throws InterruptedException
   * @throws NotAllMetaRegionsOnlineException if timed out waiting
   * @throws IOException
   */
  public HRegionInterface waitForMetaServerConnection(long timeout)
  throws InterruptedException, NotAllMetaRegionsOnlineException, IOException {
    return getCachedConnection(waitForMeta(timeout));
  }

  /**
   * Gets a connection to the server hosting meta, as reported by ZooKeeper,
   * waiting up to the specified timeout for availability.
   * @see #waitForMeta(long) for additional information
   * @return connection to server hosting meta
   * @throws NotAllMetaRegionsOnlineException if timed out or interrupted
   * @throws IOException
   */
  public HRegionInterface waitForMetaServerConnectionDefault()
  throws NotAllMetaRegionsOnlineException, IOException {
    try {
      return getCachedConnection(waitForMeta(defaultTimeout));
    } catch (InterruptedException e) {
      throw new NotAllMetaRegionsOnlineException("Interrupted");
    }
  }

  private void resetMetaLocation() {
    LOG.info("Current cached META location is not valid, resetting");
    this.metaAvailable.set(false);
  }

  private void setMetaLocation(final ServerName metaLocation) {
    metaAvailable.set(true);
    this.metaLocation = metaLocation;
    // no synchronization because these are private and already under lock
    this.metaAvailable.notifyAll();
  }

  private HRegionInterface getCachedConnection(ServerName sn)
  throws IOException {
    HRegionInterface protocol = null;
    try {
      protocol = connection.getHRegionConnection(sn.getHostname(), sn.getPort());
    } catch (RetriesExhaustedException e) {
      if (e.getCause() != null && e.getCause() instanceof ConnectException) {
        // Catch this; presume it means the cached connection has gone bad.
      } else {
        throw e;
      }
    } catch (SocketTimeoutException e) {
      // Return 'protocol' == null.
      LOG.debug("Timed out connecting to " + sn);
    } catch (SocketException e) {
      // Return 'protocol' == null.
      LOG.debug("Exception connecting to " + sn);
    } catch (IOException ioe) {
      Throwable cause = ioe.getCause();
      if (cause != null && cause instanceof EOFException) {
        // Catch. Other end disconnected us.
      } else if (cause != null && cause.getMessage() != null &&
        cause.getMessage().toLowerCase().contains("connection reset")) {
        // Catch. Connection reset.
      } else {
        throw ioe;
      }
      
    }
    return protocol;
  }

  private boolean verifyRegionLocation(HRegionInterface metaServer,
      final ServerName address,
      byte [] regionName)
  throws IOException {
    if (metaServer == null) {
      LOG.info("Passed metaserver is null");
      return false;
    }
    Throwable t = null;
    try {
      return metaServer.getRegionInfo(regionName) != null;
    } catch (ConnectException e) {
      t = e;
    } catch (RemoteException e) {
      IOException ioe = e.unwrapRemoteException();
      if (ioe instanceof NotServingRegionException) {
        t = ioe;
      } else {
        throw e;
      }
    } catch (IOException e) {
      Throwable cause = e.getCause();
      if (cause != null && cause instanceof EOFException) {
        t = cause;
      } else if (cause != null && cause.getMessage() != null
          && cause.getMessage().contains("Connection reset")) {
        t = cause;
      } else {
        throw e;
      }
    }
    LOG.info("Failed verification of " + Bytes.toStringBinary(regionName) +
      " at address=" + address + "; " + t);
    return false;
  }

  /**
   * Verify <code>-ROOT-</code> is deployed and accessible.
   * @param timeout How long to wait on zk for root address (passed through to
   * the internal call to {@link #waitForRootServerConnection(long)}.
   * @return True if the <code>-ROOT-</code> location is healthy.
   * @throws IOException
   * @throws InterruptedException 
   */
  public boolean verifyRootRegionLocation(final long timeout)
  throws InterruptedException, IOException {
    HRegionInterface connection = null;
    try {
      connection = waitForRootServerConnection(timeout);
    } catch (NotAllMetaRegionsOnlineException e) {
      // Pass
    } catch (org.apache.hadoop.hbase.ipc.ServerNotRunningException e) {
      // Pass -- remote server is not up so can't be carrying root
    } catch (IOException e) {
      // Unexpected exception
      throw e;
    }
    return (connection == null)? false:
      verifyRegionLocation(connection,
        this.rootRegionTracker.getRootRegionLocation(),
        HRegionInfo.ROOT_REGIONINFO.getRegionName());
  }

  /**
   * Verify <code>.META.</code> is deployed and accessible.
   * @param timeout How long to wait on zk for <code>.META.</code> address
   * (passed through to the internal call to {@link #waitForMetaServerConnection(long)}.
   * @return True if the <code>.META.</code> location is healthy.
   * @throws IOException Some unexpected IOE.
   * @throws InterruptedException
   */
  public boolean verifyMetaRegionLocation(final long timeout)
  throws InterruptedException, IOException {
    return getMetaServerConnection(true) != null;
  }

  MetaNodeTracker getMetaNodeTracker() {
    return this.metaNodeTracker;
  }

  public HConnection getConnection() {
    return this.connection;
  }
}
