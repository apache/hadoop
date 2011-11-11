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
package org.apache.hadoop.hbase.catalog;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
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
  // TODO: This class needs a rethink.  The original intent was that it would be
  // the one-stop-shop for root and meta locations and that it would get this
  // info from reading and watching zk state.  The class was to be used by
  // servers when they needed to know of root and meta movement but also by
  // client-side (inside in HTable) so rather than figure root and meta
  // locations on fault, the client would instead get notifications out of zk.
  // 
  // But this original intent is frustrated by the fact that this class has to
  // read an hbase table, the -ROOT- table, to figure out the .META. region
  // location which means we depend on an HConnection.  HConnection will do
  // retrying but also, it has its own mechanism for finding root and meta
  // locations (and for 'verifying'; it tries the location and if it fails, does
  // new lookup, etc.).  So, at least for now, HConnection (or HTable) can't
  // have a CT since CT needs a HConnection (Even then, do want HT to have a CT?
  // For HT keep up a session with ZK?  Rather, shouldn't we do like asynchbase
  // where we'd open a connection to zk, read what we need then let the
  // connection go?).  The 'fix' is make it so both root and meta addresses
  // are wholey up in zk -- not in zk (root) -- and in an hbase table (meta).
  //
  // But even then, this class does 'verification' of the location and it does
  // this by making a call over an HConnection (which will do its own root
  // and meta lookups).  Isn't this verification 'useless' since when we
  // return, whatever is dependent on the result of this call then needs to
  // use HConnection; what we have verified may change in meantime (HConnection
  // uses the CT primitives, the root and meta trackers finding root locations).
  //
  // When meta is moved to zk, this class may make more sense.  In the
  // meantime, it does not cohere.  It should just watch meta and root and not
  // NOT do verification -- let that be out in HConnection since its going to
  // be done there ultimately anyways.
  //
  // This class has spread throughout the codebase.  It needs to be reigned in.
  // This class should be used server-side only, even if we move meta location
  // up into zk.  Currently its used over in the client package. Its used in
  // MetaReader and MetaEditor classes usually just to get the Configuration
  // its using (It does this indirectly by asking its HConnection for its
  // Configuration and even then this is just used to get an HConnection out on
  // the other end). I made https://issues.apache.org/jira/browse/HBASE-4495 for
  // doing CT fixup. St.Ack 09/30/2011.
  //
  private static final Log LOG = LogFactory.getLog(CatalogTracker.class);
  private final HConnection connection;
  private final ZooKeeperWatcher zookeeper;
  private final RootRegionTracker rootRegionTracker;
  private final MetaNodeTracker metaNodeTracker;
  private final AtomicBoolean metaAvailable = new AtomicBoolean(false);
  private boolean instantiatedzkw = false;

  /*
   * Do not clear this address once set.  Its needed when we do
   * server shutdown processing -- we need to know who had .META. last.  If you
   * want to know if the address is good, rely on {@link #metaAvailable} value.
   */
  private ServerName metaLocation;

  /*
   * Timeout waiting on root or meta to be set.
   */
  private final int defaultTimeout;

  private boolean stopped = false;

  static final byte [] ROOT_REGION_NAME =
    HRegionInfo.ROOT_REGIONINFO.getRegionName();
  static final byte [] META_REGION_NAME =
    HRegionInfo.FIRST_META_REGIONINFO.getRegionName();

  /**
   * Constructs a catalog tracker. Find current state of catalog tables.
   * Begin active tracking by executing {@link #start()} post construction. Does
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
   * Constructs the catalog tracker.  Find current state of catalog tables.
   * Begin active tracking by executing {@link #start()} post construction.
   * Does not timeout.
   * @param zk If zk is null, we'll create an instance (and shut it down
   * when {@link #stop()} is called) else we'll use what is passed.
   * @param conf
   * @param abortable If fatal exception we'll call abort on this.  May be null.
   * If it is we'll use the Connection associated with the passed
   * {@link Configuration} as our Abortable.
   * @throws IOException 
   */
  public CatalogTracker(final ZooKeeperWatcher zk, final Configuration conf,
      final Abortable abortable)
  throws IOException {
    this(zk, conf, abortable,
      conf.getInt("hbase.catalogtracker.default.timeout", 1000));
  }

  /**
   * Constructs the catalog tracker.  Find current state of catalog tables.
   * Begin active tracking by executing {@link #start()} post construction.
   * @param zk If zk is null, we'll create an instance (and shut it down
   * when {@link #stop()} is called) else we'll use what is passed.
   * @param conf
   * @param abortable If fatal exception we'll call abort on this.  May be null.
   * If it is we'll use the Connection associated with the passed
   * {@link Configuration} as our Abortable.
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
    this.connection = connection;
    if (abortable == null) {
      // A connection is abortable.
      abortable = this.connection;
    }
    if (zk == null) {
      // Create our own.  Set flag so we tear it down on stop.
      this.zookeeper =
        new ZooKeeperWatcher(conf, "catalogtracker-on-" + connection.toString(),
          abortable);
      instantiatedzkw = true;
    } else {
      this.zookeeper = zk;
    }
    this.rootRegionTracker = new RootRegionTracker(zookeeper, abortable);
    final CatalogTracker ct = this;
    // Override nodeDeleted so we get notified when meta node deleted
    this.metaNodeTracker = new MetaNodeTracker(zookeeper, abortable) {
      public void nodeDeleted(String path) {
        if (!path.equals(node)) return;
        ct.resetMetaLocation();
      }
    };
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
    LOG.debug("Starting catalog tracker " + this);
    this.rootRegionTracker.start();
    this.metaNodeTracker.start();
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
      if (this.instantiatedzkw) {
        this.zookeeper.close();
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
   * @return {@link ServerName} for server hosting <code>-ROOT-</code> or null
   * if none available
   * @throws InterruptedException 
   */
  public ServerName getRootLocation() throws InterruptedException {
    return this.rootRegionTracker.getRootRegionLocation();
  }

  /**
   * @return {@link ServerName} for server hosting <code>.META.</code> or null
   * if none available
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
   * @return {@link ServerName} for server hosting <code>-ROOT-</code> or null
   * if none available
   * @throws InterruptedException if interrupted while waiting
   * @throws NotAllMetaRegionsOnlineException if root not available before
   * timeout
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
   * @param timeout How long to wait on root location
   * @see #waitForRoot(long) for additional information
   * @return connection to server hosting root
   * @throws InterruptedException
   * @throws NotAllMetaRegionsOnlineException if timed out waiting
   * @throws IOException
   * @deprecated Use #getRootServerConnection(long)
   */
  public HRegionInterface waitForRootServerConnection(long timeout)
  throws InterruptedException, NotAllMetaRegionsOnlineException, IOException {
    return getRootServerConnection(timeout);
  }

  /**
   * Gets a connection to the server hosting root, as reported by ZooKeeper,
   * waiting up to the specified timeout for availability.
   * <p>WARNING: Does not retry.  Use an {@link HTable} instead.
   * @param timeout How long to wait on root location
   * @see #waitForRoot(long) for additional information
   * @return connection to server hosting root
   * @throws InterruptedException
   * @throws NotAllMetaRegionsOnlineException if timed out waiting
   * @throws IOException
   */
  HRegionInterface getRootServerConnection(long timeout)
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
   * @deprecated Use {@link #getRootServerConnection(long)}
   */
  public HRegionInterface waitForRootServerConnectionDefault()
  throws NotAllMetaRegionsOnlineException, IOException {
    try {
      return getRootServerConnection(this.defaultTimeout);
    } catch (InterruptedException e) {
      throw new NotAllMetaRegionsOnlineException("Interrupted");
    }
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
  private HRegionInterface getMetaServerConnection()
  throws IOException, InterruptedException {
    synchronized (metaAvailable) {
      if (metaAvailable.get()) {
        HRegionInterface current = getCachedConnection(this.metaLocation);
        // If we are to refresh, verify we have a good connection by making
        // an invocation on it.
        if (verifyRegionLocation(current, this.metaLocation, META_REGION_NAME)) {
          return current;
        }
        resetMetaLocation();
      }
      // We got here because there is no meta available or because whats
      // available is bad.

      // Now read the current .META. content from -ROOT-.  Note: This goes via
      // an HConnection.  It has its own way of figuring root and meta locations
      // which we have to wait on.
      ServerName newLocation =
        MetaReader.readRegionLocation(this, META_REGION_NAME);
      if (newLocation == null) return null;

      HRegionInterface newConnection = getCachedConnection(newLocation);
      if (verifyRegionLocation(newConnection, newLocation, META_REGION_NAME)) {
        setMetaLocation(newLocation);
        return newConnection;
      } else {
        if (LOG.isTraceEnabled()) {
          LOG.trace("New .META. server: " + newLocation + " isn't valid." +
            " Cached .META. server: " + this.metaLocation);
        }
      }
      return null;
    }
  }

  /**
   * Waits indefinitely for availability of <code>.META.</code>.  Used during
   * cluster startup.  Does not verify meta, just that something has been
   * set up in zk.
   * @see #waitForMeta(long)
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitForMeta() throws InterruptedException {
    while (!this.stopped) {
      try {
        if (waitForMeta(100) != null) break;
      } catch (NotAllMetaRegionsOnlineException e) {
        if (LOG.isTraceEnabled()) {
          LOG.info(".META. still not available, sleeping and retrying." +
          " Reason: " + e.getMessage());
        }
      } catch (IOException e) {
        LOG.info("Retrying", e);
      }
    }
  }

  /**
   * Gets the current location for <code>.META.</code> if available and waits
   * for up to the specified timeout if not immediately available.  Throws an
   * exception if timed out waiting.  This method differs from {@link #waitForMeta()}
   * in that it will go ahead and verify the location gotten from ZooKeeper and
   * -ROOT- region by trying to use returned connection.
   * @param timeout maximum time to wait for meta availability, in milliseconds
   * @return {@link ServerName} for server hosting <code>.META.</code> or null
   * if none available
   * @throws InterruptedException if interrupted while waiting
   * @throws IOException unexpected exception connecting to meta server
   * @throws NotAllMetaRegionsOnlineException if meta not available before
   * timeout
   */
  public ServerName waitForMeta(long timeout)
  throws InterruptedException, IOException, NotAllMetaRegionsOnlineException {
    long stop = System.currentTimeMillis() + timeout;
    long waitTime = Math.min(50, timeout);
    synchronized (metaAvailable) {
      while(!stopped && (timeout == 0 || System.currentTimeMillis() < stop)) {
        if (getMetaServerConnection() != null) {
          return metaLocation;
        }
        // perhaps -ROOT- region isn't available, let us wait a bit and retry.
        metaAvailable.wait(waitTime);
      }
      if (getMetaServerConnection() == null) {
        throw new NotAllMetaRegionsOnlineException("Timed out (" + timeout + "ms)");
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
   * @deprecated Does not retry; use an HTable instance instead.
   */
  public HRegionInterface waitForMetaServerConnection(long timeout)
  throws InterruptedException, NotAllMetaRegionsOnlineException, IOException {
    return getCachedConnection(waitForMeta(timeout));
  }

  /**
   * Gets a connection to the server hosting meta, as reported by ZooKeeper,
   * waiting up to the specified timeout for availability.
   * Used in tests.
   * @see #waitForMeta(long) for additional information
   * @return connection to server hosting meta
   * @throws NotAllMetaRegionsOnlineException if timed out or interrupted
   * @throws IOException
   * @deprecated Does not retry; use an HTable instance instead.
   */
  public HRegionInterface waitForMetaServerConnectionDefault()
  throws NotAllMetaRegionsOnlineException, IOException {
    try {
      return getCachedConnection(waitForMeta(defaultTimeout));
    } catch (InterruptedException e) {
      throw new NotAllMetaRegionsOnlineException("Interrupted");
    }
  }

  /**
   * Called when we figure current meta is off (called from zk callback).
   */
  public void resetMetaLocation() {
    LOG.debug("Current cached META location, " + metaLocation +
      ", is not valid, resetting");
    synchronized(this.metaAvailable) {
      this.metaAvailable.set(false);
      this.metaAvailable.notifyAll();
    }
  }

  /**
   * @param metaLocation
   */
  void setMetaLocation(final ServerName metaLocation) {
    LOG.debug("Set new cached META location: " + metaLocation);
    synchronized (this.metaAvailable) {
      this.metaLocation = metaLocation;
      this.metaAvailable.set(true);
      // no synchronization because these are private and already under lock
      this.metaAvailable.notifyAll();
    }
  }

  /**
   * @param sn ServerName to get a connection against.
   * @return The HRegionInterface we got when we connected to <code>sn</code>
   * May have come from cache, may not be good, may have been setup by this
   * invocation, or may be null.
   * @throws IOException
   */
  private HRegionInterface getCachedConnection(ServerName sn)
  throws IOException {
    if (sn == null) {
      return null;
    }
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
      LOG.debug("Timed out connecting to " + sn);
    } catch (NoRouteToHostException e) {
      LOG.debug("Connecting to " + sn, e);
    } catch (SocketException e) {
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

  /**
   * Verify we can connect to <code>hostingServer</code> and that its carrying
   * <code>regionName</code>.
   * @param hostingServer Interface to the server hosting <code>regionName</code>
   * @param serverName The servername that goes with the <code>metaServer</code>
   * Interface.  Used logging.
   * @param regionName The regionname we are interested in.
   * @return True if we were able to verify the region located at other side of
   * the Interface.
   * @throws IOException
   */
  // TODO: We should be able to get the ServerName from the HRegionInterface
  // rather than have to pass it in.  Its made awkward by the fact that the
  // HRI is likely a proxy against remote server so the getServerName needs
  // to be fixed to go to a local method or to a cache before we can do this.
  private boolean verifyRegionLocation(HRegionInterface hostingServer,
      final ServerName address, final byte [] regionName)
  throws IOException {
    if (hostingServer == null) {
      LOG.info("Passed hostingServer is null");
      return false;
    }
    Throwable t = null;
    try {
      // Try and get regioninfo from the hosting server.
      return hostingServer.getRegionInfo(regionName) != null;
    } catch (ConnectException e) {
      t = e;
    } catch (RemoteException e) {
      IOException ioe = e.unwrapRemoteException();
      t = ioe;
    } catch (IOException e) {
      Throwable cause = e.getCause();
      if (cause != null && cause instanceof EOFException) {
        t = cause;
      } else if (cause != null && cause.getMessage() != null
          && cause.getMessage().contains("Connection reset")) {
        t = cause;
      } else {
        t = e;
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
    } catch (ServerNotRunningYetException e) {
      // Pass -- remote server is not up so can't be carrying root
    }
    return (connection == null)? false:
      verifyRegionLocation(connection,
        this.rootRegionTracker.getRootRegionLocation(), ROOT_REGION_NAME);
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
    HRegionInterface connection = null;
    try {
      connection = waitForMetaServerConnection(timeout);
    } catch (NotAllMetaRegionsOnlineException e) {
      // Pass
    } catch (ServerNotRunningYetException e) {
      // Pass -- remote server is not up so can't be carrying .META.
    }
    return connection != null;
  }

  // Used by tests.
  MetaNodeTracker getMetaNodeTracker() {
    return this.metaNodeTracker;
  }

  public HConnection getConnection() {
    return this.connection;
  }
}
