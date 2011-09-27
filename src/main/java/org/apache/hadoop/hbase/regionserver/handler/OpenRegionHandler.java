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
package org.apache.hadoop.hbase.regionserver.handler;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.zookeeper.KeeperException;

/**
 * Handles opening of a region on a region server.
 * <p>
 * This is executed after receiving an OPEN RPC from the master or client.
 */
public class OpenRegionHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(OpenRegionHandler.class);

  private final RegionServerServices rsServices;

  private final HRegionInfo regionInfo;
  private final HTableDescriptor htd;

  // We get version of our znode at start of open process and monitor it across
  // the total open. We'll fail the open if someone hijacks our znode; we can
  // tell this has happened if version is not as expected.
  private volatile int version = -1;
  //version of the offline node that was set by the master
  private volatile int versionOfOfflineNode = -1;

  public OpenRegionHandler(final Server server,
      final RegionServerServices rsServices, HRegionInfo regionInfo,
      HTableDescriptor htd) {
    this(server, rsServices, regionInfo, htd, EventType.M_RS_OPEN_REGION, -1);
  }
  public OpenRegionHandler(final Server server,
      final RegionServerServices rsServices, HRegionInfo regionInfo,
      HTableDescriptor htd, int versionOfOfflineNode) {
    this(server, rsServices, regionInfo, htd, EventType.M_RS_OPEN_REGION,
        versionOfOfflineNode);
  }

  protected OpenRegionHandler(final Server server,
      final RegionServerServices rsServices, final HRegionInfo regionInfo,
      final HTableDescriptor htd, EventType eventType,
      final int versionOfOfflineNode) {
    super(server, eventType);
    this.rsServices = rsServices;
    this.regionInfo = regionInfo;
    this.htd = htd;
    this.versionOfOfflineNode = versionOfOfflineNode;
  }

  public HRegionInfo getRegionInfo() {
    return regionInfo;
  }

  @Override
  public void process() throws IOException {
    try {
      final String name = regionInfo.getRegionNameAsString();
      if (this.server.isStopped() || this.rsServices.isStopping()) {
        return;
      }
      final String encodedName = regionInfo.getEncodedName();

      // Check that this region is not already online
      HRegion region = this.rsServices.getFromOnlineRegions(encodedName);

      // If fails, just return.  Someone stole the region from under us.
      // Calling transitionZookeeperOfflineToOpening initalizes this.version.
      if (!transitionZookeeperOfflineToOpening(encodedName,
          versionOfOfflineNode)) {
        LOG.warn("Region was hijacked? It no longer exists, encodedName=" +
          encodedName);
        return;
      }

      // Open region.  After a successful open, failures in subsequent
      // processing needs to do a close as part of cleanup.
      region = openRegion();
      if (region == null) {
        tryTransitionToFailedOpen(regionInfo);
        return;
      }

      boolean failed = true;
      if (tickleOpening("post_region_open")) {
        if (updateMeta(region)) failed = false;
      }
      if (failed || this.server.isStopped() ||
          this.rsServices.isStopping()) {
        cleanupFailedOpen(region);
        tryTransitionToFailedOpen(regionInfo);
        return;
      }

      if (!transitionToOpened(region)) {
        // If we fail to transition to opened, it's because of one of two cases:
        //    (a) we lost our ZK lease
        // OR (b) someone else opened the region before us
        // In either case, we don't need to transition to FAILED_OPEN state.
        // In case (a), the Master will process us as a dead server. In case
        // (b) the region is already being handled elsewhere anyway.
        cleanupFailedOpen(region);
        return;
      }

      // Done!  Successful region open
      LOG.debug("Opened " + name + " on server:" +
        this.server.getServerName());
    } finally {
      this.rsServices.getRegionsInTransitionInRS().
          remove(this.regionInfo.getEncodedNameAsBytes());
    }
  }

  /**
   * Update ZK, ROOT or META.  This can take a while if for example the
   * .META. is not available -- if server hosting .META. crashed and we are
   * waiting on it to come back -- so run in a thread and keep updating znode
   * state meantime so master doesn't timeout our region-in-transition.
   * Caller must cleanup region if this fails.
   */
  boolean updateMeta(final HRegion r) {
    if (this.server.isStopped() || this.rsServices.isStopping()) {
      return false;
    }
    // Object we do wait/notify on.  Make it boolean.  If set, we're done.
    // Else, wait.
    final AtomicBoolean signaller = new AtomicBoolean(false);
    PostOpenDeployTasksThread t = new PostOpenDeployTasksThread(r,
      this.server, this.rsServices, signaller);
    t.start();
    int assignmentTimeout = this.server.getConfiguration().
      getInt("hbase.master.assignment.timeoutmonitor.period", 10000);
    // Total timeout for meta edit.  If we fail adding the edit then close out
    // the region and let it be assigned elsewhere.
    long timeout = assignmentTimeout * 10;
    long now = System.currentTimeMillis();
    long endTime = now + timeout;
    // Let our period at which we update OPENING state to be be 1/3rd of the
    // regions-in-transition timeout period.
    long period = Math.max(1, assignmentTimeout/ 3);
    long lastUpdate = now;
    boolean tickleOpening = true;
    while (!signaller.get() && t.isAlive() && !this.server.isStopped() &&
        !this.rsServices.isStopping() && (endTime > now)) {
      long elapsed = now - lastUpdate;
      if (elapsed > period) {
        // Only tickle OPENING if postOpenDeployTasks is taking some time.
        lastUpdate = now;
        tickleOpening = tickleOpening("post_open_deploy");
      }
      synchronized (signaller) {
        try {
          signaller.wait(period);
        } catch (InterruptedException e) {
          // Go to the loop check.
        }
      }
      now = System.currentTimeMillis();
    }
    // Is thread still alive?  We may have left above loop because server is
    // stopping or we timed out the edit.  Is so, interrupt it.
    if (t.isAlive()) {
      if (!signaller.get()) {
        // Thread still running; interrupt
        LOG.debug("Interrupting thread " + t);
        t.interrupt();
      }
      try {
        t.join();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted joining " +
          r.getRegionInfo().getRegionNameAsString(), ie);
        Thread.currentThread().interrupt();
      }
    }

    // Was there an exception opening the region?  This should trigger on
    // InterruptedException too.  If so, we failed.  Even if tickle opening fails
    // then it is a failure.
    return ((!Thread.interrupted() && t.getException() == null) && tickleOpening);
  }

  /**
   * Thread to run region post open tasks.  Call {@link #getException()} after
   * the thread finishes to check for exceptions running
   * {@link RegionServerServices#postOpenDeployTasks(HRegion, org.apache.hadoop.hbase.catalog.CatalogTracker, boolean)}.
   */
  static class PostOpenDeployTasksThread extends Thread {
    private Exception exception = null;
    private final Server server;
    private final RegionServerServices services;
    private final HRegion region;
    private final AtomicBoolean signaller;

    PostOpenDeployTasksThread(final HRegion region, final Server server,
        final RegionServerServices services, final AtomicBoolean signaller) {
      super("PostOpenDeployTasks:" + region.getRegionInfo().getEncodedName());
      this.setDaemon(true);
      this.server = server;
      this.services = services;
      this.region = region;
      this.signaller = signaller;
    }

    public void run() {
      try {
        this.services.postOpenDeployTasks(this.region,
          this.server.getCatalogTracker(), false);
      } catch (Exception e) {
        LOG.warn("Exception running postOpenDeployTasks; region=" +
          this.region.getRegionInfo().getEncodedName(), e);
        this.exception = e;
      }
      // We're done.  Set flag then wake up anyone waiting on thread to complete.
      this.signaller.set(true);
      synchronized (this.signaller) {
        this.signaller.notify();
      }
    }

    /**
     * @return Null or the run exception; call this method after thread is done.
     */
    Exception getException() {
      return this.exception;
    }
  }


  /**
   * @param r Region we're working on.
   * @return Transition znode to OPENED state.
   * @throws IOException 
   */
  private boolean transitionToOpened(final HRegion r) throws IOException {
    boolean result = false;
    HRegionInfo hri = r.getRegionInfo();
    final String name = hri.getRegionNameAsString();
    // Finally, Transition ZK node to OPENED
    try {
      if (ZKAssign.transitionNodeOpened(this.server.getZooKeeper(), hri,
          this.server.getServerName(), this.version) == -1) {
        LOG.warn("Completed the OPEN of region " + name +
          " but when transitioning from " +
          " OPENING to OPENED got a version mismatch, someone else clashed " +
          "so now unassigning -- closing region on server: " +
          this.server.getServerName());
      } else {
        LOG.debug("region transitioned to opened in zookeeper: " +
          r.getRegionInfo() + ", server: " + this.server.getServerName());
        result = true;
      }
    } catch (KeeperException e) {
      LOG.error("Failed transitioning node " + name +
        " from OPENING to OPENED -- closing region", e);
    }
    return result;
  }
  
  /**
   * @param  Region we're working on.
   * This is not guaranteed to succeed, we just do our best.
   * @return Transition znode to CLOSED state.
   */
  private boolean tryTransitionToFailedOpen(final HRegionInfo hri) {
    boolean result = false;
    final String name = hri.getRegionNameAsString();
    try {
      LOG.info("Opening of region " + hri + " failed, marking as FAILED_OPEN in ZK");
      if (ZKAssign.transitionNode(
          this.server.getZooKeeper(), hri,
          this.server.getServerName(),
          EventType.RS_ZK_REGION_OPENING,
          EventType.RS_ZK_REGION_FAILED_OPEN,
          this.version) == -1) {
        LOG.warn("Unable to mark region " + hri + " as FAILED_OPEN. " +
            "It's likely that the master already timed out this open " +
            "attempt, and thus another RS already has the region.");
      } else {
        result = true;
      }
    } catch (KeeperException e) {
      LOG.error("Failed transitioning node " + name +
        " from OPENING to FAILED_OPEN", e);
    }
    return result;
  }

  /**
   * @return Instance of HRegion if successful open else null.
   */
  HRegion openRegion() {
    HRegion region = null;
    try {
      // Instantiate the region.  This also periodically tickles our zk OPENING
      // state so master doesn't timeout this region in transition.
      region = HRegion.openHRegion(this.regionInfo, this.htd,
          this.rsServices.getWAL(), this.server.getConfiguration(),
          this.rsServices,
        new CancelableProgressable() {
          public boolean progress() {
            // We may lose the znode ownership during the open.  Currently its
            // too hard interrupting ongoing region open.  Just let it complete
            // and check we still have the znode after region open.
            return tickleOpening("open_region_progress");
          }
        });
    } catch (Throwable t) {
      // We failed open. Our caller will see the 'null' return value
      // and transition the node back to FAILED_OPEN. If that fails,
      // we rely on the Timeout Monitor in the master to reassign.
      LOG.error("Failed open of region=" +
        this.regionInfo.getRegionNameAsString(), t);
    }
    return region;
  }

  private void cleanupFailedOpen(final HRegion region) throws IOException {
    if (region != null) region.close();
    this.rsServices.removeFromOnlineRegions(regionInfo.getEncodedName());
  }

  /**
   * Transition ZK node from OFFLINE to OPENING.
   * @param encodedName Name of the znode file (Region encodedName is the znode
   * name).
   * @param versionOfOfflineNode - version Of OfflineNode that needs to be compared
   * before changing the node's state from OFFLINE 
   * @return True if successful transition.
   */
  boolean transitionZookeeperOfflineToOpening(final String encodedName,
      int versionOfOfflineNode) {
    // TODO: should also handle transition from CLOSED?
    try {
      // Initialize the znode version.
      this.version = ZKAssign.transitionNode(server.getZooKeeper(), regionInfo,
          server.getServerName(), EventType.M_ZK_REGION_OFFLINE,
          EventType.RS_ZK_REGION_OPENING, versionOfOfflineNode);
    } catch (KeeperException e) {
      LOG.error("Error transition from OFFLINE to OPENING for region=" +
        encodedName, e);
    }
    boolean b = isGoodVersion();
    if (!b) {
      LOG.warn("Failed transition from OFFLINE to OPENING for region=" +
        encodedName);
    }
    return b;
  }

  /**
   * Update our OPENING state in zookeeper.
   * Do this so master doesn't timeout this region-in-transition.
   * @param context Some context to add to logs if failure
   * @return True if successful transition.
   */
  boolean tickleOpening(final String context) {
    // If previous checks failed... do not try again.
    if (!isGoodVersion()) return false;
    String encodedName = this.regionInfo.getEncodedName();
    try {
      this.version =
        ZKAssign.retransitionNodeOpening(server.getZooKeeper(),
          this.regionInfo, this.server.getServerName(), this.version);
    } catch (KeeperException e) {
      server.abort("Exception refreshing OPENING; region=" + encodedName +
        ", context=" + context, e);
      this.version = -1;
    }
    boolean b = isGoodVersion();
    if (!b) {
      LOG.warn("Failed refreshing OPENING; region=" + encodedName +
        ", context=" + context);
    }
    return b;
  }

  private boolean isGoodVersion() {
    return this.version != -1;
  }
}
