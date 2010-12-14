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
package org.apache.hadoop.hbase.master.handler;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.DeadServer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.AssignmentManager.RegionState;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.zookeeper.KeeperException;

/**
 * Process server shutdown.
 * Server-to-handle must be already in the deadservers lists.  See
 * {@link ServerManager#expireServer(HServerInfo)}.
 */
public class ServerShutdownHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(ServerShutdownHandler.class);
  private final HServerInfo hsi;
  private final Server server;
  private final MasterServices services;
  private final DeadServer deadServers;

  public ServerShutdownHandler(final Server server, final MasterServices services,
      final DeadServer deadServers, final HServerInfo hsi) {
    this(server, services, deadServers, hsi, EventType.M_SERVER_SHUTDOWN);
  }

  ServerShutdownHandler(final Server server, final MasterServices services,
      final DeadServer deadServers, final HServerInfo hsi, EventType type) {
    super(server, type);
    this.hsi = hsi;
    this.server = server;
    this.services = services;
    this.deadServers = deadServers;
    if (!this.deadServers.contains(hsi.getServerName())) {
      LOG.warn(hsi.getServerName() + " is NOT in deadservers; it should be!");
    }
  }

  /**
   * @return True if the server we are processing was carrying <code>-ROOT-</code>
   */
  boolean isCarryingRoot() {
    return false;
  }

  /**
   * @return True if the server we are processing was carrying <code>.META.</code>
   */
  boolean isCarryingMeta() {
    return false;
  }

  @Override
  public void process() throws IOException {
    final String serverName = this.hsi.getServerName();

    LOG.info("Splitting logs for " + serverName);
    this.services.getMasterFileSystem().splitLog(serverName);

    // Clean out anything in regions in transition.  Being conservative and
    // doing after log splitting.  Could do some states before -- OPENING?
    // OFFLINE? -- and then others after like CLOSING that depend on log
    // splitting.
    List<RegionState> regionsInTransition =
      this.services.getAssignmentManager().processServerShutdown(this.hsi);

    // Assign root and meta if we were carrying them.
    if (isCarryingRoot()) { // -ROOT-
      try {
        this.services.getAssignmentManager().assignRoot();
      } catch (KeeperException e) {
        this.server.abort("In server shutdown processing, assigning root", e);
        throw new IOException("Aborting", e);
      }
    }

    // Carrying meta?
    if (isCarryingMeta()) this.services.getAssignmentManager().assignMeta();

    // Wait on meta to come online; we need it to progress.
    // TODO: Best way to hold strictly here?  We should build this retry logic
    //       into the MetaReader operations themselves.
    NavigableMap<HRegionInfo, Result> hris = null;
    while (!this.server.isStopped()) {
      try {
        this.server.getCatalogTracker().waitForMeta();
        hris = MetaReader.getServerUserRegions(this.server.getCatalogTracker(),
            this.hsi);
        break;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted", e);
      } catch (IOException ioe) {
        LOG.info("Received exception accessing META during server shutdown of " +
            serverName + ", retrying META read");
      }
    }

    // Skip regions that were in transition unless CLOSING or PENDING_CLOSE
    for (RegionState rit : regionsInTransition) {
      if (!rit.isClosing() && !rit.isPendingClose()) {
        hris.remove(rit.getRegion());
      }
    }

    LOG.info("Reassigning " + hris.size() + " region(s) that " + serverName
        + " was carrying (skipping " + regionsInTransition.size() +
        " regions(s) that are already in transition)");

    // Iterate regions that were on this server and assign them
    for (Map.Entry<HRegionInfo, Result> e: hris.entrySet()) {
      if (processDeadRegion(e.getKey(), e.getValue(),
          this.services.getAssignmentManager(),
          this.server.getCatalogTracker())) {
        this.services.getAssignmentManager().assign(e.getKey(), true);
      }
    }
    this.deadServers.finish(serverName);
    LOG.info("Finished processing of shutdown of " + serverName);
  }

  /**
   * Process a dead region from a dead RS.  Checks if the region is disabled
   * or if the region has a partially completed split.
   * @param hri
   * @param result
   * @param assignmentManager
   * @param catalogTracker
   * @return Returns true if specified region should be assigned, false if not.
   * @throws IOException
   */
  public static boolean processDeadRegion(HRegionInfo hri, Result result,
      AssignmentManager assignmentManager, CatalogTracker catalogTracker)
  throws IOException {
    // If table is not disabled but the region is offlined,
    boolean disabled = assignmentManager.getZKTable().isDisabledTable(
        hri.getTableDesc().getNameAsString());
    if (disabled) return false;
    if (hri.isOffline() && hri.isSplit()) {
      fixupDaughters(result, assignmentManager, catalogTracker);
      return false;
    }
    return true;
  }

  /**
   * Check that daughter regions are up in .META. and if not, add them.
   * @param hris All regions for this server in meta.
   * @param result The contents of the parent row in .META.
   * @throws IOException
   */
  static void fixupDaughters(final Result result,
      final AssignmentManager assignmentManager,
      final CatalogTracker catalogTracker) throws IOException {
    fixupDaughter(result, HConstants.SPLITA_QUALIFIER, assignmentManager,
        catalogTracker);
    fixupDaughter(result, HConstants.SPLITB_QUALIFIER, assignmentManager,
        catalogTracker);
  }

  /**
   * Check individual daughter is up in .META.; fixup if its not.
   * @param result The contents of the parent row in .META.
   * @param qualifier Which daughter to check for.
   * @throws IOException
   */
  static void fixupDaughter(final Result result, final byte [] qualifier,
      final AssignmentManager assignmentManager,
      final CatalogTracker catalogTracker)
  throws IOException {
    byte [] bytes = result.getValue(HConstants.CATALOG_FAMILY, qualifier);
    if (bytes == null || bytes.length <= 0) return;
    HRegionInfo hri = Writables.getHRegionInfo(bytes);
    Pair<HRegionInfo, HServerAddress> pair =
      MetaReader.getRegion(catalogTracker, hri.getRegionName());
    if (pair == null || pair.getFirst() == null) {
      LOG.info("Fixup; missing daughter " + hri.getEncodedName());
      MetaEditor.addDaughter(catalogTracker, hri, null);
      assignmentManager.assign(hri, true);
    }
  }
}
