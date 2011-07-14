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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.ipc.HBaseRpcMetrics;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.zookeeper.KeeperException;
import java.util.Set;

/**
 * Services provided by {@link HRegionServer}
 */
public interface RegionServerServices extends OnlineRegions {
  /**
   * @return True if this regionserver is stopping.
   */
  public boolean isStopping();

  /** @return the HLog */
  public HLog getWAL();

  /**
   * @return Implementation of {@link CompactionRequestor} or null.
   */
  public CompactionRequestor getCompactionRequester();

  /**
   * @return Implementation of {@link FlushRequester} or null.
   */
  public FlushRequester getFlushRequester();

  /**
   * @return the RegionServerAccounting for this Region Server
   */
  public RegionServerAccounting getRegionServerAccounting();

  /**
   * Tasks to perform after region open to complete deploy of region on
   * regionserver
   * @param r Region to open.
   * @param ct Instance of {@link CatalogTracker}
   * @param daughter True if this is daughter of a split
   * @throws KeeperException
   * @throws IOException
   */
  public void postOpenDeployTasks(final HRegion r, final CatalogTracker ct,
      final boolean daughter)
  throws KeeperException, IOException;

  /**
   * Returns a reference to the RPC server metrics.
   */
  public HBaseRpcMetrics getRpcMetrics();

  /**
   * Get the regions that are currently being opened or closed in the RS
   * @return set of regions in transition in this RS
   */
  public Set<byte[]> getRegionsInTransitionInRS();
}