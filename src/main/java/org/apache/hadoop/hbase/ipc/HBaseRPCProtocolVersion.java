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

package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * There is one version id for all the RPC interfaces. If any interface
 * is changed, the versionID must be changed here.
 */
public interface HBaseRPCProtocolVersion extends VersionedProtocol {
  /**
   * Interface version.
   *
   * HMasterInterface version history:
   * <ul>
   * <li>Version was incremented to 2 when we brought the hadoop RPC local to
   * hbase HADOOP-2495</li>
   * <li>Version was incremented to 3 when we changed the RPC to send codes
   * instead of actual class names (HADOOP-2519).</li>
   * <li>Version 4 when we moved to all byte arrays (HBASE-42).</li>
   * <li>Version 5  HBASE-576.</li>
   * <li>Version 6  modifyTable.</li>
   * </ul>
   * <p>HMasterRegionInterface version history:
   * <ul>
   * <li>Version 2 was when the regionServerStartup was changed to return a
   * MapWritable instead of a HbaseMapWritable as part of HBASE-82 changes.</li>
   * <li>Version 3 was when HMsg was refactored so it could carry optional
   * messages (HBASE-504).</li>
   * <li>HBASE-576 we moved this to 4.</li>
   * </ul>
   * <p>HRegionInterface version history:
   * <ul>
   * <li>Upped to 5 when we added scanner caching</li>
   * <li>HBASE-576, we moved this to 6.</li>
   * </ul>
   * <p>TransactionalRegionInterface version history:
   * <ul>
   * <li>Moved to 2 for hbase-576.</li>
   * </ul>
   * <p>Unified RPC version number history:
   * <ul>
   * <li>Version 10: initial version (had to be &gt all other RPC versions</li>
   * <li>Version 11: Changed getClosestRowBefore signature.</li>
   * <li>Version 12: HServerLoad extensions (HBASE-1018).</li>
   * <li>Version 13: HBASE-847</li>
   * <li>Version 14: HBASE-900</li>
   * <li>Version 15: HRegionInterface.exists</li>
   * <li>Version 16: Removed HMasterRegionInterface.getRootRegionLocation and
   * HMasterInterface.findRootRegion. We use ZooKeeper to store root region
   * location instead.</li>
   * <li>Version 17: Added incrementColumnValue.</li>
   * <li>Version 18: HBASE-1302.</li>
   * <li>Version 19: Added getClusterStatus().</li>
   * <li>Version 20: Backed Transaction HBase out of HBase core.</li>
   * <li>Version 21: HBASE-1665.</li>
   * <li>Version 22: HBASE-2209. Added List support to RPC</li>
   * <li>Version 23: HBASE-2066, multi-put.</li>
   * <li>Version 24: HBASE-2473, create table with regions.</li>
   * <li>Version 25: Added openRegion and Stoppable/Abortable to API.</li>
   * <li>Version 26: New master and Increment, 0.90 version bump.</li>
   * </ul>
   */
  public static final long versionID = 25L; // Setting it to 25 temporarily to see if hudson passes.  #1608 hudson failed because of version mismatch 25 vs 26.
}
