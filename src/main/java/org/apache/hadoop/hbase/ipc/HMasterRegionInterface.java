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

import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;

/**
 * HRegionServers interact with the HMasterRegionInterface to report on local
 * goings-on and to obtain data-handling instructions from the HMaster.
 * <p>Changes here need to be reflected in HbaseObjectWritable HbaseRPC#Invoker.
 *
 * <p>NOTE: if you change the interface, you must change the RPC version
 * number in HBaseRPCProtocolVersion
 *
 */
public interface HMasterRegionInterface extends VersionedProtocol {
  /**
   * This Interfaces' version. Version changes when the Interface changes.
   */
  // All HBase Interfaces used derive from HBaseRPCProtocolVersion.  It
  // maintained a single global version number on all HBase Interfaces.  This
  // meant all HBase RPC was broke though only one of the three RPC Interfaces
  // had changed.  This has since been undone.
  public static final long VERSION = 28L;

  /**
   * Called when a region server first starts
   * @param info server info
   * @param serverCurrentTime The current time of the region server in ms
   * @throws IOException e
   * @return Configuration for the regionserver to use: e.g. filesystem,
   * hbase rootdir, etc.
   */
  public MapWritable regionServerStartup(HServerInfo info,
    long serverCurrentTime) throws IOException;

  /**
   * Called to renew lease, tell master what the region server is doing and to
   * receive new instructions from the master
   *
   * @param info server's address and start code
   * @param msgs things the region server wants to tell the master
   * @param mostLoadedRegions Array of HRegionInfos that should contain the
   * reporting server's most loaded regions. These are candidates for being
   * rebalanced.
   * @return instructions from the master to the region server
   * @throws IOException e
   */
  public HMsg[] regionServerReport(HServerInfo info, HMsg msgs[],
    HRegionInfo mostLoadedRegions[])
  throws IOException;
}