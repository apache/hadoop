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

import java.io.IOException;

import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.hbase.ipc.VersionedProtocol;

/**
 * The Master publishes this Interface for RegionServers to register themselves
 * on.
 */
public interface HMasterRegionInterface extends VersionedProtocol {
  /**
   * This Interfaces' version. Version changes when the Interface changes.
   */
  // All HBase Interfaces used derive from HBaseRPCProtocolVersion.  It
  // maintained a single global version number on all HBase Interfaces.  This
  // meant all HBase RPC was broke though only one of the three RPC Interfaces
  // had changed.  This has since been undone.
  public static final long VERSION = 29L;

  /**
   * Called when a region server first starts.
   * @param port Port number this regionserver is up on.
   * @param serverStartcode This servers' startcode.
   * @param serverCurrentTime The current time of the region server in ms
   * @throws IOException e
   * @return Configuration for the regionserver to use: e.g. filesystem,
   * hbase rootdir, the hostname to use creating the RegionServer ServerName,
   * etc.
   */
  public MapWritable regionServerStartup(final int port,
    final long serverStartcode, final long serverCurrentTime)
  throws IOException;

  /**
   * @param sn {@link ServerName#getBytes()}
   * @param hsl Server load.
   * @throws IOException
   */
  public void regionServerReport(byte [] sn, HServerLoad hsl)
  throws IOException;
  
  /**
   * Called by a region server to report a fatal error that is causing
   * it to abort.
   * @param sn {@link ServerName#getBytes()}
   * @param errorMessage informative text to expose in the master logs and UI
   */
  public void reportRSFatalError(byte [] sn, String errorMessage);
}
