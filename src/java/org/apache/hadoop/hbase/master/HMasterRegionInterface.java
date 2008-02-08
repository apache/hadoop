/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * HRegionServers interact with the HMasterRegionInterface to report on local 
 * goings-on and to obtain data-handling instructions from the HMaster.
 */
public interface HMasterRegionInterface extends VersionedProtocol {
  /** Interface version number */
  public static final long versionID = 1L;
  
  /**
   * Called when a region server first starts
   * @param info
   * @throws IOException
   * @return Configuration for the regionserver to use: e.g. filesystem,
   * hbase rootdir, etc.
   */
  public HbaseMapWritable regionServerStartup(HServerInfo info) throws IOException;
  
  /**
   * Called to renew lease, tell master what the region server is doing and to
   * receive new instructions from the master
   * 
   * @param info server's address and start code
   * @param msgs things the region server wants to tell the master
   * @return instructions from the master to the region server
   * @throws IOException
   */
  public HMsg[] regionServerReport(HServerInfo info, HMsg msgs[])
  throws IOException;
}
