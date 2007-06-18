/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;

/**
 * Clients interact with the HMasterInterface to gain access to meta-level
 * HBase functionality, like finding an HRegionServer and creating/destroying
 * tables.
 */
public interface HMasterInterface extends VersionedProtocol {
  public static final long versionID = 1L; // initial version

  //////////////////////////////////////////////////////////////////////////////
  // Check to see if master is available
  //////////////////////////////////////////////////////////////////////////////

  public boolean isMasterRunning();
  
  //////////////////////////////////////////////////////////////////////////////
  // Admin tools would use these cmds
  //////////////////////////////////////////////////////////////////////////////

  public void createTable(HTableDescriptor desc) throws IOException;
  public void deleteTable(Text tableName) throws IOException;
  
  public void addColumn(Text tableName, HColumnDescriptor column) throws IOException;
  public void deleteColumn(Text tableName, Text columnName) throws IOException;
  
  public void enableTable(Text tableName) throws IOException;
  public void disableTable(Text tableName) throws IOException;
  
  /**
   * Shutdown an HBase cluster.
   */
  public void shutdown() throws IOException;

  //////////////////////////////////////////////////////////////////////////////
  // These are the method calls of last resort when trying to find an HRegion
  //////////////////////////////////////////////////////////////////////////////

  public HServerAddress findRootRegion();
}