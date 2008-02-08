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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;

/**
 * Clients interact with the HMasterInterface to gain access to meta-level
 * HBase functionality, like finding an HRegionServer and creating/destroying
 * tables.
 */
public interface HMasterInterface extends VersionedProtocol {
  /**
   * Interface version.
   * Version was incremented to 2 when we brought the hadoop RPC local to hbase
   * -- HADOOP-2495 and then to 3 when we changed the RPC to send codes instead
   * of actual class names (HADOOP-2519).
   */
  public static final long versionID = 3L;

  /** @return true if master is available */
  public boolean isMasterRunning();
  
  // Admin tools would use these cmds

  /**
   * Creates a new table
   * @param desc table descriptor
   * @throws IOException
   */
  public void createTable(HTableDescriptor desc) throws IOException;

  /**
   * Deletes a table
   * @param tableName
   * @throws IOException
   */
  public void deleteTable(Text tableName) throws IOException;
  
  /**
   * Adds a column to the specified table
   * @param tableName
   * @param column column descriptor
   * @throws IOException
   */
  public void addColumn(Text tableName, HColumnDescriptor column) throws IOException;

  /**
   * Modifies an existing column on the specified table
   * @param tableName
   * @param columnName name of the column to edit
   * @param descriptor new column descriptor
   * @throws IOException
   */
  public void modifyColumn(Text tableName, Text columnName, 
    HColumnDescriptor descriptor) 
  throws IOException;


  /**
   * Deletes a column from the specified table
   * @param tableName
   * @param columnName
   * @throws IOException
   */
  public void deleteColumn(Text tableName, Text columnName) throws IOException;
  
  /**
   * Puts the table on-line (only needed if table has been previously taken offline)
   * @param tableName
   * @throws IOException
   */
  public void enableTable(Text tableName) throws IOException;
  
  /**
   * Take table offline
   * 
   * @param tableName
   * @throws IOException
   */
  public void disableTable(Text tableName) throws IOException;
  
  /**
   * Shutdown an HBase cluster.
   * @throws IOException
   */
  public void shutdown() throws IOException;

  /**
   * Get the location of the root region
   * @return address of server that serves the root region
   */
  public HServerAddress findRootRegion();
}
