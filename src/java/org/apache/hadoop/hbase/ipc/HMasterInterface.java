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
package org.apache.hadoop.hbase.ipc;

import java.io.IOException;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.io.Writable;

/**
 * Clients interact with the HMasterInterface to gain access to meta-level
 * HBase functionality, like finding an HRegionServer and creating/destroying
 * tables.
 * 
 * <p>NOTE: if you change the interface, you must change the RPC version
 * number in HBaseRPCProtocolVersion
 * 
 */
public interface HMasterInterface extends HBaseRPCProtocolVersion {

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
  public void deleteTable(final byte [] tableName) throws IOException;
  
  /**
   * Adds a column to the specified table
   * @param tableName
   * @param column column descriptor
   * @throws IOException
   */
  public void addColumn(final byte [] tableName, HColumnDescriptor column)
  throws IOException;

  /**
   * Modifies an existing column on the specified table
   * @param tableName
   * @param columnName name of the column to edit
   * @param descriptor new column descriptor
   * @throws IOException
   */
  public void modifyColumn(final byte [] tableName, final byte [] columnName, 
    HColumnDescriptor descriptor) 
  throws IOException;


  /**
   * Deletes a column from the specified table
   * @param tableName
   * @param columnName
   * @throws IOException
   */
  public void deleteColumn(final byte [] tableName, final byte [] columnName)
  throws IOException;
  
  /**
   * Puts the table on-line (only needed if table has been previously taken offline)
   * @param tableName
   * @throws IOException
   */
  public void enableTable(final byte [] tableName) throws IOException;
  
  /**
   * Take table offline
   * 
   * @param tableName
   * @throws IOException
   */
  public void disableTable(final byte [] tableName) throws IOException;

  /**
   * Modify a table's metadata
   * 
   * @param tableName
   * @param op
   * @param args
   * @throws IOException
   */
  public void modifyTable(byte[] tableName, HConstants.Modify op, Writable[] args)
    throws IOException;

  /**
   * Shutdown an HBase cluster.
   * @throws IOException
   */
  public void shutdown() throws IOException;

  /**
   * Return cluster status.
   */
  public ClusterStatus getClusterStatus();
}
