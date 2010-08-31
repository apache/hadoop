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

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.UnknownRegionException;

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
   * Creates a new table.  If splitKeys are specified, then the table will be
   * created with an initial set of multiple regions.  If splitKeys is null,
   * the table will be created with a single region.
   * @param desc table descriptor
   * @param splitKeys
   * @throws IOException
   */
  public void createTable(HTableDescriptor desc, byte [][] splitKeys)
  throws IOException;

  /**
   * Deletes a table
   * @param tableName table to delete
   * @throws IOException e
   */
  public void deleteTable(final byte [] tableName) throws IOException;

  /**
   * Adds a column to the specified table
   * @param tableName table to modify
   * @param column column descriptor
   * @throws IOException e
   */
  public void addColumn(final byte [] tableName, HColumnDescriptor column)
  throws IOException;

  /**
   * Modifies an existing column on the specified table
   * @param tableName table name
   * @param descriptor new column descriptor
   * @throws IOException e
   */
  public void modifyColumn(final byte [] tableName, HColumnDescriptor descriptor)
  throws IOException;


  /**
   * Deletes a column from the specified table. Table must be disabled.
   * @param tableName table to alter
   * @param columnName column family to remove
   * @throws IOException e
   */
  public void deleteColumn(final byte [] tableName, final byte [] columnName)
  throws IOException;

  /**
   * Puts the table on-line (only needed if table has been previously taken offline)
   * @param tableName table to enable
   * @throws IOException e
   */
  public void enableTable(final byte [] tableName) throws IOException;

  /**
   * Take table offline
   *
   * @param tableName table to take offline
   * @throws IOException e
   */
  public void disableTable(final byte [] tableName) throws IOException;

  /**
   * Modify a table's metadata
   *
   * @param tableName table to modify
   * @param htd new descriptor for table
   * @throws IOException e
   */
  public void modifyTable(byte[] tableName, HTableDescriptor htd)
  throws IOException;

  /**
   * Shutdown an HBase cluster.
   * @throws IOException e
   */
  public void shutdown() throws IOException;

  /**
   * Stop HBase Master only.
   * Does not shutdown the cluster.
   * @throws IOException e
   */
  public void stopMaster() throws IOException;

  /**
   * Return cluster status.
   * @return status object
   */
  public ClusterStatus getClusterStatus();


  /**
   * Move the region <code>r</code> to <code>dest</code>.
   * @param encodedRegionName The encoded region name.
   * @param destServerName The servername of the destination regionserver
   * @throws UnknownRegionException Thrown if we can't find a region named
   * <code>encodedRegionName</code>
   */
  public void move(final byte [] encodedRegionName, final byte [] destServerName)
  throws UnknownRegionException;

  /**
   * @param b If true, enable balancer. If false, disable balancer.
   * @return Previous balancer value
   */
  public boolean balance(final boolean b);
}