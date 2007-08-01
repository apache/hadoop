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
import java.util.SortedMap;

import org.apache.hadoop.io.Text;

/**
 * 
 */
public interface HConnection {
  /**
   * @return proxy connection to master server for this instance
   * @throws MasterNotRunningException
   */
  public HMasterInterface getMaster() throws MasterNotRunningException;

  /** @return - true if the master server is running */
  public boolean isMasterRunning();
  
  /**
   * @param tableName Table to check.
   * @return True if table exists already.
   */
  public boolean tableExists(final Text tableName);
  
  /**
   * List all the userspace tables.  In other words, scan the META table.
   *
   * If we wanted this to be really fast, we could implement a special
   * catalog table that just contains table names and their descriptors.
   * Right now, it only exists as part of the META table's region info.
   *
   * @return - returns an array of HTableDescriptors 
   * @throws IOException
   */
  public HTableDescriptor[] listTables() throws IOException;
  
  /**
   * Gets the servers of the given table.
   * 
   * @param tableName - the table to be located
   * @return map of startRow -> RegionLocation
   * @throws IOException - if the table can not be located after retrying
   */
  public SortedMap<Text, HRegionLocation> getTableServers(Text tableName)
  throws IOException;
  
  /**
   * Reloads servers for the specified table.
   * 
   * @param tableName name of table whose servers are to be reloaded
   * @return map of start key -> RegionLocation
   * @throws IOException
   */
  public SortedMap<Text, HRegionLocation>
  reloadTableServers(final Text tableName) throws IOException;
  
  /** 
   * Establishes a connection to the region server at the specified address.
   * @param regionServer - the server to connect to
   * @return proxy for HRegionServer
   * @throws IOException
   */
  public HRegionInterface getHRegionConnection(HServerAddress regionServer)
  throws IOException;
  
  /**
   * Discard all the information about this table
   * @param tableName the name of the table to close
   */
  public void close(Text tableName);
}
