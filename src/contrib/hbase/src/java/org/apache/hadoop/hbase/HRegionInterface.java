/**
 * Copyright 2006 The Apache Software Foundation
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

import java.io.IOException;

import org.apache.hadoop.hbase.io.KeyedData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;

/*******************************************************************************
 * Clients interact with HRegionServers using
 * a handle to the HRegionInterface.
 ******************************************************************************/
public interface HRegionInterface extends VersionedProtocol {
  /** initial version */
  public static final long versionID = 1L;

  /** 
   * Get metainfo about an HRegion
   * 
   * @param regionName                  - name of the region
   * @return                            - HRegionInfo object for region
   * @throws NotServingRegionException
   */
  public HRegionInfo getRegionInfo(final Text regionName)
  throws NotServingRegionException;

  /**
   * Retrieve a single value from the specified region for the specified row
   * and column keys
   * 
   * @param regionName name of region
   * @param row row key
   * @param column column key
   * @return alue for that region/row/column
   * @throws IOException
   */
  public byte [] get(final Text regionName, final Text row, final Text column)
  throws IOException;

  /**
   * Get the specified number of versions of the specified row and column
   * 
   * @param regionName region name
   * @param row row key
   * @param column column key
   * @param numVersions number of versions to return
   * @return array of values
   * @throws IOException
   */
  public byte [][] get(final Text regionName, final Text row,
      final Text column, final int numVersions)
  throws IOException;
  
  /**
   * Get the specified number of versions of the specified row and column with
   * the specified timestamp.
   *
   * @param regionName region name
   * @param row row key
   * @param column column key
   * @param timestamp timestamp
   * @param numVersions number of versions to return
   * @return array of values
   * @throws IOException
   */
  public byte [][] get(final Text regionName, final Text row,
      final Text column, final long timestamp, final int numVersions)
  throws IOException;
  
  /**
   * Get all the data for the specified row
   * 
   * @param regionName region name
   * @param row row key
   * @return array of values
   * @throws IOException
   */
  public KeyedData[] getRow(final Text regionName, final Text row)
  throws IOException;

  //////////////////////////////////////////////////////////////////////////////
  // Start an atomic row insertion/update.  No changes are committed until the 
  // call to commit() returns. A call to abort() will abandon any updates in progress.
  //
  // Callers to this method are given a lease for each unique lockid; before the
  // lease expires, either abort() or commit() must be called. If it is not 
  // called, the system will automatically call abort() on the client's behalf.
  //
  // The client can gain extra time with a call to renewLease().
  //////////////////////////////////////////////////////////////////////////////

  /** 
   * Start an atomic row insertion/update.  No changes are committed until the 
   * call to commit() returns. A call to abort() will abandon any updates in progress.
   *
   * Callers to this method are given a lease for each unique lockid; before the
   * lease expires, either abort() or commit() must be called. If it is not 
   * called, the system will automatically call abort() on the client's behalf.
   *
   * The client can gain extra time with a call to renewLease().
   * Start an atomic row insertion or update
   * 
   * @param regionName region name
   * @param clientid a unique value to identify the client
   * @param row Name of row to start update against.
   * @return Row lockid.
   * @throws IOException
   */
  public long startUpdate(final Text regionName, final long clientid,
      final Text row)
  throws IOException;
  
  /** 
   * Change a value for the specified column
   *
   * @param regionName region name
   * @param clientid a unique value to identify the client
   * @param lockid lock id returned from startUpdate
   * @param column column whose value is being set
   * @param val new value for column
   * @throws IOException
   */
  public void put(final Text regionName, final long clientid, final long lockid,
      final Text column, final byte [] val)
  throws IOException;
  
  /** 
   * Delete the value for a column
   *
   * @param regionName region name
   * @param clientid a unique value to identify the client
   * @param lockid lock id returned from startUpdate
   * @param column name of column whose value is to be deleted
   * @throws IOException
   */
  public void delete(final Text regionName, final long clientid,
      final long lockid, final Text column)
  throws IOException;
  
  /** 
   * Abort a row mutation
   *
   * @param regionName region name
   * @param clientid a unique value to identify the client
   * @param lockid lock id returned from startUpdate
   * @throws IOException
   */
  public void abort(final Text regionName, final long clientid, 
      final long lockid)
  throws IOException;
  
  /** 
   * Finalize a row mutation
   *
   * @param regionName region name
   * @param clientid a unique value to identify the client
   * @param lockid lock id returned from startUpdate
   * @throws IOException
   */
  public void commit(final Text regionName, final long clientid,
      final long lockid)
  throws IOException;
  
  /**
   * Renew lease on update
   * 
   * @param lockid lock id returned from startUpdate
   * @param clientid a unique value to identify the client
   * @throws IOException
   */
  public void renewLease(long lockid, long clientid) throws IOException;

  //////////////////////////////////////////////////////////////////////////////
  // remote scanner interface
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Opens a remote scanner.
   * 
   * @param regionName name of region to scan
   * @param columns columns to scan
   * @param startRow starting row to scan
   *
   * @return scannerId scanner identifier used in other calls
   * @throws IOException
   */
  public long openScanner(Text regionName, Text[] columns, Text startRow)
  throws IOException;

  /**
   * Get the next set of values
   * 
   * @param scannerId clientId passed to openScanner
   * @return array of values
   * @throws IOException
   */
  public KeyedData[] next(long scannerId) throws IOException;
  
  /**
   * Close a scanner
   * 
   * @param scannerId the scanner id returned by openScanner
   * @throws IOException
   */
  public void close(long scannerId) throws IOException;
}
