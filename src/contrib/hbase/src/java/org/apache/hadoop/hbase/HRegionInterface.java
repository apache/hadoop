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

import org.apache.hadoop.io.*;

import java.io.*;

/*******************************************************************************
 * Clients interact with HRegionServers using
 * a handle to the HRegionInterface.
 ******************************************************************************/
public interface HRegionInterface {
  public static final long versionID = 1L; // initial version

  // Get metainfo about an HRegion

  public HRegionInfo getRegionInfo(Text regionName);

  // Start a scanner for a given HRegion.

  public HScannerInterface openScanner(Text regionName, Text[] columns, Text startRow) throws IOException;

  // GET methods for an HRegion.

  public BytesWritable get(Text regionName, Text row, Text column) throws IOException;
  public BytesWritable[] get(Text regionName, Text row, Text column, int numVersions) throws IOException;
  public BytesWritable[] get(Text regionName, Text row, Text column, long timestamp, int numVersions) throws IOException;
  public LabelledData[] getRow(Text regionName, Text row) throws IOException;

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

  public long startUpdate(Text regionName, long clientid, Text row) throws IOException;
  public void put(Text regionName, long clientid, long lockid, Text column, BytesWritable val) throws IOException;
  public void delete(Text regionName, long clientid, long lockid, Text column) throws IOException;
  public void abort(Text regionName, long clientid, long lockid) throws IOException;
  public void commit(Text regionName, long clientid, long lockid) throws IOException;
  public void renewLease(long lockid, long clientid) throws IOException;
}
