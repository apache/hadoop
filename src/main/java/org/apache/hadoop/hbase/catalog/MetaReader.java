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
package org.apache.hadoop.hbase.catalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.ipc.RemoteException;

/**
 * Reads region and assignment information from <code>.META.</code>.
 * <p>
 * Uses the {@link CatalogTracker} to obtain locations and connections to
 * catalogs.
 */
public class MetaReader {
  public static final byte [] META_REGION_PREFIX;
  static {
    // Copy the prefix from FIRST_META_REGIONINFO into META_REGION_PREFIX.
    // FIRST_META_REGIONINFO == '.META.,,1'.  META_REGION_PREFIX == '.META.,'
    int len = HRegionInfo.FIRST_META_REGIONINFO.getRegionName().length - 2;
    META_REGION_PREFIX = new byte [len];
    System.arraycopy(HRegionInfo.FIRST_META_REGIONINFO.getRegionName(), 0,
      META_REGION_PREFIX, 0, len);
  }

  /**
   * @param ct
   * @param tableName A user tablename or a .META. table name.
   * @return Interface on to server hosting the <code>-ROOT-</code> or
   * <code>.META.</code> regions.
   * @throws NotAllMetaRegionsOnlineException
   * @throws IOException
   */
  private static HRegionInterface getCatalogRegionInterface(final CatalogTracker ct,
      final byte [] tableName)
  throws NotAllMetaRegionsOnlineException, IOException {
    return Bytes.equals(HConstants.META_TABLE_NAME, tableName)?
      ct.waitForRootServerConnectionDefault():
      ct.waitForMetaServerConnectionDefault();
  }

  /**
   * @param tableName
   * @return Returns region name to look in for regions for <code>tableName</code>;
   * e.g. if we are looking for <code>.META.</code> regions, we need to look
   * in the <code>-ROOT-</code> region, else if a user table, we need to look
   * in the <code>.META.</code> region.
   */
  private static byte [] getCatalogRegionNameForTable(final byte [] tableName) {
    return Bytes.equals(HConstants.META_TABLE_NAME, tableName)?
      HRegionInfo.ROOT_REGIONINFO.getRegionName():
      HRegionInfo.FIRST_META_REGIONINFO.getRegionName();
  }

  /**
   * @param regionName
   * @return Returns region name to look in for <code>regionName</code>;
   * e.g. if we are looking for <code>.META.,,1</code> region, we need to look
   * in <code>-ROOT-</code> region, else if a user region, we need to look
   * in the <code>.META.,,1</code> region.
   */
  private static byte [] getCatalogRegionNameForRegion(final byte [] regionName) {
    return isMetaRegion(regionName)?
      HRegionInfo.ROOT_REGIONINFO.getRegionName():
      HRegionInfo.FIRST_META_REGIONINFO.getRegionName();
  }

  /**
   * @param regionName
   * @return True if <code>regionName</code> is from <code>.META.</code> table.
   */
  private static boolean isMetaRegion(final byte [] regionName) {
    if (regionName.length < META_REGION_PREFIX.length + 2 /* ',', + '1' */) {
      // Can't be meta table region.
      return false;
    }
    // Compare the prefix of regionName.  If it matches META_REGION_PREFIX prefix,
    // then this is region from .META. table.
    return Bytes.compareTo(regionName, 0, META_REGION_PREFIX.length,
      META_REGION_PREFIX, 0, META_REGION_PREFIX.length) == 0;
  }

  /**
   * Performs a full scan of <code>.META.</code>.
   * <p>
   * Returns a map of every region to it's currently assigned server, according
   * to META.  If the region does not have an assignment it will have a null
   * value in the map.
   *
   * @return map of regions to their currently assigned server
   * @throws IOException
   */
  public static Map<HRegionInfo,HServerAddress> fullScan(
      CatalogTracker catalogTracker)
  throws IOException {
    return fullScan(catalogTracker, new TreeSet<String>());
  }

  /**
   * Performs a full scan of <code>.META.</code>, skipping regions from any
   * tables in the specified set of disabled tables.
   * <p>
   * Returns a map of every region to it's currently assigned server, according
   * to META.  If the region does not have an assignment it will have a null
   * value in the map.
   *
   * @param catalogTracker
   * @param disabledTables set of disabled tables that will not be returned
   * @return map of regions to their currently assigned server
   * @throws IOException
   */
  public static Map<HRegionInfo,HServerAddress> fullScan(
      CatalogTracker catalogTracker, final Set<String> disabledTables)
  throws IOException {
    return fullScan(catalogTracker, disabledTables, false);
  }

  /**
   * Performs a full scan of <code>.META.</code>, skipping regions from any
   * tables in the specified set of disabled tables.
   * <p>
   * Returns a map of every region to it's currently assigned server, according
   * to META.  If the region does not have an assignment it will have a null
   * value in the map.
   *
   * @param catalogTracker
   * @param disabledTables set of disabled tables that will not be returned
   * @param excludeOfflinedSplitParents If true, do not include offlined split
   * parents in the return.
   * @return map of regions to their currently assigned server
   * @throws IOException
   */
  public static Map<HRegionInfo,HServerAddress> fullScan(
      CatalogTracker catalogTracker, final Set<String> disabledTables,
      final boolean excludeOfflinedSplitParents)
  throws IOException {
    final Map<HRegionInfo,HServerAddress> regions =
      new TreeMap<HRegionInfo,HServerAddress>();
    Visitor v = new Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r ==  null || r.isEmpty()) return true;
        Pair<HRegionInfo,HServerAddress> region = metaRowToRegionPair(r);
        if (region == null) return true;
        HRegionInfo hri = region.getFirst();
        if (disabledTables.contains(
            hri.getTableDesc().getNameAsString())) return true;
        // Are we to include split parents in the list?
        if (excludeOfflinedSplitParents && hri.isSplitParent()) return true;
        regions.put(hri, region.getSecond());
        return true;
      }
    };
    fullScan(catalogTracker, v);
    return regions;
  }

  /**
   * Performs a full scan of <code>.META.</code>.
   * <p>
   * Returns a map of every region to it's currently assigned server, according
   * to META.  If the region does not have an assignment it will have a null
   * value in the map.
   * <p>
   * Returns HServerInfo which includes server startcode.
   *
   * @return map of regions to their currently assigned server
   * @throws IOException
   */
  public static List<Result> fullScanOfResults(
      CatalogTracker catalogTracker)
  throws IOException {
    final List<Result> regions = new ArrayList<Result>();
    Visitor v = new Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r ==  null || r.isEmpty()) return true;
        regions.add(r);
        return true;
      }
    };
    fullScan(catalogTracker, v);
    return regions;
  }

  /**
   * Performs a full scan of <code>.META.</code>.
   * <p>
   * Returns a map of every region to it's currently assigned server, according
   * to META.  If the region does not have an assignment it will have a null
   * value in the map.
   * @param catalogTracker
   * @param visitor
   * @throws IOException
   */
  public static void fullScan(CatalogTracker catalogTracker,
      final Visitor visitor)
  throws IOException {
    fullScan(catalogTracker, visitor, null);
  }

  /**
   * Performs a full scan of <code>.META.</code>.
   * <p>
   * Returns a map of every region to it's currently assigned server, according
   * to META.  If the region does not have an assignment it will have a null
   * value in the map.
   * @param catalogTracker
   * @param visitor
   * @param startrow Where to start the scan. Pass null if want to begin scan
   * at first row.
   * @throws IOException
   */
  public static void fullScan(CatalogTracker catalogTracker,
      final Visitor visitor, final byte [] startrow)
  throws IOException {
    HRegionInterface metaServer =
      catalogTracker.waitForMetaServerConnectionDefault();
    Scan scan = new Scan();
    if (startrow != null) scan.setStartRow(startrow);
    scan.addFamily(HConstants.CATALOG_FAMILY);
    long scannerid = metaServer.openScanner(
        HRegionInfo.FIRST_META_REGIONINFO.getRegionName(), scan);
    try {
      Result data;
      while((data = metaServer.next(scannerid)) != null) {
        if (!data.isEmpty()) visitor.visit(data);
      }
    } finally {
      metaServer.close(scannerid);
    }
    return;
  }

  /**
   * Reads the location of META from ROOT.
   * @param metaServer connection to server hosting ROOT
   * @return location of META in ROOT, null if not available
   * @throws IOException
   */
  public static HServerAddress readMetaLocation(HRegionInterface metaServer)
  throws IOException {
    return readLocation(metaServer, CatalogTracker.ROOT_REGION,
        CatalogTracker.META_REGION);
  }

  /**
   * Reads the location of the specified region from META.
   * @param catalogTracker
   * @param regionName region to read location of
   * @return location of region in META, null if not available
   * @throws IOException
   */
  public static HServerAddress readRegionLocation(CatalogTracker catalogTracker,
      byte [] regionName)
  throws IOException {
    if (isMetaRegion(regionName)) throw new IllegalArgumentException("See readMetaLocation");
    return readLocation(catalogTracker.waitForMetaServerConnectionDefault(),
        CatalogTracker.META_REGION, regionName);
  }

  private static HServerAddress readLocation(HRegionInterface metaServer,
      byte [] catalogRegionName, byte [] regionName)
  throws IOException {
    Result r = null;
    try {
      r = metaServer.get(catalogRegionName,
        new Get(regionName).addColumn(HConstants.CATALOG_FAMILY,
        HConstants.SERVER_QUALIFIER));
    } catch (java.net.SocketTimeoutException e) {
      // Treat this exception + message as unavailable catalog table. Catch it
      // and fall through to return a null
    } catch (java.net.ConnectException e) {
      if (e.getMessage() != null &&
          e.getMessage().contains("Connection refused")) {
        // Treat this exception + message as unavailable catalog table. Catch it
        // and fall through to return a null
      } else {
        throw e;
      }
    } catch (RemoteException re) {
      IOException ioe = re.unwrapRemoteException();
      if (ioe instanceof NotServingRegionException) {
        // Treat this NSRE as unavailable table.  Catch and fall through to
        // return null below
      } else if (ioe.getMessage().contains("Server not running")) {
        // Treat as unavailable table.
      } else {
        throw re;
      }
    } catch (IOException e) {
      if (e.getCause() != null && e.getCause() instanceof IOException &&
          e.getCause().getMessage() != null &&
          e.getCause().getMessage().contains("Connection reset by peer")) {
        // Treat this exception + message as unavailable catalog table. Catch it
        // and fall through to return a null
      } else {
        throw e;
      }
    }
    if (r == null || r.isEmpty()) {
      return null;
    }
    byte [] value = r.getValue(HConstants.CATALOG_FAMILY,
      HConstants.SERVER_QUALIFIER);
    return new HServerAddress(Bytes.toString(value));
  }

  /**
   * Gets the region info and assignment for the specified region from META.
   * @param catalogTracker
   * @param regionName
   * @return region info and assignment from META, null if not available
   * @throws IOException
   */
  public static Pair<HRegionInfo, HServerAddress> getRegion(
      CatalogTracker catalogTracker, byte [] regionName)
  throws IOException {
    Get get = new Get(regionName);
    get.addFamily(HConstants.CATALOG_FAMILY);
    byte [] meta = getCatalogRegionNameForRegion(regionName);
    Result r = catalogTracker.waitForMetaServerConnectionDefault().get(meta, get);
    if(r == null || r.isEmpty()) {
      return null;
    }
    return metaRowToRegionPair(r);
  }

  /**
   * @param data A .META. table row.
   * @return A pair of the regioninfo and the server address from <code>data</code>
   * or null for server address if no address set in .META. or null for a result
   * if no HRegionInfo found.
   * @throws IOException
   */
  public static Pair<HRegionInfo, HServerAddress> metaRowToRegionPair(
      Result data) throws IOException {
    byte [] bytes =
      data.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    if (bytes == null) return null;
    HRegionInfo info = Writables.getHRegionInfo(bytes);
    final byte[] value = data.getValue(HConstants.CATALOG_FAMILY,
      HConstants.SERVER_QUALIFIER);
    if (value != null && value.length > 0) {
      HServerAddress server = new HServerAddress(Bytes.toString(value));
      return new Pair<HRegionInfo,HServerAddress>(info, server);
    } else {
      return new Pair<HRegionInfo, HServerAddress>(info, null);
    }
  }

  /**
   * @param data A .META. table row.
   * @return A pair of the regioninfo and the server info from <code>data</code>
   * (or null for server address if no address set in .META.).
   * @throws IOException
   */
  public static Pair<HRegionInfo, HServerInfo> metaRowToRegionPairWithInfo(
      Result data) throws IOException {
    byte [] bytes = data.getValue(HConstants.CATALOG_FAMILY,
      HConstants.REGIONINFO_QUALIFIER);
    if (bytes == null) return null;
    HRegionInfo info = Writables.getHRegionInfo(bytes);
    final byte[] value = data.getValue(HConstants.CATALOG_FAMILY,
      HConstants.SERVER_QUALIFIER);
    if (value != null && value.length > 0) {
      final long startCode = Bytes.toLong(data.getValue(HConstants.CATALOG_FAMILY,
          HConstants.STARTCODE_QUALIFIER));
      HServerAddress server = new HServerAddress(Bytes.toString(value));
      HServerInfo hsi = new HServerInfo(server, startCode, 0,
          server.getHostname());
      return new Pair<HRegionInfo,HServerInfo>(info, hsi);
    } else {
      return new Pair<HRegionInfo, HServerInfo>(info, null);
    }
  }

  /**
   * Checks if the specified table exists.  Looks at the META table hosted on
   * the specified server.
   * @param catalogTracker
   * @param tableName table to check
   * @return true if the table exists in meta, false if not
   * @throws IOException
   */
  public static boolean tableExists(CatalogTracker catalogTracker,
      String tableName)
  throws IOException {
    if (tableName.equals(HTableDescriptor.ROOT_TABLEDESC.getNameAsString()) ||
        tableName.equals(HTableDescriptor.META_TABLEDESC.getNameAsString())) {
      // Catalog tables always exist.
      return true;
    }
    HRegionInterface metaServer =
      catalogTracker.waitForMetaServerConnectionDefault();
    Scan scan = getScanForTableName(Bytes.toBytes(tableName));
    scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    long scannerid = metaServer.openScanner(
        HRegionInfo.FIRST_META_REGIONINFO.getRegionName(), scan);
    try {
      Result data = metaServer.next(scannerid);
      if (data != null && data.size() > 0) {
          return true;
      }
      return false;
    } finally {
      metaServer.close(scannerid);
    }
  }

  /**
   * Gets all of the regions of the specified table.
   * @param catalogTracker
   * @param tableName
   * @return Ordered list of {@link HRegionInfo}.
   * @throws IOException
   */
  public static List<HRegionInfo> getTableRegions(CatalogTracker catalogTracker,
      byte [] tableName)
  throws IOException {
    return getTableRegions(catalogTracker, tableName, false);
  }

  /**
   * Gets all of the regions of the specified table.
   * @param catalogTracker
   * @param tableName
   * @param excludeOfflinedSplitParents If true, do not include offlined split
   * parents in the return.
   * @return Ordered list of {@link HRegionInfo}.
   * @throws IOException
   */
  public static List<HRegionInfo> getTableRegions(CatalogTracker catalogTracker,
      byte [] tableName, final boolean excludeOfflinedSplitParents)
  throws IOException {
    if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
      // If root, do a bit of special handling.
      List<HRegionInfo> list = new ArrayList<HRegionInfo>();
      list.add(HRegionInfo.ROOT_REGIONINFO);
      return list;
    } else if (Bytes.equals(tableName, HConstants.META_TABLE_NAME)) {
      // Same for .META. table
      List<HRegionInfo> list = new ArrayList<HRegionInfo>();
      list.add(HRegionInfo.FIRST_META_REGIONINFO);
      return list;
    }

    // Its a user table.
    HRegionInterface metaServer =
      getCatalogRegionInterface(catalogTracker, tableName);
    List<HRegionInfo> regions = new ArrayList<HRegionInfo>();

    Scan scan = getScanForTableName(tableName);
    scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    long scannerid =
      metaServer.openScanner(getCatalogRegionNameForTable(tableName), scan);
    try {
      Result data;
      while((data = metaServer.next(scannerid)) != null) {
        if (data != null && data.size() > 0) {
          HRegionInfo info = Writables.getHRegionInfo(
              data.getValue(HConstants.CATALOG_FAMILY,
                  HConstants.REGIONINFO_QUALIFIER));
          if (excludeOfflinedSplitParents && info.isSplitParent()) continue;
          regions.add(info);
        }
      }
      return regions;
    } finally {
      metaServer.close(scannerid);
    }
  }

  /**
   * This method creates a Scan object that will only scan catalog rows that
   * belong to the specified table. It doesn't specify any columns.
   * This is a better alternative to just using a start row and scan until
   * it hits a new table since that requires parsing the HRI to get the table
   * name.
   * @param tableName bytes of table's name
   * @return configured Scan object
   */
  public static Scan getScanForTableName(byte[] tableName) {
    String strName = Bytes.toString(tableName);
    // Start key is just the table name with delimiters
    byte[] startKey = Bytes.toBytes(strName + ",,");
    // Stop key appends the smallest possible char to the table name
    byte[] stopKey = Bytes.toBytes(strName + " ,,");

    Scan scan = new Scan(startKey);
    scan.setStopRow(stopKey);
    return scan;
  }

  /**
   * @param catalogTracker
   * @param tableName
   * @return Return list of regioninfos and server addresses.
   * @throws IOException
   * @throws InterruptedException
   */
  public static List<Pair<HRegionInfo, HServerAddress>>
  getTableRegionsAndLocations(CatalogTracker catalogTracker, String tableName)
  throws IOException, InterruptedException {
    byte [] tableNameBytes = Bytes.toBytes(tableName);
    if (Bytes.equals(tableNameBytes, HConstants.ROOT_TABLE_NAME)) {
      // If root, do a bit of special handling.
      HServerAddress hsa = catalogTracker.getRootLocation();
      List<Pair<HRegionInfo, HServerAddress>> list =
        new ArrayList<Pair<HRegionInfo, HServerAddress>>();
      list.add(new Pair<HRegionInfo, HServerAddress>(HRegionInfo.ROOT_REGIONINFO, hsa));
      return list;
    }
    HRegionInterface metaServer =
      getCatalogRegionInterface(catalogTracker, tableNameBytes);
    List<Pair<HRegionInfo, HServerAddress>> regions =
      new ArrayList<Pair<HRegionInfo, HServerAddress>>();
    Scan scan = getScanForTableName(tableNameBytes);
    scan.addFamily(HConstants.CATALOG_FAMILY);
    long scannerid =
      metaServer.openScanner(getCatalogRegionNameForTable(tableNameBytes), scan);
    try {
      Result data;
      while((data = metaServer.next(scannerid)) != null) {
        if (data != null && data.size() > 0) {
          Pair<HRegionInfo, HServerAddress> region = metaRowToRegionPair(data);
          if (region == null) continue;
          regions.add(region);
        }
      }
      return regions;
    } finally {
      metaServer.close(scannerid);
    }
  }

  /**
   * @param catalogTracker
   * @param hsi Server specification
   * @return List of user regions installed on this server (does not include
   * catalog regions).
   * @throws IOException
   */
  public static NavigableMap<HRegionInfo, Result>
  getServerUserRegions(CatalogTracker catalogTracker, final HServerInfo hsi)
  throws IOException {
    HRegionInterface metaServer =
      catalogTracker.waitForMetaServerConnectionDefault();
    NavigableMap<HRegionInfo, Result> hris = new TreeMap<HRegionInfo, Result>();
    Scan scan = new Scan();
    scan.addFamily(HConstants.CATALOG_FAMILY);
    long scannerid = metaServer.openScanner(
        HRegionInfo.FIRST_META_REGIONINFO.getRegionName(), scan);
    try {
      Result result;
      while((result = metaServer.next(scannerid)) != null) {
        if (result != null && result.size() > 0) {
          Pair<HRegionInfo, HServerInfo> pair =
            metaRowToRegionPairWithInfo(result);
          if (pair == null) continue;
          if (pair.getSecond() == null || !pair.getSecond().equals(hsi)) {
            continue;
          }
          hris.put(pair.getFirst(), result);
        }
      }
      return hris;
    } finally {
      metaServer.close(scannerid);
    }
  }

  /**
   * Implementations 'visit' a catalog table row.
   */
  public interface Visitor {
    /**
     * Visit the catalog table row.
     * @param r A row from catalog table
     * @return True if we are to proceed scanning the table, else false if
     * we are to stop now.
     */
    public boolean visit(final Result r) throws IOException;
  }
}
