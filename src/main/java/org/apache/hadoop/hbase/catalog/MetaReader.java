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
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Reads region and assignment information from <code>.META.</code>.
 * <p>
 * Uses the {@link CatalogTracker} to obtain locations and connections to
 * catalogs.
 */
public class MetaReader {
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
    HRegionInterface metaServer =
      catalogTracker.waitForMetaServerConnectionDefault();
    Map<HRegionInfo,HServerAddress> allRegions =
      new TreeMap<HRegionInfo,HServerAddress>();
    Scan scan = new Scan();
    scan.addFamily(HConstants.CATALOG_FAMILY);
    long scannerid = metaServer.openScanner(
        HRegionInfo.FIRST_META_REGIONINFO.getRegionName(), scan);
    try {
      Result data;
      while((data = metaServer.next(scannerid)) != null) {
        if (!data.isEmpty()) {
          Pair<HRegionInfo,HServerAddress> region =
            metaRowToRegionPair(data);
          allRegions.put(region.getFirst(), region.getSecond());
        }
      }
    } finally {
      metaServer.close(scannerid);
    }
    return allRegions;
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
    } catch (java.net.ConnectException e) {
      if (e.getMessage() != null &&
          e.getMessage().contains("Connection refused")) {
        // Treat this exception + message as unavailable catalog table. Catch it
        // and fall through to return a null
      } else {
        throw e;
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
    Result r = catalogTracker.waitForMetaServerConnectionDefault().get(
        CatalogTracker.META_REGION, get);
    if(r == null || r.isEmpty()) {
      return null;
    }
    return metaRowToRegionPair(r);
  }

  public static Pair<HRegionInfo, HServerAddress> metaRowToRegionPair(
      Result data) throws IOException {
    HRegionInfo info = Writables.getHRegionInfo(
      data.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER));
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
   * Checks if the specified table exists.  Looks at the META table hosted on
   * the specified server.
   * @param metaServer server hosting meta
   * @param tableName table to check
   * @return true if the table exists in meta, false if not
   * @throws IOException
   */
  public static boolean tableExists(CatalogTracker catalogTracker,
      String tableName)
  throws IOException {
    HRegionInterface metaServer =
      catalogTracker.waitForMetaServerConnectionDefault();
    byte[] firstRowInTable = Bytes.toBytes(tableName + ",,");
    Scan scan = new Scan(firstRowInTable);
    scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    long scannerid = metaServer.openScanner(
        HRegionInfo.FIRST_META_REGIONINFO.getRegionName(), scan);
    try {
      Result data = metaServer.next(scannerid);
      if (data != null && data.size() > 0) {
        HRegionInfo info = Writables.getHRegionInfo(
          data.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER));
        if (info.getTableDesc().getNameAsString().equals(tableName)) {
          // A region for this table already exists. Ergo table exists.
          return true;
        }
      }
      return false;
    } finally {
      metaServer.close(scannerid);
    }
  }

  /**
   * Gets all of the regions of the specified table from META.
   * @param catalogTracker
   * @param tableName
   * @return
   * @throws IOException
   */
  public static List<HRegionInfo> getTableRegions(CatalogTracker catalogTracker,
      byte [] tableName)
  throws IOException {
    HRegionInterface metaServer =
      catalogTracker.waitForMetaServerConnectionDefault();
    List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
    String tableString = Bytes.toString(tableName);
    byte[] firstRowInTable = Bytes.toBytes(tableString + ",,");
    Scan scan = new Scan(firstRowInTable);
    scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    long scannerid = metaServer.openScanner(
        HRegionInfo.FIRST_META_REGIONINFO.getRegionName(), scan);
    try {
      Result data;
      while((data = metaServer.next(scannerid)) != null) {
        if (data != null && data.size() > 0) {
          HRegionInfo info = Writables.getHRegionInfo(
              data.getValue(HConstants.CATALOG_FAMILY,
                  HConstants.REGIONINFO_QUALIFIER));
          if (info.getTableDesc().getNameAsString().equals(tableString)) {
            regions.add(info);
          } else {
            break;
          }
        }
      }
      return regions;
    } finally {
      metaServer.close(scannerid);
    }
  }

  public static List<Pair<HRegionInfo, HServerAddress>>
  getTableRegionsAndLocations(CatalogTracker catalogTracker, String tableName)
  throws IOException {
    HRegionInterface metaServer =
      catalogTracker.waitForMetaServerConnectionDefault();
    List<Pair<HRegionInfo, HServerAddress>> regions =
      new ArrayList<Pair<HRegionInfo, HServerAddress>>();
    byte[] firstRowInTable = Bytes.toBytes(tableName + ",,");
    Scan scan = new Scan(firstRowInTable);
    scan.addFamily(HConstants.CATALOG_FAMILY);
    long scannerid = metaServer.openScanner(
        HRegionInfo.FIRST_META_REGIONINFO.getRegionName(), scan);
    try {
      Result data;
      while((data = metaServer.next(scannerid)) != null) {
        if (data != null && data.size() > 0) {
          Pair<HRegionInfo, HServerAddress> region = metaRowToRegionPair(data);
          if (region.getFirst().getTableDesc().getNameAsString().equals(
              tableName)) {
            regions.add(region);
          } else {
            break;
          }
        }
      }
      return regions;
    } finally {
      metaServer.close(scannerid);
    }
  }

  public static NavigableMap<HRegionInfo, Result>
  getServerRegions(CatalogTracker catalogTracker, final HServerInfo hsi)
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
          Pair<HRegionInfo, HServerAddress> pair = metaRowToRegionPair(result);
          if (!pair.getSecond().equals(hsi.getServerAddress())) continue;
          hris.put(pair.getFirst(), result);
        }
      }
      return hris;
    } finally {
      metaServer.close(scannerid);
    }
  }
}