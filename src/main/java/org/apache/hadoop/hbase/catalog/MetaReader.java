/**
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.ipc.RemoteException;

/**
 * Reads region and assignment information from <code>.META.</code>.
 */
public class MetaReader {
  // TODO: Strip CatalogTracker from this class.  Its all over and in the end
  // its only used to get its Configuration so we can get associated
  // Connection.
  private static final Log LOG = LogFactory.getLog(MetaReader.class);

  static final byte [] META_REGION_PREFIX;
  static {
    // Copy the prefix from FIRST_META_REGIONINFO into META_REGION_PREFIX.
    // FIRST_META_REGIONINFO == '.META.,,1'.  META_REGION_PREFIX == '.META.,'
    int len = HRegionInfo.FIRST_META_REGIONINFO.getRegionName().length - 2;
    META_REGION_PREFIX = new byte [len];
    System.arraycopy(HRegionInfo.FIRST_META_REGIONINFO.getRegionName(), 0,
      META_REGION_PREFIX, 0, len);
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
    return Bytes.equals(regionName, 0, META_REGION_PREFIX.length,
      META_REGION_PREFIX, 0, META_REGION_PREFIX.length);
  }

  /**
   * Performs a full scan of <code>.META.</code>, skipping regions from any
   * tables in the specified set of disabled tables.
   * @param catalogTracker
   * @param disabledTables set of disabled tables that will not be returned
   * @return Returns a map of every region to it's currently assigned server,
   * according to META.  If the region does not have an assignment it will have
   * a null value in the map.
   * @throws IOException
   */
  public static Map<HRegionInfo, ServerName> fullScan(
      CatalogTracker catalogTracker, final Set<String> disabledTables)
  throws IOException {
    return fullScan(catalogTracker, disabledTables, false);
  }

  /**
   * Performs a full scan of <code>.META.</code>, skipping regions from any
   * tables in the specified set of disabled tables.
   * @param catalogTracker
   * @param disabledTables set of disabled tables that will not be returned
   * @param excludeOfflinedSplitParents If true, do not include offlined split
   * parents in the return.
   * @return Returns a map of every region to it's currently assigned server,
   * according to META.  If the region does not have an assignment it will have
   * a null value in the map.
   * @throws IOException
   */
  public static Map<HRegionInfo, ServerName> fullScan(
      CatalogTracker catalogTracker, final Set<String> disabledTables,
      final boolean excludeOfflinedSplitParents)
  throws IOException {
    final Map<HRegionInfo, ServerName> regions =
      new TreeMap<HRegionInfo, ServerName>();
    Visitor v = new Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r ==  null || r.isEmpty()) return true;
        Pair<HRegionInfo, ServerName> region = parseCatalogResult(r);
        if (region == null) return true;
        HRegionInfo hri = region.getFirst();
        if (disabledTables.contains(
            hri.getTableNameAsString())) return true;
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
   * @return List of {@link Result}
   * @throws IOException
   */
  public static List<Result> fullScan(CatalogTracker catalogTracker)
  throws IOException {
    CollectAllVisitor v = new CollectAllVisitor();
    fullScan(catalogTracker, v, null);
    return v.getResults();
  }

  /**
   * Performs a full scan of a <code>-ROOT-</code> table.
   * @return List of {@link Result}
   * @throws IOException
   */
  public static List<Result> fullScanOfRoot(CatalogTracker catalogTracker)
  throws IOException {
    CollectAllVisitor v = new CollectAllVisitor();
    fullScan(catalogTracker, v, null, true);
    return v.getResults();
  }

  /**
   * Performs a full scan of <code>.META.</code>.
   * @param catalogTracker
   * @param visitor Visitor invoked against each row.
   * @throws IOException
   */
  public static void fullScan(CatalogTracker catalogTracker,
      final Visitor visitor)
  throws IOException {
    fullScan(catalogTracker, visitor, null);
  }

  /**
   * Performs a full scan of <code>.META.</code>.
   * @param catalogTracker
   * @param visitor Visitor invoked against each row.
   * @param startrow Where to start the scan. Pass null if want to begin scan
   * at first row (The visitor will stop the Scan when its done so no need to
   * pass a stoprow).
   * @throws IOException
   */
  public static void fullScan(CatalogTracker catalogTracker,
      final Visitor visitor, final byte [] startrow)
  throws IOException {
    fullScan(catalogTracker, visitor, startrow, false);
  }

  /**
   * Callers should call close on the returned {@link HTable} instance.
   * @param catalogTracker We'll use this catalogtracker's connection
   * @param tableName Table to get an {@link HTable} against.
   * @return An {@link HTable} for <code>tableName</code>
   * @throws IOException
   */
  private static HTable getHTable(final CatalogTracker catalogTracker,
      final byte [] tableName)
  throws IOException {
    // Passing the CatalogTracker's connection configuration ensures this
    // HTable instance uses the CatalogTracker's connection.
    return new HTable(catalogTracker.getConnection().getConfiguration(), tableName);
  }

  /**
   * Callers should call close on the returned {@link HTable} instance.
   * @param catalogTracker
   * @param regionName
   * @return
   * @throws IOException
   */
  static HTable getCatalogHTable(final CatalogTracker catalogTracker,
      final byte [] regionName)
  throws IOException {
    return isMetaRegion(regionName)?
      getRootHTable(catalogTracker):
      getMetaHTable(catalogTracker);
  }

  /**
   * Callers should call close on the returned {@link HTable} instance.
   * @param ct
   * @return An {@link HTable} for <code>.META.</code>
   * @throws IOException
   */
  static HTable getMetaHTable(final CatalogTracker ct)
  throws IOException {
    return getHTable(ct, HConstants.META_TABLE_NAME);
  }

  /**
   * Callers should call close on the returned {@link HTable} instance.
   * @param ct
   * @return An {@link HTable} for <code>-ROOT-</code>
   * @throws IOException
   */
  static HTable getRootHTable(final CatalogTracker ct)
  throws IOException {
    return getHTable(ct, HConstants.ROOT_TABLE_NAME);
  }

  /**
   * @param t Table to use (will be closed when done).
   * @param g Get to run
   * @throws IOException
   */
  private static Result get(final HTable t, final Get g) throws IOException {
    try {
      return t.get(g);
    } finally {
      t.close();
    }
  }

  /**
   * Reads the location of META from ROOT.
   * @param metaServer connection to server hosting ROOT
   * @return location of META in ROOT where location, or null if not available
   * @throws IOException
   * @deprecated Does not retry; use {@link #readRegionLocation(CatalogTracker, byte[])}
   */
  public static ServerName readMetaLocation(HRegionInterface metaServer)
  throws IOException {
    return readLocation(metaServer, CatalogTracker.ROOT_REGION_NAME,
        CatalogTracker.META_REGION_NAME);
  }

  /**
   * Reads the location of the specified region
   * @param catalogTracker
   * @param regionName region whose location we are after
   * @return location of region as a {@link ServerName} or null if not found
   * @throws IOException
   */
  public static ServerName readRegionLocation(CatalogTracker catalogTracker,
      byte [] regionName)
  throws IOException {
    Pair<HRegionInfo, ServerName> pair = getRegion(catalogTracker, regionName);
    return (pair == null || pair.getSecond() == null)? null: pair.getSecond();
  }

  // TODO: Remove when deprecated dependencies are removed.
  private static ServerName readLocation(HRegionInterface metaServer,
      byte [] catalogRegionName, byte [] regionName)
  throws IOException {
    Result r = null;
    try {
      r = metaServer.get(catalogRegionName,
        new Get(regionName).
        addColumn(HConstants.CATALOG_FAMILY,
          HConstants.SERVER_QUALIFIER).
        addColumn(HConstants.CATALOG_FAMILY,
          HConstants.STARTCODE_QUALIFIER));
    } catch (java.net.SocketTimeoutException e) {
      // Treat this exception + message as unavailable catalog table. Catch it
      // and fall through to return a null
    } catch (java.net.SocketException e) {
      // Treat this exception + message as unavailable catalog table. Catch it
      // and fall through to return a null
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
    return getServerNameFromCatalogResult(r);
  }

  /**
   * Gets the region info and assignment for the specified region.
   * @param catalogTracker
   * @param regionName Region to lookup.
   * @return Location and HRegionInfo for <code>regionName</code>
   * @throws IOException
   */
  public static Pair<HRegionInfo, ServerName> getRegion(
      CatalogTracker catalogTracker, byte [] regionName)
  throws IOException {
    Get get = new Get(regionName);
    get.addFamily(HConstants.CATALOG_FAMILY);
    Result r = get(getCatalogHTable(catalogTracker, regionName), get);
    return (r == null || r.isEmpty())? null: parseCatalogResult(r);
  }

  /**
   * Extract a {@link ServerName}
   * For use on catalog table {@link Result}.
   * @param r Result to pull from
   * @return A ServerName instance or null if necessary fields not found or empty.
   */
  public static ServerName getServerNameFromCatalogResult(final Result r) {
    byte[] value = r.getValue(HConstants.CATALOG_FAMILY,
      HConstants.SERVER_QUALIFIER);
    if (value == null || value.length == 0) return null;
    String hostAndPort = Bytes.toString(value);
    value = r.getValue(HConstants.CATALOG_FAMILY,
      HConstants.STARTCODE_QUALIFIER);
    if (value == null || value.length == 0) return null;
    return new ServerName(hostAndPort, Bytes.toLong(value));
  }

  /**
   * Extract a HRegionInfo and ServerName.
   * For use on catalog table {@link Result}.
   * @param r Result to pull from
   * @return A pair of the {@link HRegionInfo} and the {@link ServerName}
   * (or null for server address if no address set in .META.).
   * @throws IOException
   */
  public static Pair<HRegionInfo, ServerName> parseCatalogResult(final Result r)
  throws IOException {
    HRegionInfo info =
      parseHRegionInfoFromCatalogResult(r, HConstants.REGIONINFO_QUALIFIER);
    ServerName sn = getServerNameFromCatalogResult(r);
    return new Pair<HRegionInfo, ServerName>(info, sn);
  }

  /**
   * Parse the content of the cell at {@link HConstants#CATALOG_FAMILY} and
   * <code>qualifier</code> as an HRegionInfo and return it, or null.
   * For use on catalog table {@link Result}.
   * @param r Result instance to pull from.
   * @param qualifier Column family qualifier -- either
   * {@link HConstants#SPLITA_QUALIFIER}, {@link HConstants#SPLITB_QUALIFIER} or
   * {@link HConstants#REGIONINFO_QUALIFIER}.
   * @return An HRegionInfo instance or null.
   * @throws IOException
   */
  public static HRegionInfo parseHRegionInfoFromCatalogResult(final Result r,
      byte [] qualifier)
  throws IOException {
    byte [] bytes = r.getValue(HConstants.CATALOG_FAMILY, qualifier);
    if (bytes == null || bytes.length <= 0) return null;
    return Writables.getHRegionInfoOrNull(bytes);
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
    final byte [] tableNameBytes = Bytes.toBytes(tableName);
    // Make a version of ResultCollectingVisitor that only collects the first
    CollectingVisitor<HRegionInfo> visitor = new CollectingVisitor<HRegionInfo>() {
      private HRegionInfo current = null;

      @Override
      public boolean visit(Result r) throws IOException {
        this.current =
          parseHRegionInfoFromCatalogResult(r, HConstants.REGIONINFO_QUALIFIER);
        if (this.current == null) {
          LOG.warn("No serialized HRegionInfo in " + r);
          return true;
        }
        if (!isInsideTable(this.current, tableNameBytes)) return false;
        if (this.current.isSplitParent()) return true;
        // Else call super and add this Result to the collection.
        super.visit(r);
        // Stop collecting regions from table after we get one.
        return false;
      }

      @Override
      void add(Result r) {
        // Add the current HRI.
        this.results.add(this.current);
      }
    };
    fullScan(catalogTracker, visitor, getTableStartRowForMeta(tableNameBytes));
    // If visitor has results >= 1 then table exists.
    return visitor.getResults().size() >= 1;
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
    List<Pair<HRegionInfo, ServerName>> result = null;
    try {
      result = getTableRegionsAndLocations(catalogTracker, tableName,
        excludeOfflinedSplitParents);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return getListOfHRegionInfos(result);
  }

  static List<HRegionInfo> getListOfHRegionInfos(final List<Pair<HRegionInfo, ServerName>> pairs) {
    if (pairs == null || pairs.isEmpty()) return null;
    List<HRegionInfo> result = new ArrayList<HRegionInfo>(pairs.size());
    for (Pair<HRegionInfo, ServerName> pair: pairs) {
      result.add(pair.getFirst());
    }
    return result;
  }

  /**
   * @param current
   * @param tableName
   * @return True if <code>current</code> tablename is equal to
   * <code>tableName</code>
   */
  static boolean isInsideTable(final HRegionInfo current, final byte [] tableName) {
    return Bytes.equals(tableName, current.getTableName());
  }

  /**
   * @param tableName
   * @return Place to start Scan in <code>.META.</code> when passed a
   * <code>tableName</code>; returns &lt;tableName&rt; &lt;,&rt; &lt;,&rt;
   */
  static byte [] getTableStartRowForMeta(final byte [] tableName) {
    byte [] startRow = new byte[tableName.length + 2];
    System.arraycopy(tableName, 0, startRow, 0, tableName.length);
    startRow[startRow.length - 2] = HRegionInfo.DELIMITER;
    startRow[startRow.length - 1] = HRegionInfo.DELIMITER;
    return startRow;
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
   * @return Return list of regioninfos and server.
   * @throws IOException
   * @throws InterruptedException
   */
  public static List<Pair<HRegionInfo, ServerName>>
  getTableRegionsAndLocations(CatalogTracker catalogTracker, String tableName)
  throws IOException, InterruptedException {
    return getTableRegionsAndLocations(catalogTracker, Bytes.toBytes(tableName),
      true);
  }

  /**
   * @param catalogTracker
   * @param tableName
   * @return Return list of regioninfos and server addresses.
   * @throws IOException
   * @throws InterruptedException
   */
  public static List<Pair<HRegionInfo, ServerName>>
  getTableRegionsAndLocations(final CatalogTracker catalogTracker,
      final byte [] tableName, final boolean excludeOfflinedSplitParents)
  throws IOException, InterruptedException {
    if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
      // If root, do a bit of special handling.
      ServerName serverName = catalogTracker.getRootLocation();
      List<Pair<HRegionInfo, ServerName>> list =
        new ArrayList<Pair<HRegionInfo, ServerName>>();
      list.add(new Pair<HRegionInfo, ServerName>(HRegionInfo.ROOT_REGIONINFO,
        serverName));
      return list;
    }
    // Make a version of CollectingVisitor that collects HRegionInfo and ServerAddress
    CollectingVisitor<Pair<HRegionInfo, ServerName>> visitor =
        new CollectingVisitor<Pair<HRegionInfo, ServerName>>() {
      private Pair<HRegionInfo, ServerName> current = null;

      @Override
      public boolean visit(Result r) throws IOException {
        HRegionInfo hri =
          parseHRegionInfoFromCatalogResult(r, HConstants.REGIONINFO_QUALIFIER);
        if (hri == null) {
          LOG.warn("No serialized HRegionInfo in " + r);
          return true;
        }
        if (!isInsideTable(hri, tableName)) return false;
        if (excludeOfflinedSplitParents && hri.isSplitParent()) return true;
        ServerName sn = getServerNameFromCatalogResult(r);
        // Populate this.current so available when we call #add
        this.current = new Pair<HRegionInfo, ServerName>(hri, sn);
        // Else call super and add this Result to the collection.
        return super.visit(r);
      }

      @Override
      void add(Result r) {
        this.results.add(this.current);
      }
    };
    fullScan(catalogTracker, visitor, getTableStartRowForMeta(tableName),
      Bytes.equals(tableName, HConstants.META_TABLE_NAME));
    return visitor.getResults();
  }

  /**
   * @param catalogTracker
   * @param serverName
   * @return List of user regions installed on this server (does not include
   * catalog regions).
   * @throws IOException
   */
  public static NavigableMap<HRegionInfo, Result>
  getServerUserRegions(CatalogTracker catalogTracker, final ServerName serverName)
  throws IOException {
    final NavigableMap<HRegionInfo, Result> hris = new TreeMap<HRegionInfo, Result>();
    // Fill the above hris map with entries from .META. that have the passed
    // servername.
    CollectingVisitor<Result> v = new CollectingVisitor<Result>() {
      @Override
      void add(Result r) {
        if (r == null || r.isEmpty()) return;
        ServerName sn = getServerNameFromCatalogResult(r);
        if (sn != null && sn.equals(serverName)) this.results.add(r); 
      }
    };
    fullScan(catalogTracker, v);
    List<Result> results = v.getResults();
    if (results != null && !results.isEmpty()) {
      // Convert results to Map keyed by HRI
      for (Result r: results) {
        Pair<HRegionInfo, ServerName> p = parseCatalogResult(r);
        if (p != null && p.getFirst() != null) hris.put(p.getFirst(), r);
      }
    }
    return hris;
  }

  public static void fullScanMetaAndPrint(final CatalogTracker catalogTracker)
  throws IOException {
    Visitor v = new Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r ==  null || r.isEmpty()) return true;
        LOG.info("fullScanMetaAndPrint.Current Meta Row: " + r);
        HRegionInfo hrim = MetaEditor.getHRegionInfo(r);
        LOG.info("fullScanMetaAndPrint.HRI Print= " + hrim);
        return true;
      }
    };
    fullScan(catalogTracker, v);
  }

  /**
   * Fully scan a given region, on a given server starting with given row.
   * @param hRegionInterface region server
   * @param visitor visitor
   * @param regionName name of region
   * @param startrow start row
   * @throws IOException
   * @deprecated Does not retry; use fullScan xxx instead.
   x
   */
  public static void fullScan(HRegionInterface hRegionInterface,
                              Visitor visitor, final byte[] regionName,
                              byte[] startrow) throws IOException {
    if (hRegionInterface == null) return;
    Scan scan = new Scan();
    if (startrow != null) scan.setStartRow(startrow);
    scan.addFamily(HConstants.CATALOG_FAMILY);
    long scannerid = hRegionInterface.openScanner(regionName, scan);
    try {
      Result data;
      while((data = hRegionInterface.next(scannerid)) != null) {
        if (!data.isEmpty()) visitor.visit(data);
      }
    } finally {
      hRegionInterface.close(scannerid);
    }
    return;
  }

  /**
   * Performs a full scan of a catalog table.
   * @param catalogTracker
   * @param visitor Visitor invoked against each row.
   * @param startrow Where to start the scan. Pass null if want to begin scan
   * at first row.
   * @param scanRoot True if we are to scan <code>-ROOT-</code> rather than
   * <code>.META.</code>, the default (pass false to scan .META.)
   * @throws IOException
   */
  static void fullScan(CatalogTracker catalogTracker,
    final Visitor visitor, final byte [] startrow, final boolean scanRoot)
  throws IOException {
    Scan scan = new Scan();
    if (startrow != null) scan.setStartRow(startrow);
    scan.addFamily(HConstants.CATALOG_FAMILY);
    HTable metaTable = scanRoot?
      getRootHTable(catalogTracker): getMetaHTable(catalogTracker);
    ResultScanner scanner = metaTable.getScanner(scan);
    try {
      Result data;
      while((data = scanner.next()) != null) {
        if (data.isEmpty()) continue;
        // Break if visit returns false.
        if (!visitor.visit(data)) break;
      }
    } finally {
      scanner.close();
      metaTable.close();
    }
    return;
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

  /**
   * A {@link Visitor} that collects content out of passed {@link Result}.
   */
  static abstract class CollectingVisitor<T> implements Visitor {
    final List<T> results = new ArrayList<T>();
    @Override
    public boolean visit(Result r) throws IOException {
      if (r ==  null || r.isEmpty()) return true;
      add(r);
      return true;
    }

    abstract void add(Result r);

    /**
     * @return Collected results; wait till visits complete to collect all
     * possible results
     */
    List<T> getResults() {
      return this.results;
    }
  }

  /**
   * Collects all returned.
   */
  static class CollectAllVisitor extends CollectingVisitor<Result> {
    @Override
    void add(Result r) {
      this.results.add(r);
    }
  }
}
