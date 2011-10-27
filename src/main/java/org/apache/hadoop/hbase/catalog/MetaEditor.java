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
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Writes region and assignment information to <code>.META.</code>.
 * TODO: Put MetaReader and MetaEditor together; doesn't make sense having
 * them distinct.
 */
public class MetaEditor {
  // TODO: Strip CatalogTracker from this class.  Its all over and in the end
  // its only used to get its Configuration so we can get associated
  // Connection.
  private static final Log LOG = LogFactory.getLog(MetaEditor.class);

  private static Put makePutFromRegionInfo(HRegionInfo regionInfo)
  throws IOException {
    Put put = new Put(regionInfo.getRegionName());
    put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(regionInfo));
    return put;
  }

  /**
   * Put the passed <code>p</code> to the <code>.META.</code> table.
   * @param ct CatalogTracker on whose back we will ride the edit.
   * @param p Put to add to .META.
   * @throws IOException
   */
  static void putToMetaTable(final CatalogTracker ct, final Put p)
  throws IOException {
    put(MetaReader.getMetaHTable(ct), p);
  }

  /**
   * Put the passed <code>p</code> to the <code>.META.</code> table.
   * @param ct CatalogTracker on whose back we will ride the edit.
   * @param p Put to add to .META.
   * @throws IOException
   */
  static void putToRootTable(final CatalogTracker ct, final Put p)
  throws IOException {
    put(MetaReader.getRootHTable(ct), p);
  }

  /**
   * Put the passed <code>p</code> to a catalog table.
   * @param ct CatalogTracker on whose back we will ride the edit.
   * @param p Put to add
   * @throws IOException
   */
  static void putToCatalogTable(final CatalogTracker ct, final Put p)
  throws IOException {
    HTable t = MetaReader.getCatalogHTable(ct, p.getRow());
    put(t, p);
  }

  /**
   * @param t Table to use (will be closed when done).
   * @param p
   * @throws IOException
   */
  private static void put(final HTable t, final Put p) throws IOException {
    try {
      t.put(p);
    } finally {
      t.close();
    }
  }

  /**
   * Put the passed <code>ps</code> to the <code>.META.</code> table.
   * @param ct CatalogTracker on whose back we will ride the edit.
   * @param ps Put to add to .META.
   * @throws IOException
   */
  static void putsToMetaTable(final CatalogTracker ct, final List<Put> ps)
  throws IOException {
    HTable t = MetaReader.getMetaHTable(ct);
    try {
      t.put(ps);
    } finally {
      t.close();
    }
  }

  /**
   * Delete the passed <code>d</code> from the <code>.META.</code> table.
   * @param ct CatalogTracker on whose back we will ride the edit.
   * @param d Delete to add to .META.
   * @throws IOException
   */
  static void deleteMetaTable(final CatalogTracker ct, final Delete d)
  throws IOException {
    HTable t = MetaReader.getMetaHTable(ct);
    try {
      t.delete(d);
    } finally {
      t.close();
    }
  }
  
  /**
   * Adds a META row for the specified new region.
   * @param regionInfo region information
   * @throws IOException if problem connecting or updating meta
   */
  public static void addRegionToMeta(CatalogTracker catalogTracker,
      HRegionInfo regionInfo)
  throws IOException {
    putToMetaTable(catalogTracker, makePutFromRegionInfo(regionInfo));
    LOG.info("Added region " + regionInfo.getRegionNameAsString() + " to META");
  }

  /**
   * Adds a META row for each of the specified new regions.
   * @param catalogTracker CatalogTracker
   * @param regionInfos region information list
   * @throws IOException if problem connecting or updating meta
   */
  public static void addRegionsToMeta(CatalogTracker catalogTracker,
      List<HRegionInfo> regionInfos)
  throws IOException {
    List<Put> puts = new ArrayList<Put>();
    for (HRegionInfo regionInfo : regionInfos) { 
      puts.add(makePutFromRegionInfo(regionInfo));
    }
    putsToMetaTable(catalogTracker, puts);
    LOG.info("Added " + puts.size() + " regions in META");
  }

  /**
   * Offline parent in meta.
   * Used when splitting.
   * @param catalogTracker
   * @param parent
   * @param a Split daughter region A
   * @param b Split daughter region B
   * @throws NotAllMetaRegionsOnlineException
   * @throws IOException
   */
  public static void offlineParentInMeta(CatalogTracker catalogTracker,
      HRegionInfo parent, final HRegionInfo a, final HRegionInfo b)
  throws NotAllMetaRegionsOnlineException, IOException {
    HRegionInfo copyOfParent = new HRegionInfo(parent);
    copyOfParent.setOffline(true);
    copyOfParent.setSplit(true);
    Put put = new Put(copyOfParent.getRegionName());
    addRegionInfo(put, copyOfParent);
    put.add(HConstants.CATALOG_FAMILY, HConstants.SPLITA_QUALIFIER,
      Writables.getBytes(a));
    put.add(HConstants.CATALOG_FAMILY, HConstants.SPLITB_QUALIFIER,
      Writables.getBytes(b));
    putToMetaTable(catalogTracker, put);
    LOG.info("Offlined parent region " + parent.getRegionNameAsString() +
      " in META");
  }

  public static void addDaughter(final CatalogTracker catalogTracker,
      final HRegionInfo regionInfo, final ServerName sn)
  throws NotAllMetaRegionsOnlineException, IOException {
    Put put = new Put(regionInfo.getRegionName());
    addRegionInfo(put, regionInfo);
    if (sn != null) addLocation(put, sn);
    putToMetaTable(catalogTracker, put);
    LOG.info("Added daughter " + regionInfo.getRegionNameAsString() +
      (sn == null? ", serverName=null": ", serverName=" + sn.toString()));
  }

  /**
   * Updates the location of the specified META region in ROOT to be the
   * specified server hostname and startcode.
   * <p>
   * Uses passed catalog tracker to get a connection to the server hosting
   * ROOT and makes edits to that region.
   *
   * @param catalogTracker catalog tracker
   * @param regionInfo region to update location of
   * @param sn Server name
   * @throws IOException
   * @throws ConnectException Usually because the regionserver carrying .META.
   * is down.
   * @throws NullPointerException Because no -ROOT- server connection
   */
  public static void updateMetaLocation(CatalogTracker catalogTracker,
      HRegionInfo regionInfo, ServerName sn)
  throws IOException, ConnectException {
    updateLocation(catalogTracker, regionInfo, sn);
  }

  /**
   * Updates the location of the specified region in META to be the specified
   * server hostname and startcode.
   * <p>
   * Uses passed catalog tracker to get a connection to the server hosting
   * META and makes edits to that region.
   *
   * @param catalogTracker catalog tracker
   * @param regionInfo region to update location of
   * @param sn Server name
   * @throws IOException
   */
  public static void updateRegionLocation(CatalogTracker catalogTracker,
      HRegionInfo regionInfo, ServerName sn)
  throws IOException {
    updateLocation(catalogTracker, regionInfo, sn);
  }

  /**
   * Updates the location of the specified region to be the specified server.
   * <p>
   * Connects to the specified server which should be hosting the specified
   * catalog region name to perform the edit.
   *
   * @param catalogTracker
   * @param regionInfo region to update location of
   * @param sn Server name
   * @throws IOException In particular could throw {@link java.net.ConnectException}
   * if the server is down on other end.
   */
  private static void updateLocation(final CatalogTracker catalogTracker,
      HRegionInfo regionInfo, ServerName sn)
  throws IOException {
    Put put = new Put(regionInfo.getRegionName());
    addLocation(put, sn);
    putToCatalogTable(catalogTracker, put);
    LOG.info("Updated row " + regionInfo.getRegionNameAsString() +
      " with server=" + sn);
  }

  /**
   * Deletes the specified region from META.
   * @param catalogTracker
   * @param regionInfo region to be deleted from META
   * @throws IOException
   */
  public static void deleteRegion(CatalogTracker catalogTracker,
      HRegionInfo regionInfo)
  throws IOException {
    Delete delete = new Delete(regionInfo.getRegionName());
    deleteMetaTable(catalogTracker, delete);
    LOG.info("Deleted region " + regionInfo.getRegionNameAsString() + " from META");
  }

  /**
   * Deletes daughter reference in offlined split parent.
   * @param catalogTracker
   * @param parent Parent row we're to remove daughter reference from
   * @param qualifier SplitA or SplitB daughter to remove
   * @param daughter
   * @throws NotAllMetaRegionsOnlineException
   * @throws IOException
   */
  public static void deleteDaughterReferenceInParent(CatalogTracker catalogTracker,
      final HRegionInfo parent, final byte [] qualifier,
      final HRegionInfo daughter)
  throws NotAllMetaRegionsOnlineException, IOException {
    Delete delete = new Delete(parent.getRegionName());
    delete.deleteColumns(HConstants.CATALOG_FAMILY, qualifier);
    deleteMetaTable(catalogTracker, delete);
    LOG.info("Deleted daughter reference " + daughter.getRegionNameAsString() +
      ", qualifier=" + Bytes.toStringBinary(qualifier) + ", from parent " +
      parent.getRegionNameAsString());
  }

  public static HRegionInfo getHRegionInfo(
      Result data) throws IOException {
    byte [] bytes =
      data.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    if (bytes == null) return null;
    HRegionInfo info = Writables.getHRegionInfo(bytes);
    LOG.info("Current INFO from scan results = " + info);
    return info;
  }

  private static Put addRegionInfo(final Put p, final HRegionInfo hri)
  throws IOException {
    p.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(hri));
    return p;
  }

  private static Put addLocation(final Put p, final ServerName sn) {
    p.add(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
      Bytes.toBytes(sn.getHostAndPort()));
    p.add(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER,
      Bytes.toBytes(sn.getStartcode()));
    return p;
  }
}