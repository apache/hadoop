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

import java.util.ArrayList;
import java.util.List;

import java.io.IOException;
import java.net.ConnectException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.migration.HRegionInfo090x;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.catalog.MetaReader.Visitor;

/**
 * Writes region and assignment information to <code>.META.</code>.
 * <p>
 * Uses the {@link CatalogTracker} to obtain locations and connections to
 * catalogs.
 */
public class MetaEditor {
  private static final Log LOG = LogFactory.getLog(MetaEditor.class);

  private static Put makePutFromRegionInfo(HRegionInfo regionInfo) throws IOException {
    Put put = new Put(regionInfo.getRegionName());
    put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(regionInfo));
    return put;
  }
  
  /**
   * Adds a META row for the specified new region.
   * @param regionInfo region information
   * @throws IOException if problem connecting or updating meta
   */
  public static void addRegionToMeta(CatalogTracker catalogTracker,
      HRegionInfo regionInfo)
  throws IOException {
    catalogTracker.waitForMetaServerConnectionDefault().put(
        CatalogTracker.META_REGION, makePutFromRegionInfo(regionInfo));
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
      LOG.debug("Added region " + regionInfo.getRegionNameAsString() + " to META");
    }
    catalogTracker.waitForMetaServerConnectionDefault().put(
        CatalogTracker.META_REGION, puts);
    LOG.info("Added " + puts.size() + " regions to META");
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
    catalogTracker.waitForMetaServerConnectionDefault().put(CatalogTracker.META_REGION, put);
    LOG.info("Offlined parent region " + parent.getRegionNameAsString() +
      " in META");
  }

  public static void addDaughter(final CatalogTracker catalogTracker,
      final HRegionInfo regionInfo, final ServerName sn)
  throws NotAllMetaRegionsOnlineException, IOException {
    HRegionInterface server = catalogTracker.waitForMetaServerConnectionDefault();
    byte [] catalogRegionName = CatalogTracker.META_REGION;
    Put put = new Put(regionInfo.getRegionName());
    addRegionInfo(put, regionInfo);
    if (sn != null) addLocation(put, sn);
    server.put(catalogRegionName, put);
    LOG.info("Added daughter " + regionInfo.getRegionNameAsString() +
      " in region " + Bytes.toString(catalogRegionName) +
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
    HRegionInterface server = catalogTracker.waitForRootServerConnectionDefault();
    if (server == null) throw new IOException("No server for -ROOT-");
    updateLocation(server, CatalogTracker.ROOT_REGION, regionInfo, sn);
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
    updateLocation(catalogTracker.waitForMetaServerConnectionDefault(),
        CatalogTracker.META_REGION, regionInfo, sn);
  }

  /**
   * Updates the location of the specified region to be the specified server.
   * <p>
   * Connects to the specified server which should be hosting the specified
   * catalog region name to perform the edit.
   *
   * @param server connection to server hosting catalog region
   * @param catalogRegionName name of catalog region being updated
   * @param regionInfo region to update location of
   * @param sn Server name
   * @throws IOException In particular could throw {@link java.net.ConnectException}
   * if the server is down on other end.
   */
  private static void updateLocation(HRegionInterface server,
      byte [] catalogRegionName, HRegionInfo regionInfo, ServerName sn)
  throws IOException {
    Put put = new Put(regionInfo.getRegionName());
    addLocation(put, sn);
    server.put(catalogRegionName, put);
    LOG.info("Updated row " + regionInfo.getRegionNameAsString() +
      " in region " + Bytes.toStringBinary(catalogRegionName) + " with " +
      "serverName=" + sn.toString());
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
    catalogTracker.waitForMetaServerConnectionDefault().
      delete(CatalogTracker.META_REGION, delete);
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
    catalogTracker.waitForMetaServerConnectionDefault().
      delete(CatalogTracker.META_REGION, delete);
    LOG.info("Deleted daughter reference " + daughter.getRegionNameAsString() +
      ", qualifier=" + Bytes.toStringBinary(qualifier) + ", from parent " +
      parent.getRegionNameAsString());
  }

  /**
   * Update the metamigrated flag in -ROOT-.
   * @param catalogTracker
   * @throws IOException
   */
  public static void updateRootWithMetaMigrationStatus(
      CatalogTracker catalogTracker) throws IOException {
    updateRootWithMetaMigrationStatus(catalogTracker, true);
  }

  /**
   * Update the metamigrated flag in -ROOT-.
   * @param catalogTracker
   * @param metaUpdated
   * @throws IOException
   */
  public static void updateRootWithMetaMigrationStatus(
      CatalogTracker catalogTracker, boolean metaUpdated)
      throws IOException {
    Put put = new Put(HRegionInfo.ROOT_REGIONINFO.getRegionName());
    addMetaUpdateStatus(put, metaUpdated);
    catalogTracker.waitForRootServerConnectionDefault().put(
        CatalogTracker.ROOT_REGION, put);
    LOG.info("Updated -ROOT- row with metaMigrated status = " + metaUpdated);
  }

  /**
   * Update legacy META rows, removing HTD from HRI.
   * @param masterServices
   * @return
   * @throws IOException
   */
  public static List<HTableDescriptor> updateMetaWithNewRegionInfo(
      final MasterServices masterServices)
  throws IOException {
    final List<HTableDescriptor> htds = new ArrayList<HTableDescriptor>();
    Visitor v = new Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r ==  null || r.isEmpty()) return true;
        HRegionInfo090x hrfm = getHRegionInfoForMigration(r);
        if (hrfm == null) return true;
        htds.add(hrfm.getTableDesc());
        masterServices.getMasterFileSystem()
          .createTableDescriptor(hrfm.getTableDesc());
        updateHRI(masterServices.getCatalogTracker()
            .waitForMetaServerConnectionDefault(),
            hrfm, CatalogTracker.META_REGION);
        return true;
      }
    };
    MetaReader.fullScan(masterServices.getCatalogTracker(), v);
    updateRootWithMetaMigrationStatus(masterServices.getCatalogTracker());
    return htds;
  }

  /**
   * Migrate root and meta to newer version. This updates the META and ROOT
   * and removes the HTD from HRI.
   * @param masterServices
   * @throws IOException
   */
  public static void migrateRootAndMeta(final MasterServices masterServices)
      throws IOException {
    updateRootWithNewRegionInfo(masterServices);
    updateMetaWithNewRegionInfo(masterServices);
  }

  /**
   * Update the ROOT with new HRI. (HRI with no HTD)
   * @param masterServices
   * @return
   * @throws IOException
   */
  public static List<HTableDescriptor> updateRootWithNewRegionInfo(
      final MasterServices masterServices)
  throws IOException {
    final List<HTableDescriptor> htds = new ArrayList<HTableDescriptor>();
    Visitor v = new Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r ==  null || r.isEmpty()) return true;
        HRegionInfo090x hrfm = getHRegionInfoForMigration(r);
        if (hrfm == null) return true;
        htds.add(hrfm.getTableDesc());
        masterServices.getMasterFileSystem().createTableDescriptor(
            hrfm.getTableDesc());
        updateHRI(masterServices.getCatalogTracker()
            .waitForRootServerConnectionDefault(),
            hrfm, CatalogTracker.ROOT_REGION);
        return true;
      }
    };
    MetaReader.fullScan(
        masterServices.getCatalogTracker().waitForRootServerConnectionDefault(),
        v, HRegionInfo.ROOT_REGIONINFO.getRegionName(), null);
    return htds;
  }

  private static void updateHRI(HRegionInterface hRegionInterface,
                                HRegionInfo090x hRegionInfo090x, byte[] regionName)
    throws IOException {
    HRegionInfo regionInfo = new HRegionInfo(hRegionInfo090x);
    Put put = new Put(regionInfo.getRegionName());
    put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(regionInfo));
    hRegionInterface.put(regionName, put);
    LOG.info("Updated region " + regionInfo + " to " + Bytes.toString(regionName));
  }

  public static HRegionInfo090x getHRegionInfoForMigration(
      Result data) throws IOException {
    HRegionInfo090x info = null;
    byte [] bytes =
      data.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    if (bytes == null) return null;
    try {
      info = Writables.getHRegionInfoForMigration(bytes);
    } catch(IOException ioe) {
      if (ioe.getMessage().equalsIgnoreCase("HTD not found in input buffer")) {
         return null;
      } else {
        throw ioe;
      }
    }
    LOG.info("Current INFO from scan results = " + info);
    return info;
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

  private static Put addMetaUpdateStatus(final Put p) {
    p.add(HConstants.CATALOG_FAMILY, HConstants.META_MIGRATION_QUALIFIER,
      Bytes.toBytes("true"));
    return p;
  }


  private static Put addMetaUpdateStatus(final Put p, final boolean metaUpdated) {
    p.add(HConstants.CATALOG_FAMILY, HConstants.META_MIGRATION_QUALIFIER,
      Bytes.toBytes(metaUpdated));
    return p;
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
