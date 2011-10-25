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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.catalog.MetaReader.Visitor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.migration.HRegionInfo090x;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Tools to help with migration of meta tables so they no longer host
 * instances of HTableDescriptor.
 * @deprecated Used migration from 0.90 to 0.92 so will be going away in next
 * release
 */
public class MetaMigrationRemovingHTD {
  private static final Log LOG = LogFactory.getLog(MetaMigrationRemovingHTD.class);

  /** The metaupdated column qualifier */
  public static final byte [] META_MIGRATION_QUALIFIER =
    Bytes.toBytes("metamigrated");

  /**
   * Update legacy META rows, removing HTD from HRI.
   * @param masterServices
   * @return List of table descriptors.
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
        HRegionInfo090x hrfm = MetaMigrationRemovingHTD.getHRegionInfoForMigration(r);
        if (hrfm == null) return true;
        htds.add(hrfm.getTableDesc());
        masterServices.getMasterFileSystem()
          .createTableDescriptor(hrfm.getTableDesc());
        updateHRI(masterServices.getCatalogTracker(), false, hrfm);
        return true;
      }
    };
    MetaReader.fullScan(masterServices.getCatalogTracker(), v);
    MetaMigrationRemovingHTD.updateRootWithMetaMigrationStatus(masterServices.getCatalogTracker(), true);
    return htds;
  }

  /**
   * Update the ROOT with new HRI. (HRI with no HTD)
   * @param masterServices
   * @return List of table descriptors
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
        HRegionInfo090x hrfm = MetaMigrationRemovingHTD.getHRegionInfoForMigration(r);
        if (hrfm == null) return true;
        htds.add(hrfm.getTableDesc());
        masterServices.getMasterFileSystem().createTableDescriptor(
            hrfm.getTableDesc());
        updateHRI(masterServices.getCatalogTracker(), true, hrfm);
        return true;
      }
    };
    MetaReader.fullScan(masterServices.getCatalogTracker(), v, null, true);
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
   * Update the metamigrated flag in -ROOT-.
   * @param catalogTracker
   * @param metaUpdated
   * @throws IOException
   */
  public static void updateRootWithMetaMigrationStatus(
      CatalogTracker catalogTracker, boolean metaUpdated)
  throws IOException {
    Put p = new Put(HRegionInfo.ROOT_REGIONINFO.getRegionName());
    MetaMigrationRemovingHTD.addMetaUpdateStatus(p, metaUpdated);
    MetaEditor.putToRootTable(catalogTracker, p);
    LOG.info("Updated -ROOT- row with metaMigrated status = " + metaUpdated);
  }

  static void updateHRI(final CatalogTracker ct, final boolean rootTable,
    final HRegionInfo090x hRegionInfo090x)
  throws IOException {
    HRegionInfo regionInfo = new HRegionInfo(hRegionInfo090x);
    Put p = new Put(regionInfo.getRegionName());
    p.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
      Writables.getBytes(regionInfo));
    if (rootTable) {
      MetaEditor.putToRootTable(ct, p);
    } else {
      MetaEditor.putToMetaTable(ct, p);
    }
    LOG.info("Updated region " + regionInfo + " to " +
      (rootTable? "-ROOT-": ".META."));
  }

  /**
   * @deprecated Going away in 0.94; used for migrating to 0.92 only.
   */
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

  public static List<HRegionInfo090x> fullScanMetaAndPrintHRIM(
      CatalogTracker catalogTracker)
  throws IOException {
    final List<HRegionInfo090x> regions =
      new ArrayList<HRegionInfo090x>();
    Visitor v = new Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r ==  null || r.isEmpty()) return true;
        LOG.info("fullScanMetaAndPrint1.Current Meta Result: " + r);
        HRegionInfo090x hrim = getHRegionInfoForMigration(r);
        LOG.info("fullScanMetaAndPrint.HRIM Print= " + hrim);
        regions.add(hrim);
        return true;
      }
    };
    MetaReader.fullScan(catalogTracker, v);
    return regions;
  }

  static Put addMetaUpdateStatus(final Put p, final boolean metaUpdated) {
    p.add(HConstants.CATALOG_FAMILY,
      MetaMigrationRemovingHTD.META_MIGRATION_QUALIFIER,
      Bytes.toBytes(metaUpdated));
    return p;
  }

  /**
   * @return True if the meta table has been migrated.
   * @throws IOException
   */
  // Public because used in tests
  public static boolean isMetaHRIUpdated(final MasterServices services)
      throws IOException {
    boolean metaUpdated = false;
    List<Result> results =
      MetaReader.fullScanOfRoot(services.getCatalogTracker());
    if (results == null || results.isEmpty()) {
      LOG.info("metaUpdated = NULL.");
      return metaUpdated;
    }
    // Presume only the one result.
    Result r = results.get(0);
    byte [] metaMigrated = r.getValue(HConstants.CATALOG_FAMILY,
      MetaMigrationRemovingHTD.META_MIGRATION_QUALIFIER);
    if (metaMigrated != null && metaMigrated.length > 0) {
      metaUpdated = Bytes.toBoolean(metaMigrated);
    }
    LOG.info("Meta updated status = " + metaUpdated);
    return metaUpdated;
  }

  /**
   * @return True if migrated.
   * @throws IOException
   */
  public static boolean updateMetaWithNewHRI(final MasterServices services)
  throws IOException {
    if (isMetaHRIUpdated(services)) {
      LOG.info("ROOT/Meta already up-to date with new HRI.");
      return true;
    }
    LOG.info("Meta has HRI with HTDs. Updating meta now.");
    try {
      migrateRootAndMeta(services);
      LOG.info("ROOT and Meta updated with new HRI.");
      return true;
    } catch (IOException e) {
      throw new RuntimeException("Update ROOT/Meta with new HRI failed." +
        "Master startup aborted.");
    }
  }
}
