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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

  /**
   * Update legacy META rows, removing HTD from HRI.
   * @param masterServices
   * @return List of table descriptors.
   * @throws IOException
   */
  public static Set<HTableDescriptor> updateMetaWithNewRegionInfo(
      final MasterServices masterServices)
  throws IOException {
    MigratingVisitor v = new MigratingVisitor(masterServices);
    MetaReader.fullScan(masterServices.getCatalogTracker(), v);
    updateRootWithMetaMigrationStatus(masterServices.getCatalogTracker());
    return v.htds;
  }

  /**
   * Update the ROOT with new HRI. (HRI with no HTD)
   * @param masterServices
   * @return List of table descriptors
   * @throws IOException
   */
  static Set<HTableDescriptor> updateRootWithNewRegionInfo(
      final MasterServices masterServices)
  throws IOException {
    MigratingVisitor v = new MigratingVisitor(masterServices);
    MetaReader.fullScan(masterServices.getCatalogTracker(), v, null, true);
    return v.htds;
  }

  /**
   * Meta visitor that migrates the info:regioninfo as it visits.
   */
  static class MigratingVisitor implements Visitor {
    private final MasterServices services;
    final Set<HTableDescriptor> htds = new HashSet<HTableDescriptor>();

    MigratingVisitor(final MasterServices services) {
      this.services = services;
    }

    @Override
    public boolean visit(Result r) throws IOException {
      if (r ==  null || r.isEmpty()) return true;
      // Check info:regioninfo, info:splitA, and info:splitB.  Make sure all
      // have migrated HRegionInfos... that there are no leftover 090 version
      // HRegionInfos.
      byte [] hriBytes = getBytes(r, HConstants.REGIONINFO_QUALIFIER);
      // Presumes that an edit updating all three cells either succeeds or
      // doesn't -- that we don't have case of info:regioninfo migrated but not
      // info:splitA.
      if (isMigrated(hriBytes)) return true;
      // OK. Need to migrate this row in meta.
      HRegionInfo090x hri090 = getHRegionInfo090x(hriBytes);
      HTableDescriptor htd = hri090.getTableDesc();
      if (htd == null) {
        LOG.warn("A 090 HRI has null HTD? Continuing; " + hri090.toString());
        return true;
      }
      if (!this.htds.contains(htd)) {
        // If first time we are adding a table, then write it out to fs.
        // Presumes that first region in table has THE table's schema which
        // might not be too bad of a presumption since it'll be first region
        // 'altered'
        this.services.getMasterFileSystem().createTableDescriptor(htd);
        this.htds.add(htd);
      }
      // This will 'migrate' the hregioninfo from 090 version to 092.
      HRegionInfo hri = new HRegionInfo(hri090);
      // Now make a put to write back to meta.
      Put p = new Put(hri.getRegionName());
      p.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(hri));
      // Now check info:splitA and info:splitB if present.  Migrate these too.
      checkSplit(r, p, HConstants.SPLITA_QUALIFIER);
      checkSplit(r, p, HConstants.SPLITB_QUALIFIER);
      // Below we fake out putToCatalogTable
      MetaEditor.putToCatalogTable(this.services.getCatalogTracker(), p);
      LOG.info("Migrated " + Bytes.toString(p.getRow()));
      return true;
    }
  }

  static void checkSplit(final Result r, final Put p, final byte [] which)
  throws IOException {
    byte [] hriSplitBytes = getBytes(r, which);
    if (!isMigrated(hriSplitBytes)) {
      // This will convert the HRI from 090 to 092 HRI.
      HRegionInfo hri = Writables.getHRegionInfo(hriSplitBytes);
      p.add(HConstants.CATALOG_FAMILY, which, Writables.getBytes(hri));
    }
  }

  /**
   * @param r Result to dig in.
   * @param qualifier Qualifier to look at in the passed <code>r</code>.
   * @return Bytes for an HRegionInfo or null if no bytes or empty bytes found.
   */
  static byte [] getBytes(final Result r, final byte [] qualifier) {
    byte [] hriBytes = r.getValue(HConstants.CATALOG_FAMILY, qualifier);
    if (hriBytes == null || hriBytes.length <= 0) return null;
    return hriBytes;
  }

  /**
   * @param r Result to look in.
   * @param qualifier What to look at in the passed result.
   * @return Either a 090 vintage HRegionInfo OR null if no HRegionInfo or
   * the HRegionInfo is up to date and not in need of migration.
   * @throws IOException
   */
  static HRegionInfo090x get090HRI(final Result r, final byte [] qualifier)
  throws IOException {
    byte [] hriBytes = r.getValue(HConstants.CATALOG_FAMILY, qualifier);
    if (hriBytes == null || hriBytes.length <= 0) return null;
    if (isMigrated(hriBytes)) return null;
    return getHRegionInfo090x(hriBytes);
  }

  static boolean isMigrated(final byte [] hriBytes) {
    if (hriBytes == null || hriBytes.length <= 0) return true;
    // Else, what version this HRegionInfo instance is at.  The first byte
    // is the version byte in a serialized HRegionInfo.  If its same as our
    // current HRI, then nothing to do.
    if (hriBytes[0] == HRegionInfo.VERSION) return true;
    if (hriBytes[0] == HRegionInfo.VERSION_PRE_092) return false;
    // Unknown version.  Return true that its 'migrated' but log warning.
    // Should 'never' happen.
    assert false: "Unexpected version; bytes=" + Bytes.toStringBinary(hriBytes);
    return true;
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
   * Update the version flag in -ROOT-.
   * @param catalogTracker
   * @throws IOException
   */
  public static void updateRootWithMetaMigrationStatus(final CatalogTracker catalogTracker)
  throws IOException {
    Put p = new Put(HRegionInfo.FIRST_META_REGIONINFO.getRegionName());
    MetaEditor.putToRootTable(catalogTracker, setMetaVersion(p));
    LOG.info("Updated -ROOT- meta version=" + HConstants.META_VERSION);
  }

  static Put setMetaVersion(final Put p) {
    p.add(HConstants.CATALOG_FAMILY, HConstants.META_VERSION_QUALIFIER,
      Bytes.toBytes(HConstants.META_VERSION));
    return p;
  }

  /**
   * @return True if the meta table has been migrated.
   * @throws IOException
   */
  // Public because used in tests
  public static boolean isMetaHRIUpdated(final MasterServices services)
      throws IOException {
    List<Result> results = MetaReader.fullScanOfRoot(services.getCatalogTracker());
    if (results == null || results.isEmpty()) {
      LOG.info("Not migrated");
      return false;
    }
    // Presume only the one result because we only support on meta region.
    Result r = results.get(0);
    short version = getMetaVersion(r);
    boolean migrated = version >= HConstants.META_VERSION;
    LOG.info("Meta version=" + version + "; migrated=" + migrated);
    return migrated;
  }

  /**
   * @param r Result to look at
   * @return Current meta table version or -1 if no version found.
   */
  static short getMetaVersion(final Result r) {
    byte [] value = r.getValue(HConstants.CATALOG_FAMILY,
        HConstants.META_VERSION_QUALIFIER);
    return value == null || value.length <= 0? -1: Bytes.toShort(value);
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

  /**
   * Get HREgionInfoForMigration serialized from bytes.
   * @param bytes serialized bytes
   * @return An instance of a 090 HRI or null if we failed deserialize
   */
  public static HRegionInfo090x getHRegionInfo090x(final byte [] bytes) {
    if (bytes == null || bytes.length == 0) return null;
    HRegionInfo090x hri = null;
    try {
      hri = (HRegionInfo090x)Writables.getWritable(bytes, new HRegionInfo090x());
    } catch (IOException ioe) {
      LOG.warn("Failed deserialize as a 090 HRegionInfo); bytes=" +
        Bytes.toStringBinary(bytes), ioe);
    }
    return hri;
  }
}
