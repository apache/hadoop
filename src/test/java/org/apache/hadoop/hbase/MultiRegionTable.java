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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Utility class to build a table of multiple regions.
 */
public class MultiRegionTable extends HBaseClusterTestCase {
  protected static final byte [][] KEYS = {
    HConstants.EMPTY_BYTE_ARRAY,
    Bytes.toBytes("bbb"),
    Bytes.toBytes("ccc"),
    Bytes.toBytes("ddd"),
    Bytes.toBytes("eee"),
    Bytes.toBytes("fff"),
    Bytes.toBytes("ggg"),
    Bytes.toBytes("hhh"),
    Bytes.toBytes("iii"),
    Bytes.toBytes("jjj"),
    Bytes.toBytes("kkk"),
    Bytes.toBytes("lll"),
    Bytes.toBytes("mmm"),
    Bytes.toBytes("nnn"),
    Bytes.toBytes("ooo"),
    Bytes.toBytes("ppp"),
    Bytes.toBytes("qqq"),
    Bytes.toBytes("rrr"),
    Bytes.toBytes("sss"),
    Bytes.toBytes("ttt"),
    Bytes.toBytes("uuu"),
    Bytes.toBytes("vvv"),
    Bytes.toBytes("www"),
    Bytes.toBytes("xxx"),
    Bytes.toBytes("yyy")
  };

  protected final byte [] columnFamily;
  protected HTableDescriptor desc;

  /**
   * @param familyName the family to populate.
   */
  public MultiRegionTable(final String familyName) {
    this(1, familyName);
  }

  public MultiRegionTable(int nServers, final String familyName) {
    super(nServers);

     this.columnFamily = Bytes.toBytes(familyName);
    // These are needed for the new and improved Map/Reduce framework
    System.setProperty("hadoop.log.dir", conf.get("hadoop.log.dir"));
    conf.set("mapred.output.dir", conf.get("hadoop.tmp.dir"));
  }

  /**
   * Run after dfs is ready but before hbase cluster is started up.
   */
  @Override
  protected void preHBaseClusterSetup() throws Exception {
    try {
      // Create a bunch of regions
      FileSystem filesystem = FileSystem.get(conf);
      Path rootdir = filesystem.makeQualified(
          new Path(conf.get(HConstants.HBASE_DIR)));
      filesystem.mkdirs(rootdir);
      FSUtils.createTableDescriptor(fs, rootdir, desc);
      
      HRegion[] regions = new HRegion[KEYS.length];
      for (int i = 0; i < regions.length; i++) {
        int j = (i + 1) % regions.length;
        regions[i] = createARegion(KEYS[i], KEYS[j]);
      }

      // Now create the root and meta regions and insert the data regions
      // created above into the meta

      createRootAndMetaRegions();

      for(int i = 0; i < regions.length; i++) {
        HRegion.addRegionToMETA(meta, regions[i]);
      }

      closeRootAndMeta();
    } catch (Exception e) {
      shutdownDfs(dfsCluster);
      throw e;
    }
  }

  private HRegion createARegion(byte [] startKey, byte [] endKey) throws IOException {
    HRegion region = createNewHRegion(desc, startKey, endKey);
    addContent(region, this.columnFamily);
    closeRegionAndDeleteLog(region);
    return region;
  }

  private void closeRegionAndDeleteLog(HRegion region) throws IOException {
    region.close();
    region.getLog().closeAndDelete();
  }
}
