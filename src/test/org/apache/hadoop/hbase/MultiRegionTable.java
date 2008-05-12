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

import org.apache.hadoop.io.Text;

import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * Utility class to build a table of multiple regions.
 */
public class MultiRegionTable extends HBaseClusterTestCase {
  private static final Text[] KEYS = {
    null,
    new Text("bbb"),
    new Text("ccc"),
    new Text("ddd"),
    new Text("eee"),
    new Text("fff"),
    new Text("ggg"),
    new Text("hhh"),
    new Text("iii"),
    new Text("jjj"),
    new Text("kkk"),
    new Text("lll"),
    new Text("mmm"),
    new Text("nnn"),
    new Text("ooo"),
    new Text("ppp"),
    new Text("qqq"),
    new Text("rrr"),
    new Text("sss"),
    new Text("ttt"),
    new Text("uuu"),
    new Text("vvv"),
    new Text("www"),
    new Text("xxx"),
    new Text("yyy")
  };
  
  protected final String columnName;
  protected HTableDescriptor desc;

  /**
   * @param columnName the column to populate.
   */
  public MultiRegionTable(final String columnName) {
    super();
    this.columnName = columnName;
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

      HRegion[] regions = new HRegion[KEYS.length];
      for (int i = 0; i < regions.length; i++) {
        int j = (i + 1) % regions.length;
        regions[i] = createARegion(KEYS[i], KEYS[j]);
      }

      // Now create the root and meta regions and insert the data regions
      // created above into the meta

      HRegion root = HRegion.createHRegion(HRegionInfo.rootRegionInfo,
          testDir, this.conf);
      HRegion meta = HRegion.createHRegion(HRegionInfo.firstMetaRegionInfo,
          testDir, this.conf);
      HRegion.addRegionToMETA(root, meta);

      for(int i = 0; i < regions.length; i++) {
        HRegion.addRegionToMETA(meta, regions[i]);
      }

      closeRegionAndDeleteLog(root);
      closeRegionAndDeleteLog(meta);
    } catch (Exception e) {
      shutdownDfs(dfsCluster);
      throw e;
    }
  } 

  private HRegion createARegion(Text startKey, Text endKey) throws IOException {
    HRegion region = createNewHRegion(desc, startKey, endKey);
    addContent(region, this.columnName);
    closeRegionAndDeleteLog(region);
    return region;
  }
  
  private void closeRegionAndDeleteLog(HRegion region) throws IOException {
    region.close();
    region.getLog().closeAndDelete();
  }
}