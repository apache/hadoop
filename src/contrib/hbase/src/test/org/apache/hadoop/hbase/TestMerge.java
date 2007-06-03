/**
 * Copyright 2007 The Apache Software Foundation
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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Random;

import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/** Tests region merging */
public class TestMerge extends HBaseTestCase {
  private static final Text COLUMN_NAME = new Text("contents:");
  private Random rand;
  private HTableDescriptor desc;
  private BytesWritable value;

  private MiniDFSCluster dfsCluster;
  private FileSystem fs;
  private Path dir;

  private MiniHBaseCluster hCluster;
  
  public void testMerge() {
    setup();
    startMiniDFSCluster();
    createRegions();
    try {
      HMerge.merge(conf, fs, HConstants.META_TABLE_NAME);
      
      hCluster = new MiniHBaseCluster(conf, 1, dfsCluster);
      try {
        HMerge.merge(conf, fs, desc.getName());
      
      } finally {
        hCluster.shutdown();
      }
      
    } catch(Throwable t) {
      t.printStackTrace();
      fail();
      
    } finally {
      dfsCluster.shutdown();
    }
  }
  
  private void setup() {
    rand = new Random();
    desc = new HTableDescriptor("test");
    desc.addFamily(new HColumnDescriptor(COLUMN_NAME.toString()));
    
    // We will use the same value for the rows as that is not really important here
    
    String partialValue = String.valueOf(System.currentTimeMillis());
    StringBuilder val = new StringBuilder();
    while(val.length() < 1024) {
      val.append(partialValue);
    }
    try {
      value = new BytesWritable(val.toString().getBytes(HConstants.UTF8_ENCODING));
      
    } catch(UnsupportedEncodingException e) {
      fail();
    }
  }

  private void startMiniDFSCluster() {
    try {
      dfsCluster = new MiniDFSCluster(conf, 2, true, (String[])null);
      fs = dfsCluster.getFileSystem();
      dir = new Path("/hbase");
      fs.mkdirs(dir);
      
    } catch(Throwable t) {
      t.printStackTrace();
      fail();
    }
  }
  
  private void createRegions() {
    // We create three data regions: The first is too large to merge since it 
    // will be > 64 MB in size. The second two will be smaller and will be 
    // selected for merging.
    
    // To ensure that the first region is larger than 64MB we need to write at
    // least 65536 rows. We will make certain by writing 70000

    try {
      Text row_70001 = new Text("row_70001");
      Text row_80001 = new Text("row_80001");
      
      HRegion[] regions = {
        createAregion(null, row_70001, 1, 70000),
        createAregion(row_70001, row_80001, 70001, 10000),
        createAregion(row_80001, null, 80001, 10000)
      };
      
      // Now create the root and meta regions and insert the data regions
      // created above into the meta
      
      HRegion root = HRegion.createNewHRegion(fs, dir, conf, 
          HGlobals.rootTableDesc, 0L, null, null);
      HRegion meta = HRegion.createNewHRegion(fs, dir, conf,
          HGlobals.metaTableDesc, 1L, null, null);
    
      HRegion.addRegionToMeta(root, meta);
      
      for(int i = 0; i < regions.length; i++) {
        HRegion.addRegionToMeta(meta, regions[i]);
      }
      
      root.close();
      root.getLog().close();
      fs.delete(new Path(root.getRegionDir(), HConstants.HREGION_LOGDIR_NAME));
      meta.close();
      meta.getLog().close();
      fs.delete(new Path(meta.getRegionDir(), HConstants.HREGION_LOGDIR_NAME));
      
    } catch(Throwable t) {
      t.printStackTrace();
      fail();
    }
  }
  
  private HRegion createAregion(Text startKey, Text endKey, int firstRow, int nrows)
      throws IOException {
    HRegion region = HRegion.createNewHRegion(fs, dir, conf, desc,
        rand.nextLong(), startKey, endKey);
    
    System.out.println("created region " + region.getRegionName());

    for(int i = firstRow; i < firstRow + nrows; i++) {
      long lockid = region.startUpdate(new Text("row_"
          + String.format("%1$05d", i)));

      region.put(lockid, COLUMN_NAME, value);
      region.commit(lockid);
      if(i % 10000 == 0) {
        System.out.println("Flushing write #" + i);
        region.flushcache(false);
      }
    }
    System.out.println("Rolling log...");
    region.log.rollWriter();
    region.compactStores();
    region.close();
    region.getLog().close();
    fs.delete(new Path(region.getRegionDir(), HConstants.HREGION_LOGDIR_NAME));
    region.getRegionInfo().offLine = true;
    return region;
  }
}
