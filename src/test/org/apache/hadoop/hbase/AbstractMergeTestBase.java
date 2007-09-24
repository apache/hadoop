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
import java.util.Random;

import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/** Abstract base class for merge tests */
public abstract class AbstractMergeTestBase extends HBaseTestCase {
  static final Logger LOG =
    Logger.getLogger(AbstractMergeTestBase.class.getName());
  protected static final Text COLUMN_NAME = new Text("contents:");
  protected final Random rand = new Random();
  protected HTableDescriptor desc;
  protected ImmutableBytesWritable value;

  protected MiniDFSCluster dfsCluster = null;
  protected FileSystem fs;
  protected Path dir;

  /**
   * {@inheritDoc}
   */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    desc = new HTableDescriptor("test");
    desc.addFamily(new HColumnDescriptor(COLUMN_NAME.toString()));
    
    // We will use the same value for the rows as that is not really important here
    
    String partialValue = String.valueOf(System.currentTimeMillis());
    StringBuilder val = new StringBuilder();
    while(val.length() < 1024) {
      val.append(partialValue);
    }
    
    value = new ImmutableBytesWritable(
        val.toString().getBytes(HConstants.UTF8_ENCODING));

    dfsCluster = new MiniDFSCluster(conf, 2, true, (String[])null);
      
    // We create three data regions: The first is too large to merge since it 
    // will be > 64 MB in size. The second two will be smaller and will be 
    // selected for merging.
    
    // To ensure that the first region is larger than 64MB we need to write at
    // least 65536 rows. We will make certain by writing 70000

    try {
      fs = dfsCluster.getFileSystem();
      dir = new Path("/hbase");
      fs.mkdirs(dir);

      Text row_70001 = new Text("row_70001");
      Text row_80001 = new Text("row_80001");
      
      HRegion[] regions = {
        createAregion(null, row_70001, 1, 70000),
        createAregion(row_70001, row_80001, 70001, 10000),
        createAregion(row_80001, null, 80001, 10000)
      };
      
      // Now create the root and meta regions and insert the data regions
      // created above into the meta
      
      HRegion root =
        createNewHRegion(dir, conf, HGlobals.rootTableDesc, 0L, null, null);
      
      HRegion meta =
        createNewHRegion(dir, conf, HGlobals.metaTableDesc, 1L, null, null);
    
      HRegion.addRegionToMETA(root, meta);
      
      for(int i = 0; i < regions.length; i++) {
        HRegion.addRegionToMETA(meta, regions[i]);
      }
      
      root.close();
      root.getLog().closeAndDelete();
      meta.close();
      meta.getLog().closeAndDelete();
      
    } catch (Exception e) {
      if(dfsCluster != null) {
        dfsCluster.shutdown();
      }
      throw e;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
    if (this.fs != null) {
      try {
        this.fs.close();
      } catch (IOException e) {
        LOG.info("During tear down got a " + e.getMessage());
      }
    }
  }

  private HRegion createAregion(Text startKey, Text endKey, int firstRow,
      int nrows) throws IOException {
    
    HRegion region =
      createNewHRegion(dir, conf, desc, rand.nextLong(), startKey, endKey);
    
    System.out.println("created region " + region.getRegionName());

    for(int i = firstRow; i < firstRow + nrows; i++) {
      long lockid = region.startUpdate(new Text("row_"
          + String.format("%1$05d", i)));

      region.put(lockid, COLUMN_NAME, value.get());
      region.commit(lockid, System.currentTimeMillis());
      if(i % 10000 == 0) {
        System.out.println("Flushing write #" + i);
        region.flushcache(false);
      }
    }
    System.out.println("Rolling log...");
    region.log.rollWriter();
    region.compactStores();
    region.close();
    region.getLog().closeAndDelete();
    region.getRegionInfo().offLine = true;
    return region;
  }
}
