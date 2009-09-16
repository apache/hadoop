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
import java.io.UnsupportedEncodingException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

/** Abstract base class for merge tests */
public abstract class AbstractMergeTestBase extends HBaseClusterTestCase {
  static final Log LOG =
    LogFactory.getLog(AbstractMergeTestBase.class.getName());
  static final byte [] COLUMN_NAME = Bytes.toBytes("contents");
  protected final Random rand = new Random();
  protected HTableDescriptor desc;
  protected ImmutableBytesWritable value;
  protected boolean startMiniHBase;
  
  public AbstractMergeTestBase() {
    this(true);
  }
  
  /** constructor 
   * @param startMiniHBase
   */
  public AbstractMergeTestBase(boolean startMiniHBase) {
    super();
    
    this.startMiniHBase = startMiniHBase;
    
    // We will use the same value for the rows as that is not really important here
    
    String partialValue = String.valueOf(System.currentTimeMillis());
    StringBuilder val = new StringBuilder();
    while(val.length() < 1024) {
      val.append(partialValue);
    }
 
    try {
      value = new ImmutableBytesWritable(
          val.toString().getBytes(HConstants.UTF8_ENCODING));
    } catch (UnsupportedEncodingException e) {
      fail();
    }
    desc = new HTableDescriptor(Bytes.toBytes("test"));
    desc.addFamily(new HColumnDescriptor(COLUMN_NAME));
  }

  @Override
  protected void hBaseClusterSetup() throws Exception {
    if (startMiniHBase) {
      super.hBaseClusterSetup();
    }
  }

  @Override
  public void preHBaseClusterSetup() throws Exception {
    conf.setLong("hbase.hregion.max.filesize", 64L * 1024L * 1024L);

    // We create three data regions: The first is too large to merge since it 
    // will be > 64 MB in size. The second two will be smaller and will be 
    // selected for merging.
    
    // To ensure that the first region is larger than 64MB we need to write at
    // least 65536 rows. We will make certain by writing 70000

    byte [] row_70001 = Bytes.toBytes("row_70001");
    byte [] row_80001 = Bytes.toBytes("row_80001");

    // XXX: Note that the number of rows we put in is different for each region
    // because currently we don't have a good mechanism to handle merging two
    // store files with the same sequence id. We can't just dumbly stick them
    // in because it will screw up the order when the store files are loaded up.
    // The sequence ids are used for arranging the store files, so if two files
    // have the same id, one will overwrite the other one in our listing, which
    // is very bad. See HBASE-1212 and HBASE-1274.
    HRegion[] regions = {
      createAregion(null, row_70001, 1, 70000),
      createAregion(row_70001, row_80001, 70001, 10000),
      createAregion(row_80001, null, 80001, 11000)
    };
    
    // Now create the root and meta regions and insert the data regions
    // created above into the meta

    createRootAndMetaRegions();
    
    for(int i = 0; i < regions.length; i++) {
      HRegion.addRegionToMETA(meta, regions[i]);
    }

    closeRootAndMeta();
  }

  private HRegion createAregion(byte [] startKey, byte [] endKey, int firstRow,
      int nrows) throws IOException {
    
    HRegion region = createNewHRegion(desc, startKey, endKey);
    
    System.out.println("created region " +
        Bytes.toString(region.getRegionName()));

    HRegionIncommon r = new HRegionIncommon(region);
    for(int i = firstRow; i < firstRow + nrows; i++) {
      Put put = new Put(Bytes.toBytes("row_"
          + String.format("%1$05d", i)));
      put.add(COLUMN_NAME, null,  value.get());
      region.put(put);
      if(i % 10000 == 0) {
        System.out.println("Flushing write #" + i);
        r.flushcache();
      }
    }
    region.close();
    region.getLog().closeAndDelete();
    region.getRegionInfo().setOffline(true);
    return region;
  }
}
