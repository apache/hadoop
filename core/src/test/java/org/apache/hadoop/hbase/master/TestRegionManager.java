/*
 * Copyright 2009 The Apache Software Foundation
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

package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class TestRegionManager extends HBaseClusterTestCase {
   public void testGetFirstMetaRegionForRegionAfterMetaSplit()
   throws Exception {
     HTable meta = new HTable(HConstants.META_TABLE_NAME);
     HMaster master = this.cluster.getMaster();
     HServerAddress address = master.getMasterAddress();
     HTableDescriptor tableDesc = new HTableDescriptor(Bytes.toBytes("_MY_TABLE_"));
     HTableDescriptor metaTableDesc = meta.getTableDescriptor();
     // master.regionManager.onlineMetaRegions already contains first .META. region at key Bytes.toBytes("")
     byte[] startKey0 = Bytes.toBytes("f");
     byte[] endKey0 = Bytes.toBytes("h");
     HRegionInfo regionInfo0 = new HRegionInfo(tableDesc, startKey0, endKey0);

     // 1st .META. region will be something like .META.,,1253625700761
     HRegionInfo metaRegionInfo0 = new HRegionInfo(metaTableDesc, Bytes.toBytes(""), regionInfo0.getRegionName());
     MetaRegion meta0 = new MetaRegion(address, metaRegionInfo0);
   
     byte[] startKey1 = Bytes.toBytes("j");
     byte[] endKey1 = Bytes.toBytes("m");
     HRegionInfo regionInfo1 = new HRegionInfo(tableDesc, startKey1, endKey1);
     // 2nd .META. region will be something like .META.,_MY_TABLE_,f,1253625700761,1253625700761 
     HRegionInfo metaRegionInfo1 = new HRegionInfo(metaTableDesc, regionInfo0.getRegionName(), regionInfo1.getRegionName());
     MetaRegion meta1 = new MetaRegion(address, metaRegionInfo1);


     // 3rd .META. region will be something like .META.,_MY_TABLE_,j,1253625700761,1253625700761
     HRegionInfo metaRegionInfo2 = new HRegionInfo(metaTableDesc, regionInfo1.getRegionName(), Bytes.toBytes(""));
     MetaRegion meta2 = new MetaRegion(address, metaRegionInfo2);

     byte[] startKeyX = Bytes.toBytes("h");
     byte[] endKeyX = Bytes.toBytes("j");
     HRegionInfo regionInfoX = new HRegionInfo(tableDesc, startKeyX, endKeyX);
   
   
     master.getRegionManager().offlineMetaRegion(startKey0);
     master.getRegionManager().putMetaRegionOnline(meta0);
     master.getRegionManager().putMetaRegionOnline(meta1);
     master.getRegionManager().putMetaRegionOnline(meta2);
   
//    for (byte[] b : master.regionManager.getOnlineMetaRegions().keySet()) {
//      System.out.println("FROM TEST KEY " + b +"  " +new String(b));
//    }

     assertEquals(metaRegionInfo1.getStartKey(),
       master.getRegionManager().getFirstMetaRegionForRegion(regionInfoX).getStartKey());
     assertEquals(metaRegionInfo1.getRegionName(),
      master.getRegionManager().getFirstMetaRegionForRegion(regionInfoX).getRegionName());
   }
}
