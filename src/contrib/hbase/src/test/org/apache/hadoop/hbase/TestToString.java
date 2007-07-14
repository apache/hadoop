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

package org.apache.hadoop.hbase;

import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

public class TestToString extends TestCase {
  public void testServerInfo() throws Exception {
    final String hostport = "127.0.0.1:9999";
    HServerAddress address = new HServerAddress(hostport);
    assertEquals("HServerAddress toString", address.toString(), hostport);
    HServerInfo info = new HServerInfo(address, -1);
    assertEquals("HServerInfo", info.toString(),
        "address: " + hostport + ", startcode: " + -1);
  }
  
  public void testHRegionInfo() throws Exception {
    HTableDescriptor htd = new HTableDescriptor("hank");
    htd.addFamily(new HColumnDescriptor("hankfamily:"));
    htd.addFamily(new HColumnDescriptor(new Text("hankotherfamily:"), 10,
        HColumnDescriptor.CompressionType.BLOCK, true, 1000, null));
    assertEquals("Table descriptor", "name: hank, families: "
        + "{hankfamily:=(hankfamily:, max versions: 3, compression: none, "
        + "in memory: false, max value length: 2147483647, bloom filter: none), "
        + "hankotherfamily:=(hankotherfamily:, max versions: 10, "
        + "compression: block, in memory: true, max value length: 1000, "
        + "bloom filter: none)}", htd.toString());
    HRegionInfo hri = new HRegionInfo(-1, htd, new Text(), new Text("10"));
    System.out.println(hri.toString());
    assertEquals("HRegionInfo", 
        "regionname: hank,,-1, startKey: <>, tableDesc: {" + "name: hank, "
        + "families: {hankfamily:=(hankfamily:, max versions: 3, "
        + "compression: none, in memory: false, max value length: 2147483647, "
        + "bloom filter: none), hankotherfamily:=(hankotherfamily:, "
        + "max versions: 10, compression: block, in memory: true, max value "
        + "length: 1000, bloom filter: none)}}",
        hri.toString());
  }
}
