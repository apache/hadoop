/*
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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestResettingCounters {

  @Test
  public void testResettingCounters() throws Exception {

    HBaseTestingUtility htu = new HBaseTestingUtility();
    Configuration conf = htu.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    byte [] table = Bytes.toBytes("table");
    byte [][] families = new byte [][] {
        Bytes.toBytes("family1"),
        Bytes.toBytes("family2"),
        Bytes.toBytes("family3")
    };
    int numQualifiers = 10;
    byte [][] qualifiers = new byte [numQualifiers][];
    for (int i=0; i<numQualifiers; i++) qualifiers[i] = Bytes.toBytes("qf" + i);
    int numRows = 10;
    byte [][] rows = new byte [numRows][];
    for (int i=0; i<numRows; i++) rows[i] = Bytes.toBytes("r" + i);

    HTableDescriptor htd = new HTableDescriptor(table);
    for (byte [] family : families) htd.addFamily(new HColumnDescriptor(family));

    HRegionInfo hri = new HRegionInfo(htd.getName(), null, null, false);
    String testDir = HBaseTestingUtility.getTestDir() + "/TestResettingCounters/";
    Path path = new Path(testDir);
    if (fs.exists(path)) {
      if (!fs.delete(path, true)) {
        throw new IOException("Failed delete of " + path);
      }
    }
    HRegion region = HRegion.createHRegion(hri, path, conf, htd);

    Increment odd = new Increment(rows[0]);
    Increment even = new Increment(rows[0]);
    Increment all = new Increment(rows[0]);
    for (int i=0;i<numQualifiers;i++) {
      if (i % 2 == 0) even.addColumn(families[0], qualifiers[i], 1);
      else odd.addColumn(families[0], qualifiers[i], 1);
      all.addColumn(families[0], qualifiers[i], 1);
    }

    // increment odd qualifiers 5 times and flush
    for (int i=0;i<5;i++) region.increment(odd, null, false);
    region.flushcache();

    // increment even qualifiers 5 times
    for (int i=0;i<5;i++) region.increment(even, null, false);

    // increment all qualifiers, should have value=6 for all
    Result result = region.increment(all, null, false);
    assertEquals(numQualifiers, result.size());
    KeyValue [] kvs = result.raw();
    for (int i=0;i<kvs.length;i++) {
      System.out.println(kvs[i].toString());
      assertTrue(Bytes.equals(kvs[i].getQualifier(), qualifiers[i]));
      assertEquals(6, Bytes.toLong(kvs[i].getValue()));
    }
  }
}
