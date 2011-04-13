/**
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

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.Coprocessor.Priority;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import junit.framework.TestCase;

public class TestRegionObserverStacking extends TestCase {
  static final String DIR = "test/build/data/TestRegionObserverStacking/";

  public static class ObserverA extends BaseRegionObserverCoprocessor {
    long id;
    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> c,
        final Map<byte[], List<KeyValue>> familyMap, final boolean writeToWAL)
        throws IOException {
      id = System.currentTimeMillis();
      try {
        Thread.sleep(10);
      } catch (InterruptedException ex) {
      }
    }
  }

  public static class ObserverB extends BaseRegionObserverCoprocessor {
    long id;
    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> c,
        final Map<byte[], List<KeyValue>> familyMap, final boolean writeToWAL)
        throws IOException {
      id = System.currentTimeMillis();
      try {
        Thread.sleep(10);
      } catch (InterruptedException ex) {
      }
    }
  }

  public static class ObserverC extends BaseRegionObserverCoprocessor {
    long id;

    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> c,
        final Map<byte[], List<KeyValue>> familyMap, final boolean writeToWAL)
        throws IOException {
      id = System.currentTimeMillis();
      try {
        Thread.sleep(10);
      } catch (InterruptedException ex) {
      }
    }
  }

  HRegion initHRegion (byte [] tableName, String callingMethod,
      Configuration conf, byte [] ... families) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for(byte [] family : families) {
      htd.addFamily(new HColumnDescriptor(family));
    }
    HRegionInfo info = new HRegionInfo(htd, null, null, false);
    Path path = new Path(DIR + callingMethod);
    HRegion r = HRegion.createHRegion(info, path, conf);
    // this following piece is a hack. currently a coprocessorHost
    // is secretly loaded at OpenRegionHandler. we don't really
    // start a region server here, so just manually create cphost
    // and set it to region.
    RegionCoprocessorHost host = new RegionCoprocessorHost(r, null, conf);
    r.setCoprocessorHost(host);
    return r;
  }

  public void testRegionObserverStacking() throws Exception {
    byte[] ROW = Bytes.toBytes("testRow");
    byte[] TABLE = Bytes.toBytes(getClass().getName());
    byte[] A = Bytes.toBytes("A");
    byte[][] FAMILIES = new byte[][] { A } ;

    HRegion region = initHRegion(TABLE, getClass().getName(),
      HBaseConfiguration.create(), FAMILIES);
    RegionCoprocessorHost h = region.getCoprocessorHost();
    h.load(ObserverA.class, Priority.HIGHEST);
    h.load(ObserverB.class, Priority.USER);
    h.load(ObserverC.class, Priority.LOWEST);

    Put put = new Put(ROW);
    put.add(A, A, A);
    int lockid = region.obtainRowLock(ROW);
    region.put(put, lockid);
    region.releaseRowLock(lockid);

    Coprocessor c = h.findCoprocessor(ObserverA.class.getName());
    long idA = ((ObserverA)c).id;
    c = h.findCoprocessor(ObserverB.class.getName());
    long idB = ((ObserverB)c).id;
    c = h.findCoprocessor(ObserverC.class.getName());
    long idC = ((ObserverC)c).id;

    assertTrue(idA < idB);
    assertTrue(idB < idC);
  }
}

