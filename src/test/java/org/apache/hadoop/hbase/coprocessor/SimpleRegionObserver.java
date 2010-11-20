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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import static org.junit.Assert.*;

/**
 * A sample region observer that tests the RegionObserver interface.
 * It works with TestRegionObserverInterface to provide the test case.
 */
public class SimpleRegionObserver extends BaseRegionObserverCoprocessor {
  static final Log LOG = LogFactory.getLog(TestRegionObserverInterface.class);

  boolean beforeDelete = true;
  boolean scannerOpened = false;
  boolean hadPreGet = false;
  boolean hadPostGet = false;
  boolean hadPrePut = false;
  boolean hadPostPut = false;
  boolean hadPreDeleted = false;
  boolean hadPostDeleted = false;
  boolean hadPreGetClosestRowBefore = false;
  boolean hadPostGetClosestRowBefore = false;
  boolean hadPreIncrement = false;
  boolean hadPostIncrement = false;


  // Overriden RegionObserver methods
  @Override
  public Get preGet(CoprocessorEnvironment e, Get get) {
    if (Arrays.equals(e.getRegion().getTableDesc().getName(),
        TestRegionObserverInterface.TEST_TABLE)) {
      hadPreGet = true;
      assertNotNull(e);
      assertNotNull(e.getRegion());
    }
    return get;
  }

  @Override
  public List<KeyValue> postGet(CoprocessorEnvironment e, Get get,
      List<KeyValue> results) {
    if (Arrays.equals(e.getRegion().getTableDesc().getName(),
        TestRegionObserverInterface.TEST_TABLE)) {
      boolean foundA = false;
      boolean foundB = false;
      boolean foundC = false;
      for (KeyValue kv: results) {
        if (Bytes.equals(kv.getFamily(), TestRegionObserverInterface.A)) {
          foundA = true;
        }
        if (Bytes.equals(kv.getFamily(), TestRegionObserverInterface.B)) {
          foundB = true;
        }
        if (Bytes.equals(kv.getFamily(), TestRegionObserverInterface.C)) {
          foundC = true;
        }
      }
      assertTrue(foundA);
      assertTrue(foundB);
      assertTrue(foundC);
      hadPostGet = true;
    }
    return results;
  }

  @Override
  public Map<byte[], List<KeyValue>> prePut(CoprocessorEnvironment e,
      Map<byte[], List<KeyValue>> familyMap) {
    if (Arrays.equals(e.getRegion().getTableDesc().getName(),
        TestRegionObserverInterface.TEST_TABLE)) {
      List<KeyValue> kvs = familyMap.get(TestRegionObserverInterface.A);
      assertNotNull(kvs);
      assertNotNull(kvs.get(0));
      assertTrue(Bytes.equals(kvs.get(0).getQualifier(),
          TestRegionObserverInterface.A));
      kvs = familyMap.get(TestRegionObserverInterface.B);
      assertNotNull(kvs);
      assertNotNull(kvs.get(0));
      assertTrue(Bytes.equals(kvs.get(0).getQualifier(),
          TestRegionObserverInterface.B));
      kvs = familyMap.get(TestRegionObserverInterface.C);
      assertNotNull(kvs);
      assertNotNull(kvs.get(0));
      assertTrue(Bytes.equals(kvs.get(0).getQualifier(),
          TestRegionObserverInterface.C));
      hadPrePut = true;
    }
    return familyMap;
  }

  @Override
  public void postPut(CoprocessorEnvironment e,
      Map<byte[], List<KeyValue>> familyMap) {
    List<KeyValue> kvs = familyMap.get(TestRegionObserverInterface.A);
    if (Arrays.equals(e.getRegion().getTableDesc().getName(),
        TestRegionObserverInterface.TEST_TABLE)) {
      assertNotNull(kvs);
      assertNotNull(kvs.get(0));
      assertTrue(Bytes.equals(kvs.get(0).getQualifier(),
          TestRegionObserverInterface.A));
      kvs = familyMap.get(TestRegionObserverInterface.B);
      assertNotNull(kvs);
      assertNotNull(kvs.get(0));
      assertTrue(Bytes.equals(kvs.get(0).getQualifier(),
          TestRegionObserverInterface.B));
      kvs = familyMap.get(TestRegionObserverInterface.C);
      assertNotNull(kvs);
      assertNotNull(kvs.get(0));
      assertTrue(Bytes.equals(kvs.get(0).getQualifier(),
          TestRegionObserverInterface.C));
      hadPostPut = true;
    }
  }

  @Override
  public Map<byte[], List<KeyValue>> preDelete(CoprocessorEnvironment e,
      Map<byte[], List<KeyValue>> familyMap) {
    if (beforeDelete && e.getRegion().getTableDesc().getName().equals(
        TestRegionObserverInterface.TEST_TABLE)) {
      hadPreDeleted = true;
    }
    return familyMap;
  }

  @Override
  public void postDelete(CoprocessorEnvironment e,
      Map<byte[], List<KeyValue>> familyMap) {
    if (Arrays.equals(e.getRegion().getTableDesc().getName(),
        TestRegionObserverInterface.TEST_TABLE)) {
      beforeDelete = false;
      hadPostDeleted = true;
    }
  }

  @Override
  public void preGetClosestRowBefore(final CoprocessorEnvironment e,
      final byte[] row, final byte[] family) {
    if (beforeDelete && e.getRegion().getTableDesc().getName().equals(
        TestRegionObserverInterface.TEST_TABLE)) {
      hadPreGetClosestRowBefore = true;
    }
  }

  @Override
  public Result postGetClosestRowBefore(final CoprocessorEnvironment e,
      final byte[] row, final byte[] family, Result result) {
    if (Arrays.equals(e.getRegion().getTableDesc().getName(),
        TestRegionObserverInterface.TEST_TABLE)) {
      hadPostGetClosestRowBefore = true;
    }
    return result;
  }

  @Override
  public Scan preScannerOpen(CoprocessorEnvironment e, Scan scan) {
    // not tested -- need to go through the RS to get here
    return scan;
  }

  @Override
  public void postScannerOpen(CoprocessorEnvironment e, Scan scan,
      long scannerId) {
    // not tested -- need to go through the RS to get here
  }

  @Override
  public void preScannerNext(final CoprocessorEnvironment e,
      final long scannerId) {
    // not tested -- need to go through the RS to get here
  }

  @Override
  public List<KeyValue> postScannerNext(final CoprocessorEnvironment e,
      final long scannerId, List<KeyValue> results) {
    // not tested -- need to go through the RS to get here
    return results;
  }

  @Override
  public void preScannerClose(final CoprocessorEnvironment e,
      final long scannerId) {
    // not tested -- need to go through the RS to get here
  }

  @Override
  public void postScannerClose(final CoprocessorEnvironment e,
      final long scannerId) {
    // not tested -- need to go through the RS to get here
  }

  @Override
  public Increment preIncrement(CoprocessorEnvironment e, Increment increment)
      throws IOException {
    if (Arrays.equals(e.getRegion().getTableDesc().getName(),
        TestRegionObserverInterface.TEST_TABLE_2)) {
      hadPreIncrement = true;
    }
    return increment;
  }

  @Override
  public Result postIncrement(CoprocessorEnvironment e, Increment increment,
      Result result) throws IOException {
    if (Arrays.equals(e.getRegion().getTableDesc().getName(),
        TestRegionObserverInterface.TEST_TABLE_2)) {
      hadPostIncrement = true;
    }
    return result;
  }

  boolean hadPreGet() {
    return hadPreGet;
  }

  boolean hadPostGet() {
    return hadPostGet;
  }

  boolean hadPrePut() {
    return hadPrePut;
  }

  boolean hadPostPut() {
    return hadPostPut;
  }

  boolean hadDelete() {
    return !beforeDelete;
  }

  boolean hadPreIncrement() {
    return hadPreIncrement;
  }

  boolean hadPostIncrement() {
    return hadPostIncrement;
  }
}
