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
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A sample region observer that tests the RegionObserver interface.
 * It works with TestRegionObserverInterface to provide the test case.
 */
public class SimpleRegionObserver extends BaseRegionObserverCoprocessor {
  static final Log LOG = LogFactory.getLog(TestRegionObserverInterface.class);

  boolean beforeDelete = true;
  boolean scannerOpened = false;
  boolean hadPreOpen;
  boolean hadPostOpen;
  boolean hadPreClose;
  boolean hadPostClose;
  boolean hadPreFlush;
  boolean hadPostFlush;
  boolean hadPreSplit;
  boolean hadPostSplit;
  boolean hadPreCompact;
  boolean hadPostCompact;
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
  boolean hadPreWALRestored = false;
  boolean hadPostWALRestored = false;

  @Override
  public void preOpen(RegionCoprocessorEnvironment e) {
    hadPreOpen = true;
  }

  @Override
  public void postOpen(RegionCoprocessorEnvironment e) {
    hadPostOpen = true;
  }

  public boolean wasOpened() {
    return hadPreOpen && hadPostOpen;
  }

  @Override
  public void preClose(RegionCoprocessorEnvironment e, boolean abortRequested) {
    hadPreClose = true;
  }

  @Override
  public void postClose(RegionCoprocessorEnvironment e, boolean abortRequested) {
    hadPostClose = true;
  }

  public boolean wasClosed() {
    return hadPreClose && hadPostClose;
  }

  @Override
  public void preFlush(RegionCoprocessorEnvironment e) {
    hadPreFlush = true;
  }

  @Override
  public void postFlush(RegionCoprocessorEnvironment e) {
    hadPostFlush = true;
  }

  public boolean wasFlushed() {
    return hadPreFlush && hadPostFlush;
  }

  @Override
  public void preSplit(RegionCoprocessorEnvironment e) {
    hadPreSplit = true;
  }

  @Override
  public void postSplit(RegionCoprocessorEnvironment e, HRegion l, HRegion r) {
    hadPostSplit = true;
  }

  public boolean wasSplit() {
    return hadPreSplit && hadPostSplit;
  }

  @Override
  public void preCompact(RegionCoprocessorEnvironment e, boolean willSplit) {
    hadPreCompact = true;
  }

  @Override
  public void postCompact(RegionCoprocessorEnvironment e, boolean willSplit) {
    hadPostCompact = true;
  }

  public boolean wasCompacted() {
    return hadPreCompact && hadPostCompact;
  }

  @Override
  public void preGet(final RegionCoprocessorEnvironment e, final Get get,
      final List<KeyValue> results) throws IOException {
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(get);
    assertNotNull(results);
    if (Arrays.equals(e.getRegion().getTableDesc().getName(),
        TestRegionObserverInterface.TEST_TABLE)) {
      hadPreGet = true;
    }
  }

  @Override
  public void postGet(final RegionCoprocessorEnvironment e, final Get get,
      final List<KeyValue> results) {
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(get);
    assertNotNull(results);
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
  }

  @Override
  public void prePut(final RegionCoprocessorEnvironment e, final Map<byte[],
      List<KeyValue>> familyMap, final boolean writeToWAL) throws IOException {
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(familyMap);
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
  }

  @Override
  public void postPut(final RegionCoprocessorEnvironment e, final Map<byte[],
      List<KeyValue>> familyMap, final boolean writeToWAL) throws IOException {
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(familyMap);
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
  public void preDelete(final RegionCoprocessorEnvironment e, final Map<byte[],
      List<KeyValue>> familyMap, final boolean writeToWAL) throws IOException {
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(familyMap);
    if (beforeDelete && e.getRegion().getTableDesc().getName().equals(
        TestRegionObserverInterface.TEST_TABLE)) {
      hadPreDeleted = true;
    }
  }

  @Override
  public void postDelete(final RegionCoprocessorEnvironment e, final Map<byte[],
      List<KeyValue>> familyMap, final boolean writeToWAL) throws IOException {
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(familyMap);
    if (Arrays.equals(e.getRegion().getTableDesc().getName(),
        TestRegionObserverInterface.TEST_TABLE)) {
      beforeDelete = false;
      hadPostDeleted = true;
    }
  }

  @Override
  public void preGetClosestRowBefore(final RegionCoprocessorEnvironment e,
      final byte[] row, final byte[] family, final Result result)
      throws IOException {
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(row);
    assertNotNull(result);
    if (beforeDelete && e.getRegion().getTableDesc().getName().equals(
        TestRegionObserverInterface.TEST_TABLE)) {
      hadPreGetClosestRowBefore = true;
    }
  }

  @Override
  public void postGetClosestRowBefore(final RegionCoprocessorEnvironment e,
      final byte[] row, final byte[] family, final Result result)
      throws IOException {
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(row);
    assertNotNull(result);
    if (Arrays.equals(e.getRegion().getTableDesc().getName(),
        TestRegionObserverInterface.TEST_TABLE)) {
      hadPostGetClosestRowBefore = true;
    }
  }

  @Override
  public void preIncrement(final RegionCoprocessorEnvironment e,
      final Increment increment, final Result result) throws IOException {
    if (Arrays.equals(e.getRegion().getTableDesc().getName(),
        TestRegionObserverInterface.TEST_TABLE_2)) {
      hadPreIncrement = true;
    }
  }

  @Override
  public void postIncrement(final RegionCoprocessorEnvironment e,
      final Increment increment, final Result result) throws IOException {
    if (Arrays.equals(e.getRegion().getTableDesc().getName(),
        TestRegionObserverInterface.TEST_TABLE_2)) {
      hadPostIncrement = true;
    }
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

  boolean hadPreWALRestored() {
    return hadPreWALRestored;
  }

  boolean hadPostWALRestored() {
    return hadPostWALRestored;
  }
}
