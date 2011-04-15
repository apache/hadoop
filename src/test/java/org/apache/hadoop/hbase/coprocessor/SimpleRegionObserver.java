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
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
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
  boolean hadPreScannerNext = false;
  boolean hadPostScannerNext = false;
  boolean hadPreScannerClose = false;
  boolean hadPostScannerClose = false;
  boolean hadPreScannerOpen = false;
  boolean hadPostScannerOpen = false;

  @Override
  public void preOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
    hadPreOpen = true;
  }

  @Override
  public void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
    hadPostOpen = true;
  }

  public boolean wasOpened() {
    return hadPreOpen && hadPostOpen;
  }

  @Override
  public void preClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested) {
    hadPreClose = true;
  }

  @Override
  public void postClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested) {
    hadPostClose = true;
  }

  public boolean wasClosed() {
    return hadPreClose && hadPostClose;
  }

  @Override
  public void preFlush(ObserverContext<RegionCoprocessorEnvironment> c) {
    hadPreFlush = true;
  }

  @Override
  public void postFlush(ObserverContext<RegionCoprocessorEnvironment> c) {
    hadPostFlush = true;
  }

  public boolean wasFlushed() {
    return hadPreFlush && hadPostFlush;
  }

  @Override
  public void preSplit(ObserverContext<RegionCoprocessorEnvironment> c) {
    hadPreSplit = true;
  }

  @Override
  public void postSplit(ObserverContext<RegionCoprocessorEnvironment> c, HRegion l, HRegion r) {
    hadPostSplit = true;
  }

  public boolean wasSplit() {
    return hadPreSplit && hadPostSplit;
  }

  @Override
  public void preCompact(ObserverContext<RegionCoprocessorEnvironment> c, boolean willSplit) {
    hadPreCompact = true;
  }

  @Override
  public void postCompact(ObserverContext<RegionCoprocessorEnvironment> c, boolean willSplit) {
    hadPostCompact = true;
  }

  @Override
  public InternalScanner preScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Scan scan,
      final InternalScanner s) throws IOException {
    hadPreScannerOpen = true;
    return null;
  }

  @Override
  public InternalScanner postScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Scan scan, final InternalScanner s)
      throws IOException {
    hadPostScannerOpen = true;
    return s;
  }

  @Override
  public boolean preScannerNext(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s, final List<Result> results,
      final int limit, final boolean hasMore) throws IOException {
    hadPreScannerNext = true;
    return hasMore;
  }

  @Override
  public boolean postScannerNext(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s, final List<Result> results, final int limit,
      final boolean hasMore) throws IOException {
    hadPostScannerNext = true;
    return hasMore;
  }

  @Override
  public void preScannerClose(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s) throws IOException {
    hadPreScannerClose = true;
  }

  @Override
  public void postScannerClose(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s) throws IOException {
    hadPostScannerClose = true;
  }

  public boolean wasCompacted() {
    return hadPreCompact && hadPostCompact;
  }

  @Override
  public void preGet(final ObserverContext<RegionCoprocessorEnvironment> c, final Get get,
      final List<KeyValue> results) throws IOException {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(get);
    assertNotNull(results);
    hadPreGet = true;
  }

  @Override
  public void postGet(final ObserverContext<RegionCoprocessorEnvironment> c, final Get get,
      final List<KeyValue> results) {
    RegionCoprocessorEnvironment e = c.getEnvironment();
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
    }
    hadPostGet = true;
  }

  @Override
  public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c, final Map<byte[],
      List<KeyValue>> familyMap, final boolean writeToWAL) throws IOException {
    RegionCoprocessorEnvironment e = c.getEnvironment();
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
    }
    hadPrePut = true;
  }

  @Override
  public void postPut(final ObserverContext<RegionCoprocessorEnvironment> c, final Map<byte[],
      List<KeyValue>> familyMap, final boolean writeToWAL) throws IOException {
    RegionCoprocessorEnvironment e = c.getEnvironment();
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
    }
    hadPostPut = true;
  }

  @Override
  public void preDelete(final ObserverContext<RegionCoprocessorEnvironment> c, final Map<byte[],
      List<KeyValue>> familyMap, final boolean writeToWAL) throws IOException {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(familyMap);
    if (beforeDelete) {
      hadPreDeleted = true;
    }
  }

  @Override
  public void postDelete(final ObserverContext<RegionCoprocessorEnvironment> c, final Map<byte[],
      List<KeyValue>> familyMap, final boolean writeToWAL) throws IOException {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(familyMap);
    beforeDelete = false;
    hadPostDeleted = true;
  }

  @Override
  public void preGetClosestRowBefore(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte[] row, final byte[] family, final Result result)
      throws IOException {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(row);
    assertNotNull(result);
    if (beforeDelete) {
      hadPreGetClosestRowBefore = true;
    }
  }

  @Override
  public void postGetClosestRowBefore(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte[] row, final byte[] family, final Result result)
      throws IOException {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    assertNotNull(e);
    assertNotNull(e.getRegion());
    assertNotNull(row);
    assertNotNull(result);
    hadPostGetClosestRowBefore = true;
  }

  @Override
  public void preIncrement(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Increment increment, final Result result) throws IOException {
    hadPreIncrement = true;
  }

  @Override
  public void postIncrement(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Increment increment, final Result result) throws IOException {
    hadPostIncrement = true;
  }

  public boolean hadPreGet() {
    return hadPreGet;
  }

  public boolean hadPostGet() {
    return hadPostGet;
  }

  public boolean hadPrePut() {
    return hadPrePut;
  }

  public boolean hadPostPut() {
    return hadPostPut;
  }
  public boolean hadDelete() {
    return !beforeDelete;
  }

  public boolean hadPreIncrement() {
    return hadPreIncrement;
  }

  public boolean hadPostIncrement() {
    return hadPostIncrement;
  }

  public boolean hadPreWALRestored() {
    return hadPreWALRestored;
  }

  public boolean hadPostWALRestored() {
    return hadPostWALRestored;
  }
  public boolean wasScannerNextCalled() {
    return hadPreScannerNext && hadPostScannerNext;
  }
  public boolean wasScannerCloseCalled() {
    return hadPreScannerClose && hadPostScannerClose;
  }
  public boolean wasScannerOpenCalled() {
    return hadPreScannerOpen && hadPostScannerOpen;
  }
  public boolean hadDeleted() {
    return hadPreDeleted && hadPostDeleted;
  }
}
