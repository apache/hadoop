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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestRegionSplitPolicy {

  private Configuration conf;
  private HTableDescriptor htd;
  private HRegion mockRegion;
  private TreeMap<byte[], Store> stores;

  @Before
  public void setupMocks() {
    conf = HBaseConfiguration.create();

    HRegionInfo hri = new HRegionInfo(Bytes.toBytes("testtable"));

    htd = new HTableDescriptor();
    mockRegion = Mockito.mock(HRegion.class);
    Mockito.doReturn(htd).when(mockRegion).getTableDesc();
    Mockito.doReturn(hri).when(mockRegion).getRegionInfo();
    
    stores = new TreeMap<byte[], Store>(Bytes.BYTES_COMPARATOR);
    Mockito.doReturn(stores).when(mockRegion).getStores();
  }
  
  @Test
  public void testCreateDefault() throws IOException {
    conf.setLong("hbase.hregion.max.filesize", 1234L);
    
    // Using a default HTD, should pick up the file size from
    // configuration.
    ConstantSizeRegionSplitPolicy policy =
        (ConstantSizeRegionSplitPolicy)RegionSplitPolicy.create(
            mockRegion, conf);
    assertEquals(1234L, policy.getDesiredMaxFileSize());
    
    // If specified in HTD, should use that
    htd.setMaxFileSize(9999L);
    policy = (ConstantSizeRegionSplitPolicy)RegionSplitPolicy.create(
        mockRegion, conf);
    assertEquals(9999L, policy.getDesiredMaxFileSize());    
  }
  
  @Test
  public void testConstantSizePolicy() throws IOException {
    htd.setMaxFileSize(1024L);
    
    ConstantSizeRegionSplitPolicy policy =
      (ConstantSizeRegionSplitPolicy)RegionSplitPolicy.create(mockRegion, conf);
    
    // For no stores, should not split
    assertFalse(policy.shouldSplit());

    // Add a store above the requisite size. Should split.
    Store mockStore = Mockito.mock(Store.class);
    Mockito.doReturn(2000L).when(mockStore).getSize();
    Mockito.doReturn(true).when(mockStore).canSplit();
    stores.put(new byte[]{1}, mockStore);
    
    assertTrue(policy.shouldSplit());
    
    // Act as if there's a reference file or some other reason it can't split.
    // This should prevent splitting even though it's big enough.
    Mockito.doReturn(false).when(mockStore).canSplit();
    assertFalse(policy.shouldSplit());

    // Reset splittability after above
    Mockito.doReturn(true).when(mockStore).canSplit();
    
    // Set to a small size but turn on forceSplit. Should result in a split.
    Mockito.doReturn(true).when(mockRegion).shouldForceSplit();
    Mockito.doReturn(100L).when(mockStore).getSize();
    assertTrue(policy.shouldSplit());
    
    // Turn off forceSplit, should not split
    Mockito.doReturn(false).when(mockRegion).shouldForceSplit();
    assertFalse(policy.shouldSplit());
  }
  
  @Test
  public void testGetSplitPoint() throws IOException {
    ConstantSizeRegionSplitPolicy policy =
      (ConstantSizeRegionSplitPolicy)RegionSplitPolicy.create(mockRegion, conf);
    
    // For no stores, should not split
    assertFalse(policy.shouldSplit());
    assertNull(policy.getSplitPoint());
    
    // Add a store above the requisite size. Should split.
    Store mockStore = Mockito.mock(Store.class);
    Mockito.doReturn(2000L).when(mockStore).getSize();
    Mockito.doReturn(true).when(mockStore).canSplit();
    Mockito.doReturn(Bytes.toBytes("store 1 split"))
      .when(mockStore).getSplitPoint();
    stores.put(new byte[]{1}, mockStore);

    assertEquals("store 1 split",
        Bytes.toString(policy.getSplitPoint()));
    
    // Add a bigger store. The split point should come from that one
    Store mockStore2 = Mockito.mock(Store.class);
    Mockito.doReturn(4000L).when(mockStore2).getSize();
    Mockito.doReturn(true).when(mockStore2).canSplit();
    Mockito.doReturn(Bytes.toBytes("store 2 split"))
      .when(mockStore2).getSplitPoint();
    stores.put(new byte[]{2}, mockStore2);
    
    assertEquals("store 2 split",
        Bytes.toString(policy.getSplitPoint()));
  }
}
