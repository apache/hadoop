/**
 * Copyright 2011 The Apache Software Foundation
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

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.metrics.RegionServerMetrics;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hbase.tmpl.regionserver.RSStatusTmpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

/**
 * Tests for the region server status page and its template.
 */
public class TestRSStatusServlet {
  private HRegionServer rs;
  
  static final int FAKE_IPC_PORT = 1585;
  static final int FAKE_WEB_PORT = 1586;
  
  @SuppressWarnings("deprecation")
  private HServerAddress fakeAddress =
    new HServerAddress("localhost", FAKE_IPC_PORT);
  @SuppressWarnings("deprecation")
  private HServerInfo fakeInfo =
    new HServerInfo(fakeAddress, FAKE_WEB_PORT);
  private RegionServerMetrics metrics =
    new RegionServerMetrics();
  
  @SuppressWarnings("deprecation")
  @Before
  public void setupBasicMocks() throws IOException {
    rs = Mockito.mock(HRegionServer.class);
    Mockito.doReturn(HBaseConfiguration.create())
      .when(rs).getConfiguration();
    Mockito.doReturn(fakeInfo).when(rs).getHServerInfo();
    Mockito.doReturn(metrics).when(rs).getMetrics();

    // Fake ZKW
    ZooKeeperWatcher zkw = Mockito.mock(ZooKeeperWatcher.class);
    Mockito.doReturn("fakequorum").when(zkw).getQuorum();
    Mockito.doReturn(zkw).when(rs).getZooKeeper();
  }
  
  @Test
  public void testBasic() throws IOException {
    new RSStatusTmpl().render(new StringWriter(), rs);
  }
  
  @Test
  public void testWithRegions() throws IOException {
    HTableDescriptor htd = new HTableDescriptor("mytable");
    List<HRegionInfo> regions = Lists.newArrayList(
        new HRegionInfo(htd.getName(), Bytes.toBytes("a"), Bytes.toBytes("d")),
        new HRegionInfo(htd.getName(), Bytes.toBytes("d"), Bytes.toBytes("z"))
        );
    Mockito.doReturn(regions).when(rs).getOnlineRegions();
    
    new RSStatusTmpl().render(new StringWriter(), rs);    
  }
  
  
}
