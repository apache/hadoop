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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests invocation of the {@link org.apache.hadoop.hbase.coprocessor.MasterObserver}
 * interface hooks at all appropriate times during normal HMaster operations.
 */
public class TestMasterObserver {

  public static class CPMasterObserver implements MasterObserver {

    private boolean preCreateTableCalled;
    private boolean postCreateTableCalled;
    private boolean preDeleteTableCalled;
    private boolean postDeleteTableCalled;
    private boolean preModifyTableCalled;
    private boolean postModifyTableCalled;
    private boolean preAddColumnCalled;
    private boolean postAddColumnCalled;
    private boolean preModifyColumnCalled;
    private boolean postModifyColumnCalled;
    private boolean preDeleteColumnCalled;
    private boolean postDeleteColumnCalled;
    private boolean preEnableTableCalled;
    private boolean postEnableTableCalled;
    private boolean preDisableTableCalled;
    private boolean postDisableTableCalled;
    private boolean preMoveCalled;
    private boolean postMoveCalled;
    private boolean preAssignCalled;
    private boolean postAssignCalled;
    private boolean preUnassignCalled;
    private boolean postUnassignCalled;
    private boolean preBalanceCalled;
    private boolean postBalanceCalled;
    private boolean preBalanceSwitchCalled;
    private boolean postBalanceSwitchCalled;
    private boolean preShutdownCalled;
    private boolean preStopMasterCalled;
    private boolean startCalled;
    private boolean stopCalled;

    @Override
    public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> env,
        HTableDescriptor desc, byte[][] splitKeys) throws IOException {
      preCreateTableCalled = true;
    }

    @Override
    public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> env,
        HRegionInfo[] regions, boolean sync) throws IOException {
      postCreateTableCalled = true;
    }

    public boolean wasCreateTableCalled() {
      return preCreateTableCalled && postCreateTableCalled;
    }

    @Override
    public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> env,
        byte[] tableName) throws IOException {
      preDeleteTableCalled = true;
    }

    @Override
    public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> env,
        byte[] tableName) throws IOException {
      postDeleteTableCalled = true;
    }

    public boolean wasDeleteTableCalled() {
      return preDeleteTableCalled && postDeleteTableCalled;
    }

    @Override
    public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> env,
        byte[] tableName, HTableDescriptor htd) throws IOException {
      preModifyTableCalled = true;
    }

    @Override
    public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> env,
        byte[] tableName, HTableDescriptor htd) throws IOException {
      postModifyTableCalled = true;
    }

    public boolean wasModifyTableCalled() {
      return preModifyTableCalled && postModifyTableCalled;
    }

    @Override
    public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> env,
        byte[] tableName, HColumnDescriptor column) throws IOException {
      preAddColumnCalled = true;
    }

    @Override
    public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> env,
        byte[] tableName, HColumnDescriptor column) throws IOException {
      postAddColumnCalled = true;
    }

    public boolean wasAddColumnCalled() {
      return preAddColumnCalled && postAddColumnCalled;
    }

    @Override
    public void preModifyColumn(ObserverContext<MasterCoprocessorEnvironment> env,
        byte[] tableName, HColumnDescriptor descriptor) throws IOException {
      preModifyColumnCalled = true;
    }

    @Override
    public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> env,
        byte[] tableName, HColumnDescriptor descriptor) throws IOException {
      postModifyColumnCalled = true;
    }

    public boolean wasModifyColumnCalled() {
      return preModifyColumnCalled && postModifyColumnCalled;
    }

    @Override
    public void preDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> env,
        byte[] tableName, byte[] c) throws IOException {
      preDeleteColumnCalled = true;
    }

    @Override
    public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> env,
        byte[] tableName, byte[] c) throws IOException {
      postDeleteColumnCalled = true;
    }

    public boolean wasDeleteColumnCalled() {
      return preDeleteColumnCalled && postDeleteColumnCalled;
    }

    @Override
    public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> env,
        byte[] tableName) throws IOException {
      preEnableTableCalled = true;
    }

    @Override
    public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> env,
        byte[] tableName) throws IOException {
      postEnableTableCalled = true;
    }

    public boolean wasEnableTableCalled() {
      return preEnableTableCalled && postEnableTableCalled;
    }

    @Override
    public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> env,
        byte[] tableName) throws IOException {
      preDisableTableCalled = true;
    }

    @Override
    public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> env,
        byte[] tableName) throws IOException {
      postDisableTableCalled = true;
    }

    public boolean wasDisableTableCalled() {
      return preDisableTableCalled && postDisableTableCalled;
    }

    @Override
    public void preMove(ObserverContext<MasterCoprocessorEnvironment> env,
        HRegionInfo region, ServerName srcServer, ServerName destServer)
    throws UnknownRegionException {
      preMoveCalled = true;
    }

    @Override
    public void postMove(ObserverContext<MasterCoprocessorEnvironment> env, HRegionInfo region,
        ServerName srcServer, ServerName destServer)
    throws UnknownRegionException {
      postMoveCalled = true;
    }

    public boolean wasMoveCalled() {
      return preMoveCalled && postMoveCalled;
    }

    @Override
    public void preAssign(ObserverContext<MasterCoprocessorEnvironment> env,
        final byte [] regionName, final boolean force) throws IOException {
      preAssignCalled = true;
    }

    @Override
    public void postAssign(ObserverContext<MasterCoprocessorEnvironment> env,
        final HRegionInfo regionInfo) throws IOException {
      postAssignCalled = true;
    }

    public boolean wasAssignCalled() {
      return preAssignCalled && postAssignCalled;
    }

    @Override
    public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> env,
        final byte [] regionName, final boolean force) throws IOException {
      preUnassignCalled = true;
    }

    @Override
    public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> env,
        final HRegionInfo regionInfo, final boolean force) throws IOException {
      postUnassignCalled = true;
    }

    public boolean wasUnassignCalled() {
      return preUnassignCalled && postUnassignCalled;
    }

    @Override
    public void preBalance(ObserverContext<MasterCoprocessorEnvironment> env)
        throws IOException {
      preBalanceCalled = true;
    }

    @Override
    public void postBalance(ObserverContext<MasterCoprocessorEnvironment> env)
        throws IOException {
      postBalanceCalled = true;
    }

    public boolean wasBalanceCalled() {
      return preBalanceCalled && postBalanceCalled;
    }

    @Override
    public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> env, boolean b)
        throws IOException {
      preBalanceSwitchCalled = true;
      return b;
    }

    @Override
    public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> env,
        boolean oldValue, boolean newValue) throws IOException {
      postBalanceSwitchCalled = true;
    }

    public boolean wasBalanceSwitchCalled() {
      return preBalanceSwitchCalled && postBalanceSwitchCalled;
    }

    @Override
    public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> env)
        throws IOException {
      preShutdownCalled = true;
    }

    @Override
    public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> env)
        throws IOException {
      preStopMasterCalled = true;
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
      startCalled = true;
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
      stopCalled = true;
    }

    public boolean wasStarted() { return startCalled; }

    public boolean wasStopped() { return stopCalled; }
  }

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static byte[] TEST_TABLE = Bytes.toBytes("observed_table");
  private static byte[] TEST_FAMILY = Bytes.toBytes("fam1");
  private static byte[] TEST_FAMILY2 = Bytes.toBytes("fam2");

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        CPMasterObserver.class.getName());

    UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testStarted() throws Exception {
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();

    HMaster master = cluster.getMaster();
    assertTrue("Master should be active", master.isActiveMaster());
    MasterCoprocessorHost host = master.getCoprocessorHost();
    assertNotNull("CoprocessorHost should not be null", host);
    CPMasterObserver cp = (CPMasterObserver)host.findCoprocessor(
        CPMasterObserver.class.getName());
    assertNotNull("CPMasterObserver coprocessor not found or not installed!", cp);

    // check basic lifecycle
    assertTrue("MasterObserver should have been started", cp.wasStarted());
  }

  @Test
  public void testTableOperations() throws Exception {
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();

    HMaster master = cluster.getMaster();
    MasterCoprocessorHost host = master.getCoprocessorHost();
    CPMasterObserver cp = (CPMasterObserver)host.findCoprocessor(
        CPMasterObserver.class.getName());
    assertFalse("No table created yet", cp.wasCreateTableCalled());

    // create a table
    HTableDescriptor htd = new HTableDescriptor(TEST_TABLE);
    htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.createTable(htd);
    assertTrue("Test table should be created", cp.wasCreateTableCalled());

    // disable
    assertFalse(cp.wasDisableTableCalled());
    admin.disableTable(TEST_TABLE);
    assertTrue(admin.isTableDisabled(TEST_TABLE));
    assertTrue("Coprocessor should have been called on table disable",
        cp.wasDisableTableCalled());

    // modify table
    htd.setMaxFileSize(512 * 1024 * 1024);
    admin.modifyTable(TEST_TABLE, htd);
    assertTrue("Test table should have been modified",
        cp.wasModifyTableCalled());

    // add a column family
    admin.addColumn(TEST_TABLE, new HColumnDescriptor(TEST_FAMILY2));
    assertTrue("New column family should have been added to test table",
        cp.wasAddColumnCalled());

    // modify a column family
    HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAMILY2);
    hcd.setMaxVersions(25);
    admin.modifyColumn(TEST_TABLE, hcd);
    assertTrue("Second column family should be modified",
        cp.wasModifyColumnCalled());

    // enable
    assertFalse(cp.wasEnableTableCalled());
    admin.enableTable(TEST_TABLE);
    assertTrue(admin.isTableEnabled(TEST_TABLE));
    assertTrue("Coprocessor should have been called on table enable",
        cp.wasEnableTableCalled());

    // disable again
    admin.disableTable(TEST_TABLE);
    assertTrue(admin.isTableDisabled(TEST_TABLE));

    // delete column
    assertFalse("No column family deleted yet", cp.wasDeleteColumnCalled());
    admin.deleteColumn(TEST_TABLE, TEST_FAMILY2);
    HTableDescriptor tableDesc = admin.getTableDescriptor(TEST_TABLE);
    assertNull("'"+Bytes.toString(TEST_FAMILY2)+"' should have been removed",
        tableDesc.getFamily(TEST_FAMILY2));
    assertTrue("Coprocessor should have been called on column delete",
        cp.wasDeleteColumnCalled());

    // delete table
    assertFalse("No table deleted yet", cp.wasDeleteTableCalled());
    admin.deleteTable(TEST_TABLE);
    assertFalse("Test table should have been deleted",
        admin.tableExists(TEST_TABLE));
    assertTrue("Coprocessor should have been called on table delete",
        cp.wasDeleteTableCalled());
  }

  @Test
  public void testRegionTransitionOperations() throws Exception {
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();

    HMaster master = cluster.getMaster();
    MasterCoprocessorHost host = master.getCoprocessorHost();
    CPMasterObserver cp = (CPMasterObserver)host.findCoprocessor(
        CPMasterObserver.class.getName());

    HTable table = UTIL.createTable(TEST_TABLE, TEST_FAMILY);
    UTIL.createMultiRegions(table, TEST_FAMILY);

    Map<HRegionInfo,HServerAddress> regions = table.getRegionsInfo();
    assertFalse(regions.isEmpty());
    Map.Entry<HRegionInfo, HServerAddress> firstRegion =
        regions.entrySet().iterator().next();

    // try to force a move
    Collection<ServerName> servers = master.getClusterStatus().getServers();
    String destName = null;
    for (ServerName info : servers) {
      HServerAddress hsa =
        new HServerAddress(info.getHostname(), info.getPort());
      if (!hsa.equals(firstRegion.getValue())) {
        destName = info.toString();
        break;
      }
    }
    master.move(firstRegion.getKey().getEncodedNameAsBytes(),
        Bytes.toBytes(destName));
    assertTrue("Coprocessor should have been called on region move",
        cp.wasMoveCalled());

    // make sure balancer is on
    master.balanceSwitch(true);
    assertTrue("Coprocessor should have been called on balance switch",
        cp.wasBalanceSwitchCalled());

    // force region rebalancing
    master.balanceSwitch(false);
    // move half the open regions from RS 0 to RS 1
    HRegionServer rs = cluster.getRegionServer(0);
    byte[] destRS = Bytes.toBytes(cluster.getRegionServer(1).getServerName().toString());
    List<HRegionInfo> openRegions = rs.getOnlineRegions();
    int moveCnt = openRegions.size()/2;
    for (int i=0; i<moveCnt; i++) {
      HRegionInfo info = openRegions.get(i);
      if (!(info.isMetaRegion() || info.isRootRegion())) {
        master.move(openRegions.get(i).getEncodedNameAsBytes(), destRS);
      }
    }

    // wait for assignments to finish
    AssignmentManager mgr = master.getAssignmentManager();
    Collection<AssignmentManager.RegionState> transRegions =
        mgr.getRegionsInTransition().values();
    for (AssignmentManager.RegionState state : transRegions) {
      mgr.waitOnRegionToClearRegionsInTransition(state.getRegion());
    }

    // now trigger a balance
    master.balanceSwitch(true);
    boolean balanceRun = master.balance();
    assertTrue("Coprocessor should be called on region rebalancing",
        cp.wasBalanceCalled());
  }
}
