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
package org.apache.hadoop.hdfs.server.federation.metrics;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.getBean;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.management.MalformedObjectNameException;

import org.apache.commons.collections.ListUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.store.protocol.NamenodeHeartbeatRequest;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;

import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipStats;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.hdfs.server.federation.store.records.StateStoreVersion;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

/**
 * Test the JMX interface for the {@link Router}.
 */
public class TestRBFMetrics extends TestMetricsBase {

  public static final String FEDERATION_BEAN =
      "Hadoop:service=Router,name=FederationState";
  public static final String ROUTER_BEAN =
      "Hadoop:service=Router,name=Router";

  @Test
  public void testClusterStatsJMX()
      throws MalformedObjectNameException, IOException {

    FederationMBean federationBean = getBean(FEDERATION_BEAN,
        FederationMBean.class);
    validateClusterStatsFederationBean(federationBean);
    testCapacity(federationBean);
    RouterMBean routerBean = getBean(ROUTER_BEAN, RouterMBean.class);
    validateClusterStatsRouterBean(routerBean);
  }

  @Test
  public void testClusterStatsDataSource() throws IOException {
    RBFMetrics metrics = getRouter().getMetrics();
    validateClusterStatsFederationBean(metrics);
    validateClusterStatsRouterBean(metrics);
  }

  @Test
  public void testMountTableStatsDataSource()
      throws IOException, JSONException {

    RBFMetrics metrics = getRouter().getMetrics();
    String jsonString = metrics.getMountTable();
    JSONArray jsonArray = new JSONArray(jsonString);
    assertEquals(jsonArray.length(), getMockMountTable().size());

    int match = 0;
    for (int i = 0; i < jsonArray.length(); i++) {
      JSONObject json = jsonArray.getJSONObject(i);
      String src = json.getString("sourcePath");

      for (MountTable entry : getMockMountTable()) {
        if (entry.getSourcePath().equals(src)) {
          assertEquals(entry.getDefaultLocation().getNameserviceId(),
              json.getString("nameserviceId"));
          assertEquals(entry.getDefaultLocation().getDest(),
              json.getString("path"));
          assertEquals(entry.getOwnerName(), json.getString("ownerName"));
          assertEquals(entry.getGroupName(), json.getString("groupName"));
          assertEquals(entry.getMode().toString(), json.getString("mode"));
          assertEquals(entry.getQuota().toString(), json.getString("quota"));
          assertNotNullAndNotEmpty(json.getString("dateCreated"));
          assertNotNullAndNotEmpty(json.getString("dateModified"));
          match++;
        }
      }
    }
    assertEquals(match, getMockMountTable().size());
  }

  private MembershipState findMockNamenode(String nsId, String nnId) {

    @SuppressWarnings("unchecked")
    List<MembershipState> namenodes =
        ListUtils.union(getActiveMemberships(), getStandbyMemberships());
    for (MembershipState nn : namenodes) {
      if (nn.getNamenodeId().equals(nnId)
          && nn.getNameserviceId().equals(nsId)) {
        return nn;
      }
    }
    return null;
  }

  @Test
  public void testNamenodeStatsDataSource() throws IOException, JSONException {

    RBFMetrics metrics = getRouter().getMetrics();
    String jsonString = metrics.getNamenodes();
    JSONObject jsonObject = new JSONObject(jsonString);
    Iterator<?> keys = jsonObject.keys();
    int nnsFound = 0;
    while (keys.hasNext()) {
      // Validate each entry against our mocks
      JSONObject json = jsonObject.getJSONObject((String) keys.next());
      String nameserviceId = json.getString("nameserviceId");
      String namenodeId = json.getString("namenodeId");

      MembershipState mockEntry =
          this.findMockNamenode(nameserviceId, namenodeId);
      assertNotNull(mockEntry);

      assertEquals(json.getString("state"), mockEntry.getState().toString());
      MembershipStats stats = mockEntry.getStats();
      assertEquals(json.getLong("numOfActiveDatanodes"),
          stats.getNumOfActiveDatanodes());
      assertEquals(json.getLong("numOfDeadDatanodes"),
          stats.getNumOfDeadDatanodes());
      assertEquals(json.getLong("numOfStaleDatanodes"),
          stats.getNumOfStaleDatanodes());
      assertEquals(json.getLong("numOfDecommissioningDatanodes"),
          stats.getNumOfDecommissioningDatanodes());
      assertEquals(json.getLong("numOfDecomActiveDatanodes"),
          stats.getNumOfDecomActiveDatanodes());
      assertEquals(json.getLong("numOfDecomDeadDatanodes"),
          stats.getNumOfDecomDeadDatanodes());
      assertEquals(json.getLong("numOfInMaintenanceLiveDataNodes"),
          stats.getNumOfInMaintenanceLiveDataNodes());
      assertEquals(json.getLong("numOfInMaintenanceDeadDataNodes"),
          stats.getNumOfInMaintenanceDeadDataNodes());
      assertEquals(json.getLong("numOfEnteringMaintenanceDataNodes"),
          stats.getNumOfEnteringMaintenanceDataNodes());
      assertEquals(json.getLong("numOfBlocks"), stats.getNumOfBlocks());
      assertEquals(json.getString("rpcAddress"), mockEntry.getRpcAddress());
      assertEquals(json.getString("webScheme"), mockEntry.getWebScheme());
      assertEquals(json.getString("webAddress"), mockEntry.getWebAddress());
      nnsFound++;
    }
    // Validate all memberships are present
    assertEquals(getActiveMemberships().size() + getStandbyMemberships().size(),
        nnsFound);
  }

  @Test
  public void testNameserviceStatsDataSource()
      throws IOException, JSONException {

    RBFMetrics metrics = getRouter().getMetrics();
    String jsonString = metrics.getNameservices();
    JSONObject jsonObject = new JSONObject(jsonString);
    Iterator<?> keys = jsonObject.keys();
    int nameservicesFound = 0;
    while (keys.hasNext()) {
      JSONObject json = jsonObject.getJSONObject((String) keys.next());
      String nameserviceId = json.getString("nameserviceId");
      String namenodeId = json.getString("namenodeId");

      MembershipState mockEntry =
          this.findMockNamenode(nameserviceId, namenodeId);
      assertNotNull(mockEntry);

      // NS should report the active NN
      assertEquals(mockEntry.getState().toString(), json.getString("state"));
      assertEquals("ACTIVE", json.getString("state"));

      // Stats in the NS should reflect the stats for the most active NN
      MembershipStats stats = mockEntry.getStats();
      assertEquals(stats.getNumOfFiles(), json.getLong("numOfFiles"));
      assertEquals(stats.getTotalSpace(), json.getLong("totalSpace"));
      assertEquals(stats.getAvailableSpace(),
          json.getLong("availableSpace"));
      assertEquals(stats.getNumOfBlocksMissing(),
          json.getLong("numOfBlocksMissing"));
      assertEquals(stats.getNumOfActiveDatanodes(),
          json.getLong("numOfActiveDatanodes"));
      assertEquals(stats.getNumOfDeadDatanodes(),
          json.getLong("numOfDeadDatanodes"));
      assertEquals(stats.getNumOfStaleDatanodes(),
          json.getLong("numOfStaleDatanodes"));
      assertEquals(stats.getNumOfDecommissioningDatanodes(),
          json.getLong("numOfDecommissioningDatanodes"));
      assertEquals(stats.getNumOfDecomActiveDatanodes(),
          json.getLong("numOfDecomActiveDatanodes"));
      assertEquals(stats.getNumOfDecomDeadDatanodes(),
          json.getLong("numOfDecomDeadDatanodes"));
      assertEquals(stats.getNumOfInMaintenanceLiveDataNodes(),
          json.getLong("numOfInMaintenanceLiveDataNodes"));
      assertEquals(stats.getNumOfInMaintenanceDeadDataNodes(),
          json.getLong("numOfInMaintenanceDeadDataNodes"));
      assertEquals(stats.getNumOfStaleDatanodes(),
          json.getLong("numOfEnteringMaintenanceDataNodes"));
      assertEquals(stats.getProvidedSpace(),
          json.getLong("providedSpace"));
      nameservicesFound++;
    }
    assertEquals(getNameservices().size(), nameservicesFound);
  }

  @Test
  public void testRouterStatsDataSource() throws IOException, JSONException {

    RBFMetrics metrics = getRouter().getMetrics();
    String jsonString = metrics.getRouters();
    JSONObject jsonObject = new JSONObject(jsonString);
    Iterator<?> keys = jsonObject.keys();
    int routersFound = 0;
    while (keys.hasNext()) {
      JSONObject json = jsonObject.getJSONObject((String) keys.next());
      String address = json.getString("address");
      assertNotNullAndNotEmpty(address);
      RouterState router = findMockRouter(address);
      assertNotNull(router);

      assertEquals(router.getStatus().toString(), json.getString("status"));
      assertEquals(router.getCompileInfo(), json.getString("compileInfo"));
      assertEquals(router.getVersion(), json.getString("version"));
      assertEquals(router.getDateStarted(), json.getLong("dateStarted"));
      assertEquals(router.getDateCreated(), json.getLong("dateCreated"));
      assertEquals(router.getDateModified(), json.getLong("dateModified"));

      StateStoreVersion version = router.getStateStoreVersion();
      assertEquals(
          RBFMetrics.getDateString(version.getMembershipVersion()),
          json.get("lastMembershipUpdate"));
      assertEquals(
          RBFMetrics.getDateString(version.getMountTableVersion()),
          json.get("lastMountTableUpdate"));
      assertEquals(version.getMembershipVersion(),
          json.get("membershipVersion"));
      assertEquals(version.getMountTableVersion(),
          json.get("mountTableVersion"));
      routersFound++;
    }

    assertEquals(getMockRouters().size(), routersFound);
  }

  private void assertNotNullAndNotEmpty(String field) {
    assertNotNull(field);
    assertTrue(field.length() > 0);
  }

  private RouterState findMockRouter(String routerId) {
    for (RouterState router : getMockRouters()) {
      if (router.getAddress().equals(routerId)) {
        return router;
      }
    }
    return null;
  }

  private void validateClusterStatsFederationBean(FederationMBean bean) {

    // Determine aggregates
    long numBlocks = 0;
    long numStale = 0;
    int numCorruptsFilesCount = 0;
    long scheduledReplicationBlocks = 0;
    long numberOfMissingBlocksWithReplicationFactorOne = 0;
    long highestPriorityLowRedundancyReplicatedBlocks = 0;
    long highestPriorityLowRedundancyECBlocks = 0;
    long numFiles = 0;
    for (MembershipState mock : getActiveMemberships()) {
      MembershipStats stats = mock.getStats();
      numBlocks += stats.getNumOfBlocks();
      numStale += stats.getNumOfStaleDatanodes();
      numCorruptsFilesCount += stats.getCorruptFilesCount();
      scheduledReplicationBlocks += stats.getScheduledReplicationBlocks();
      numberOfMissingBlocksWithReplicationFactorOne +=
          stats.getNumberOfMissingBlocksWithReplicationFactorOne();
      highestPriorityLowRedundancyReplicatedBlocks +=
          stats.getHighestPriorityLowRedundancyReplicatedBlocks();
      highestPriorityLowRedundancyECBlocks +=
          stats.getHighestPriorityLowRedundancyECBlocks();
    }

    assertEquals(numBlocks, bean.getNumBlocks());
    assertEquals(numStale, bean.getNumStaleNodes());
    assertEquals(numFiles, bean.getNumFiles());
    assertEquals(getActiveMemberships().size() + getStandbyMemberships().size(),
        bean.getNumNamenodes());
    assertEquals(getNameservices().size(), bean.getNumNameservices());
    assertEquals(numCorruptsFilesCount, bean.getCorruptFilesCount());
    assertEquals(scheduledReplicationBlocks,
        bean.getScheduledReplicationBlocks());
    assertEquals(numberOfMissingBlocksWithReplicationFactorOne,
        bean.getNumberOfMissingBlocksWithReplicationFactorOne());
    assertEquals(highestPriorityLowRedundancyReplicatedBlocks,
        bean.getHighestPriorityLowRedundancyReplicatedBlocks());
    assertEquals(highestPriorityLowRedundancyECBlocks,
        bean.getHighestPriorityLowRedundancyECBlocks());
  }

  private void validateClusterStatsRouterBean(RouterMBean bean) {
    assertTrue(bean.getVersion().length() > 0);
    assertTrue(bean.getCompiledDate().length() > 0);
    assertTrue(bean.getCompileInfo().length() > 0);
    assertTrue(bean.getRouterStarted().length() > 0);
    assertTrue(bean.getHostAndPort().length() > 0);
    assertFalse(bean.isSecurityEnabled());
  }

  private void testCapacity(FederationMBean bean) throws IOException {
    List<MembershipState> memberships = getActiveMemberships();
    assertTrue(memberships.size() > 1);

    BigInteger availableCapacity = BigInteger.valueOf(0);
    BigInteger totalCapacity = BigInteger.valueOf(0);
    BigInteger unitCapacity = BigInteger.valueOf(Long.MAX_VALUE);
    for (MembershipState mock : memberships) {
      MembershipStats stats = mock.getStats();
      stats.setTotalSpace(Long.MAX_VALUE);
      stats.setAvailableSpace(Long.MAX_VALUE);
      // reset stats to make the new value persistent
      mock.setStats(stats);
      // write back the new namenode information to state store
      assertTrue(refreshNamenodeRegistration(
          NamenodeHeartbeatRequest.newInstance(mock)));
      totalCapacity = totalCapacity.add(unitCapacity);
      availableCapacity = availableCapacity.add(unitCapacity);
    }

    // for local cache update
    assertEquals(totalCapacity, bean.getTotalCapacityBigInt());
    // not equal since overflow happened.
    assertNotEquals(totalCapacity, BigInteger.valueOf(bean.getTotalCapacity()));
    assertEquals(availableCapacity, bean.getRemainingCapacityBigInt());
    // not equal since overflow happened.
    assertNotEquals(availableCapacity,
        BigInteger.valueOf(bean.getRemainingCapacity()));
  }

  @Test
  public void testDatanodeNumMetrics()
      throws Exception {
    Configuration routerConf = new RouterConfigBuilder()
        .metrics()
        .http()
        .stateStore()
        .rpc()
        .build();
    MiniRouterDFSCluster cluster = new MiniRouterDFSCluster(false, 1);
    cluster.setNumDatanodesPerNameservice(0);
    cluster.addNamenodeOverrides(routerConf);
    cluster.startCluster();
    routerConf.setTimeDuration(
        RBFConfigKeys.DN_REPORT_CACHE_EXPIRE, 10000, TimeUnit.SECONDS);
    cluster.addRouterOverrides(routerConf);
    cluster.startRouters();
    Router router = cluster.getRandomRouter().getRouter();
    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();
    RouterRpcServer rpcServer = router.getRpcServer();
    RBFMetrics rbfMetrics = router.getMetrics();
    // Create mock dn
    DatanodeInfo[] dNInfo = new DatanodeInfo[4];
    DatanodeInfo datanodeInfo = new DatanodeInfo.DatanodeInfoBuilder().build();
    datanodeInfo.setDecommissioned();
    dNInfo[0] = datanodeInfo;
    datanodeInfo = new DatanodeInfo.DatanodeInfoBuilder().build();
    datanodeInfo.setInMaintenance();
    dNInfo[1] = datanodeInfo;
    datanodeInfo = new DatanodeInfo.DatanodeInfoBuilder().build();
    datanodeInfo.startMaintenance();
    dNInfo[2] = datanodeInfo;
    datanodeInfo = new DatanodeInfo.DatanodeInfoBuilder().build();
    datanodeInfo.startDecommission();
    dNInfo[3] = datanodeInfo;

    rpcServer.getDnCache().put(HdfsConstants.DatanodeReportType.LIVE, dNInfo);
    rpcServer.getDnCache().put(HdfsConstants.DatanodeReportType.DEAD, dNInfo);
    assertEquals(4, rbfMetrics.getNumDeadNodes());
    assertEquals(4, rbfMetrics.getNumLiveNodes());
    assertEquals(1, rbfMetrics.getNumDecomDeadNodes());
    // ENTERING_MAINTENANCE or IN_MAINTENANCE
    assertEquals(2, rbfMetrics.getNumInMaintenanceDeadDataNodes());
    assertEquals(1, rbfMetrics.getNumDecomLiveNodes());
    // ENTERING_MAINTENANCE or IN_MAINTENANCE
    assertEquals(2, rbfMetrics.getNumInMaintenanceLiveDataNodes());

    assertEquals(2, rbfMetrics.getNumEnteringMaintenanceDataNodes());
  }
}
