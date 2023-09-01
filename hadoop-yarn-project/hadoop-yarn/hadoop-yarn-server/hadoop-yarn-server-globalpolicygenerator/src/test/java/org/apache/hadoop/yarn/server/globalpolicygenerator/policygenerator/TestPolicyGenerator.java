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

package org.apache.hadoop.yarn.server.globalpolicygenerator.policygenerator;

import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONUnmarshaller;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.manager.FederationPolicyManager;
import org.apache.hadoop.yarn.server.federation.policies.manager.WeightedLocalityPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGContext;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGContextImpl;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGPolicyFacade;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfoList;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for GPG Policy Generator.
 */
public class TestPolicyGenerator {

  private static final int NUM_SC = 3;

  private Configuration conf;
  private FederationStateStore stateStore;
  private FederationStateStoreFacade facade =
      FederationStateStoreFacade.getInstance();

  private List<SubClusterId> subClusterIds;
  private Map<SubClusterId, SubClusterInfo> subClusterInfos;
  private Map<SubClusterId, Map<Class, Object>> clusterInfos;
  private Map<SubClusterId, SchedulerInfo> schedulerInfos;

  private GPGContext gpgContext;

  private PolicyGenerator policyGenerator;

  public TestPolicyGenerator() {
    conf = new Configuration();
    conf.setInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS, 0);

    gpgContext = new GPGContextImpl();
    gpgContext.setPolicyFacade(new GPGPolicyFacade(facade, conf));
    gpgContext.setStateStoreFacade(facade);
  }

  @Before
  public void setUp() throws IOException, YarnException, JAXBException {
    subClusterIds = new ArrayList<>();
    subClusterInfos = new HashMap<>();
    clusterInfos = new HashMap<>();
    schedulerInfos = new HashMap<>();

    CapacitySchedulerInfo sti1 =
        readJSON("src/test/resources/schedulerInfo1.json",
            CapacitySchedulerInfo.class);
    CapacitySchedulerInfo sti2 =
        readJSON("src/test/resources/schedulerInfo2.json",
            CapacitySchedulerInfo.class);

    // Set up sub clusters
    for (int i = 0; i < NUM_SC; ++i) {
      // Sub cluster Id
      SubClusterId id = SubClusterId.newInstance("sc" + i);
      subClusterIds.add(id);

      // Sub cluster info
      SubClusterInfo cluster = SubClusterInfo
          .newInstance(id, "amrm:" + i, "clientrm:" + i, "rmadmin:" + i,
              "rmweb:" + i, SubClusterState.SC_RUNNING, 0, "");
      subClusterInfos.put(id, cluster);

      // Cluster metrics info
      ClusterMetricsInfo metricsInfo = new ClusterMetricsInfo();
      metricsInfo.setAppsPending(2000);
      if (!clusterInfos.containsKey(id)) {
        clusterInfos.put(id, new HashMap<Class, Object>());
      }
      clusterInfos.get(id).put(ClusterMetricsInfo.class, metricsInfo);

      schedulerInfos.put(id, sti1);
    }

    // Change one of the sub cluster schedulers
    schedulerInfos.put(subClusterIds.get(0), sti2);

    stateStore = mock(FederationStateStore.class);
    when(stateStore.getSubClusters(any()))
        .thenReturn(GetSubClustersInfoResponse.newInstance(
        new ArrayList<>(subClusterInfos.values())));
    facade.reinitialize(stateStore, conf);
  }

  @After
  public void tearDown() throws Exception {
    stateStore.close();
    stateStore = null;
  }

  private <T> T readJSON(String pathname, Class<T> classy)
      throws IOException, JAXBException {

    JSONJAXBContext jc =
        new JSONJAXBContext(JSONConfiguration.mapped().build(), classy);
    JSONUnmarshaller unmarshaller = jc.createJSONUnmarshaller();
    String contents = new String(Files.readAllBytes(Paths.get(pathname)));
    return unmarshaller.unmarshalFromJSON(new StringReader(contents), classy);

  }

  @Test
  public void testPolicyGenerator() throws YarnException {
    policyGenerator = new TestablePolicyGenerator();
    policyGenerator.setPolicy(mock(GlobalPolicy.class));
    policyGenerator.run();
    verify(policyGenerator.getPolicy(), times(1))
        .updatePolicy("default", clusterInfos, null);
    verify(policyGenerator.getPolicy(), times(1))
        .updatePolicy("default2", clusterInfos, null);
  }

  @Test
  public void testBlacklist() throws YarnException {
    conf.set(YarnConfiguration.GPG_POLICY_GENERATOR_BLACKLIST,
        subClusterIds.get(0).toString());
    Map<SubClusterId, Map<Class, Object>> blacklistedCMI =
        new HashMap<>(clusterInfos);
    blacklistedCMI.remove(subClusterIds.get(0));
    policyGenerator = new TestablePolicyGenerator();
    policyGenerator.setPolicy(mock(GlobalPolicy.class));
    policyGenerator.run();
    verify(policyGenerator.getPolicy(), times(1))
        .updatePolicy("default", blacklistedCMI, null);
    verify(policyGenerator.getPolicy(), times(0))
        .updatePolicy("default", clusterInfos, null);
  }

  @Test
  public void testBlacklistTwo() throws YarnException {
    conf.set(YarnConfiguration.GPG_POLICY_GENERATOR_BLACKLIST,
        subClusterIds.get(0).toString() + "," + subClusterIds.get(1)
            .toString());
    Map<SubClusterId, Map<Class, Object>> blacklistedCMI =
        new HashMap<>(clusterInfos);
    blacklistedCMI.remove(subClusterIds.get(0));
    blacklistedCMI.remove(subClusterIds.get(1));
    policyGenerator = new TestablePolicyGenerator();
    policyGenerator.setPolicy(mock(GlobalPolicy.class));
    policyGenerator.run();
    verify(policyGenerator.getPolicy(), times(1))
        .updatePolicy("default", blacklistedCMI, null);
    verify(policyGenerator.getPolicy(), times(0))
        .updatePolicy("default", clusterInfos, null);
  }

  @Test
  public void testExistingPolicy() throws YarnException {
    WeightedLocalityPolicyManager manager = new WeightedLocalityPolicyManager();
    // Add a test policy for test queue
    manager.setQueue("default");
    manager.getWeightedPolicyInfo().setAMRMPolicyWeights(GPGUtils
        .createUniformWeights(new HashSet<>(subClusterIds)));
    manager.getWeightedPolicyInfo().setRouterPolicyWeights(GPGUtils
        .createUniformWeights(new HashSet<>(subClusterIds)));
    SubClusterPolicyConfiguration testConf = manager.serializeConf();
    when(stateStore.getPolicyConfiguration(
        GetSubClusterPolicyConfigurationRequest.newInstance("default")))
        .thenReturn(
            GetSubClusterPolicyConfigurationResponse.newInstance(testConf));

    policyGenerator = new TestablePolicyGenerator();
    policyGenerator.setPolicy(mock(GlobalPolicy.class));
    policyGenerator.run();

    ArgumentCaptor<FederationPolicyManager> argCaptor =
        ArgumentCaptor.forClass(FederationPolicyManager.class);
    verify(policyGenerator.getPolicy(), times(1))
        .updatePolicy(eq("default"), eq(clusterInfos), argCaptor.capture());
    assertEquals(argCaptor.getValue().getClass(), manager.getClass());
    assertEquals(argCaptor.getValue().serializeConf(), manager.serializeConf());
  }

  @Test
  public void testCallRM() {

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();

    final String a = CapacitySchedulerConfiguration.ROOT + ".a";
    final String b = CapacitySchedulerConfiguration.ROOT + ".b";
    final String a1 = a + ".a1";
    final String a2 = a + ".a2";
    final String b1 = b + ".b1";
    final String b2 = b + ".b2";
    final String b3 = b + ".b3";
    float aCapacity = 10.5f;
    float bCapacity = 89.5f;
    float a1Capacity = 30;
    float a2Capacity = 70;
    float b1Capacity = 79.2f;
    float b2Capacity = 0.8f;
    float b3Capacity = 20;

    // Define top-level queues
    csConf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] {"a", "b"});

    csConf.setCapacity(a, aCapacity);
    csConf.setCapacity(b, bCapacity);

    // Define 2nd-level queues
    csConf.setQueues(a, new String[] {"a1", "a2"});
    csConf.setCapacity(a1, a1Capacity);
    csConf.setUserLimitFactor(a1, 100.0f);
    csConf.setCapacity(a2, a2Capacity);
    csConf.setUserLimitFactor(a2, 100.0f);

    csConf.setQueues(b, new String[] {"b1", "b2", "b3"});
    csConf.setCapacity(b1, b1Capacity);
    csConf.setUserLimitFactor(b1, 100.0f);
    csConf.setCapacity(b2, b2Capacity);
    csConf.setUserLimitFactor(b2, 100.0f);
    csConf.setCapacity(b3, b3Capacity);
    csConf.setUserLimitFactor(b3, 100.0f);

    YarnConfiguration rmConf = new YarnConfiguration(csConf);

    ResourceManager resourceManager = new ResourceManager();
    rmConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    resourceManager.init(rmConf);
    resourceManager.start();

    String rmAddress = WebAppUtils.getRMWebAppURLWithScheme(this.conf);
    String webAppAddress = getServiceAddress(NetUtils.createSocketAddr(rmAddress));

    SchedulerTypeInfo sti = GPGUtils.invokeRMWebService(webAppAddress, RMWSConsts.SCHEDULER,
        SchedulerTypeInfo.class, this.conf);

    Assert.assertNotNull(sti);
    SchedulerInfo schedulerInfo = sti.getSchedulerInfo();
    Assert.assertTrue(schedulerInfo instanceof CapacitySchedulerInfo);

    CapacitySchedulerInfo capacitySchedulerInfo = (CapacitySchedulerInfo) schedulerInfo;
    Assert.assertNotNull(capacitySchedulerInfo);

    CapacitySchedulerQueueInfoList queues = capacitySchedulerInfo.getQueues();
    Assert.assertNotNull(queues);
    ArrayList<CapacitySchedulerQueueInfo> queueInfoList = queues.getQueueInfoList();
    Assert.assertNotNull(queueInfoList);
    Assert.assertEquals(2, queueInfoList.size());

    CapacitySchedulerQueueInfo queueA = queueInfoList.get(0);
    Assert.assertNotNull(queueA);
    Assert.assertEquals("root.a", queueA.getQueuePath());
    Assert.assertEquals(10.5f, queueA.getCapacity(), 0.00001);
    CapacitySchedulerQueueInfoList queueAQueues = queueA.getQueues();
    Assert.assertNotNull(queueAQueues);
    ArrayList<CapacitySchedulerQueueInfo> queueInfoAList = queueAQueues.getQueueInfoList();
    Assert.assertNotNull(queueInfoAList);
    Assert.assertEquals(2, queueInfoAList.size());
    CapacitySchedulerQueueInfo queueA1 = queueInfoAList.get(0);
    Assert.assertNotNull(queueA1);
    Assert.assertEquals(30f, queueA1.getCapacity(), 0.00001);
    CapacitySchedulerQueueInfo queueA2 = queueInfoAList.get(1);
    Assert.assertNotNull(queueA2);
    Assert.assertEquals(70f, queueA2.getCapacity(), 0.00001);

    CapacitySchedulerQueueInfo queueB = queueInfoList.get(1);
    Assert.assertNotNull(queueB);
    Assert.assertEquals("root.b", queueB.getQueuePath());
    Assert.assertEquals(89.5f, queueB.getCapacity(), 0.00001);
    CapacitySchedulerQueueInfoList queueBQueues = queueB.getQueues();
    Assert.assertNotNull(queueBQueues);
    ArrayList<CapacitySchedulerQueueInfo> queueInfoBList = queueBQueues.getQueueInfoList();
    Assert.assertNotNull(queueInfoBList);
    Assert.assertEquals(3, queueInfoBList.size());
    CapacitySchedulerQueueInfo queueB1 = queueInfoBList.get(0);
    Assert.assertNotNull(queueB1);
    Assert.assertEquals(79.2f, queueB1.getCapacity(), 0.00001);
    CapacitySchedulerQueueInfo queueB2 = queueInfoBList.get(1);
    Assert.assertNotNull(queueB2);
    Assert.assertEquals(0.8f, queueB2.getCapacity(), 0.00001);
    CapacitySchedulerQueueInfo queueB3 = queueInfoBList.get(2);
    Assert.assertNotNull(queueB3);
    Assert.assertEquals(20f, queueB3.getCapacity(), 0.00001);
  }

  private String getServiceAddress(InetSocketAddress address) {
    InetSocketAddress socketAddress = NetUtils.getConnectAddress(address);
    return socketAddress.getAddress().getHostAddress() + ":" + socketAddress.getPort();
  }

  /**
   * Testable policy generator overrides the methods that communicate
   * with the RM REST endpoint, allowing us to inject faked responses.
   */
  class TestablePolicyGenerator extends PolicyGenerator {

    TestablePolicyGenerator() {
      super(conf, gpgContext);
    }

    @Override
    protected Map<SubClusterId, Map<Class, Object>> getInfos(
        Map<SubClusterId, SubClusterInfo> activeSubClusters) {
      Map<SubClusterId, Map<Class, Object>> ret = new HashMap<>();
      for (SubClusterId id : activeSubClusters.keySet()) {
        if (!ret.containsKey(id)) {
          ret.put(id, new HashMap<>());
        }
        ret.get(id).put(ClusterMetricsInfo.class,
            clusterInfos.get(id).get(ClusterMetricsInfo.class));
      }
      return ret;
    }

    @Override
    protected Map<SubClusterId, SchedulerInfo> getSchedulerInfo(
        Map<SubClusterId, SubClusterInfo> activeSubClusters) {
      Map<SubClusterId, SchedulerInfo> ret = new HashMap<>();
      for (SubClusterId id : activeSubClusters.keySet()) {
        ret.put(id, schedulerInfos.get(id));
      }
      return ret;
    }
  }
}
