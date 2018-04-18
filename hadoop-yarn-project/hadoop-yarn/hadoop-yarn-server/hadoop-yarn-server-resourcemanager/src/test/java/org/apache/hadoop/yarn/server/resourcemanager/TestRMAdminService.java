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

package org.apache.hadoop.yarn.server.resourcemanager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.records.DecommissionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddToClusterNodeLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.resource.DynamicResourceConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_PROXY_USER_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.resource.DynamicResourceConfiguration.NODES;
import static org.apache.hadoop.yarn.server.resourcemanager.resource.DynamicResourceConfiguration.PREFIX;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto;

import static org.junit.Assert.assertTrue;

public class TestRMAdminService {

  private Configuration configuration;;
  private MockRM rm = null;
  private FileSystem fs;
  private Path workingPath;
  private Path tmpDir;

  static {
    YarnConfiguration.addDefaultResource(
        YarnConfiguration.CS_CONFIGURATION_FILE);
    YarnConfiguration.addDefaultResource(
        YarnConfiguration.DR_CONFIGURATION_FILE);
  }

  @Before
  public void setup() throws IOException {
    QueueMetrics.clearQueueMetrics();
    DefaultMetricsSystem.setMiniClusterMode(true);

    configuration = new YarnConfiguration();
    configuration.set(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class.getCanonicalName());
    fs = FileSystem.get(configuration);
    workingPath =
        new Path(new File("target", this.getClass().getSimpleName()
            + "-remoteDir").getAbsolutePath());
    configuration.set(YarnConfiguration.FS_BASED_RM_CONF_STORE,
        workingPath.toString());
    tmpDir = new Path(new File("target", this.getClass().getSimpleName()
        + "-tmpDir").getAbsolutePath());
    fs.delete(workingPath, true);
    fs.delete(tmpDir, true);
    fs.mkdirs(workingPath);
    fs.mkdirs(tmpDir);

    // reset the groups to what it default test settings
    MockUnixGroupsMapping.resetGroups();
  }

  @After
  public void tearDown() throws IOException {
    if (rm != null) {
      rm.stop();
    }
    fs.delete(workingPath, true);
    fs.delete(tmpDir, true);
  }

  @Test
  public void testAdminRefreshQueuesWithLocalConfigurationProvider()
      throws IOException, YarnException {
    rm = new MockRM(configuration);
    rm.init(configuration);
    rm.start();

    CapacityScheduler cs =
        (CapacityScheduler) rm.getRMContext().getScheduler();
    int maxAppsBefore = cs.getConfiguration().getMaximumSystemApplications();

    try {
      rm.adminService.refreshQueues(RefreshQueuesRequest.newInstance());
      Assert.assertEquals(maxAppsBefore, cs.getConfiguration()
          .getMaximumSystemApplications());
    } catch (Exception ex) {
      fail("Using localConfigurationProvider. Should not get any exception.");
    }
  }

  @Test
  public void testAdminRefreshQueuesWithFileSystemBasedConfigurationProvider()
      throws IOException, YarnException {
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");

    //upload default configurations
    uploadDefaultConfiguration();

    try {
      rm = new MockRM(configuration);
      rm.init(configuration);
      rm.start();
    } catch(Exception ex) {
      fail("Should not get any exceptions");
    }

    CapacityScheduler cs =
        (CapacityScheduler) rm.getRMContext().getScheduler();
    int maxAppsBefore = cs.getConfiguration().getMaximumSystemApplications();

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.set(CapacitySchedulerConfiguration.MAXIMUM_SYSTEM_APPLICATIONS,
        "5000");
    uploadConfiguration(csConf, "capacity-scheduler.xml");

    rm.adminService.refreshQueues(RefreshQueuesRequest.newInstance());

    int maxAppsAfter = cs.getConfiguration().getMaximumSystemApplications();
    Assert.assertEquals(maxAppsAfter, 5000);
    Assert.assertTrue(maxAppsAfter != maxAppsBefore);
  }

  @Test
  public void testAdminRefreshQueuesWithMutableSchedulerConfiguration() {
    configuration.set(YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);

    try {
      rm = new MockRM(configuration);
      rm.init(configuration);
      rm.start();
    } catch (Exception ex) {
      fail("Should not get any exceptions");
    }

    try {
      rm.adminService.refreshQueues(RefreshQueuesRequest.newInstance());
      fail("Expected exception while calling refreshQueues when scheduler" +
          " configuration is mutable.");
    } catch (Exception ex) {
      assertTrue(ex.getMessage().endsWith("Scheduler configuration is " +
          "mutable. refreshQueues is not allowed in this scenario."));
    }
  }

  @Test
  public void testAdminRefreshNodesWithoutConfiguration()
      throws IOException, YarnException {
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");

    // upload default configurations
    uploadDefaultConfiguration();

    try {
      rm = new MockRM(configuration);
      rm.init(configuration);
      rm.start();
    } catch (Exception ex) {
      fail("Should not get any exceptions");
    }

    try {
      rm.adminService
          .refreshNodesResources(RefreshNodesResourcesRequest.newInstance());
    } catch (Exception ex) {
      fail("Should not get any exceptions even when no configurations "
          + "are done for node resources refresh");
    }
  }

  @Test
  public void testRefreshNodesResourceWithFileSystemBasedConfigurationProvider()
      throws IOException, YarnException {
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");

    //upload default configurations
    uploadDefaultConfiguration();

    try {
      rm = new MockRM(configuration);
      rm.init(configuration);
      rm.start();
      rm.registerNode("h1:1234", 5120);
    } catch(Exception ex) {
      fail("Should not get any exceptions");
    }

    NodeId nid = NodeId.fromString("h1:1234");
    RMNode ni = rm.getRMContext().getRMNodes().get(nid);
    Resource resource = ni.getTotalCapability();
    Assert.assertEquals("<memory:5120, vCores:5>", resource.toString());

    DynamicResourceConfiguration drConf =
        new DynamicResourceConfiguration();
    drConf.set(PREFIX + NODES, "h1:1234");
    drConf.set(PREFIX + "h1:1234.vcores", "4");
    drConf.set(PREFIX + "h1:1234.memory", "4096");
    uploadConfiguration(drConf, "dynamic-resources.xml");

    rm.adminService.refreshNodesResources(
        RefreshNodesResourcesRequest.newInstance());
    rm.drainEvents();

    RMNode niAfter = rm.getRMContext().getRMNodes().get(nid);
    Resource resourceAfter = niAfter.getTotalCapability();
    Assert.assertEquals("<memory:4096, vCores:4>", resourceAfter.toString());
  }

  @Test
  public void testRefreshNodesResourceWithResourceReturnInRegistration()
      throws IOException, YarnException {
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");

    //upload default configurations
    uploadDefaultConfiguration();

    MockNM nm = null;
    try {
      rm = new MockRM(configuration);
      rm.init(configuration);
      rm.start();
      nm = rm.registerNode("h1:1234", 2048, 2);
    } catch(Exception ex) {
      fail("Should not get any exceptions");
    }

    NodeId nid = NodeId.fromString("h1:1234");
    RMNode ni = rm.getRMContext().getRMNodes().get(nid);
    Resource resource = ni.getTotalCapability();
    Assert.assertEquals("<memory:2048, vCores:2>", resource.toString());

    DynamicResourceConfiguration drConf =
        new DynamicResourceConfiguration();
    drConf.set(PREFIX + NODES, "h1:1234");
    drConf.set(PREFIX + "h1:1234.vcores", "4");
    drConf.set(PREFIX + "h1:1234.memory", "4096");
    uploadConfiguration(drConf, "dynamic-resources.xml");

    rm.adminService.refreshNodesResources(
        RefreshNodesResourcesRequest.newInstance());

    try {
      // register the same node again with original resource.
      // validate this will get new resource back;
      nm.registerNode();
    } catch (Exception ex) {
      fail("Should not get any exceptions");
    }

    RMNode niAfter = rm.getRMContext().getRMNodes().get(nid);
    Resource resourceAfter = niAfter.getTotalCapability();
    Assert.assertEquals("<memory:4096, vCores:4>", resourceAfter.toString());

    Assert.assertEquals(4096, nm.getMemory());
    Assert.assertEquals(4, nm.getvCores());
  }

  @Test
  public void testRefreshNodesResourceWithResourceReturnInHeartbeat()
      throws IOException, YarnException {
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");

    //upload default configurations
    uploadDefaultConfiguration();

    MockNM nm = null;
    try {
      rm = new MockRM(configuration);
      rm.init(configuration);
      rm.start();
      nm = rm.registerNode("h1:1234", 2048, 2);
    } catch(Exception ex) {
      fail("Should not get any exceptions");
    }

    NodeId nid = NodeId.fromString("h1:1234");
    RMNode ni = rm.getRMContext().getRMNodes().get(nid);
    Resource resource = ni.getTotalCapability();
    Assert.assertEquals("<memory:2048, vCores:2>", resource.toString());

    DynamicResourceConfiguration drConf =
        new DynamicResourceConfiguration();
    drConf.set(PREFIX + NODES, "h1:1234");
    drConf.set(PREFIX + "h1:1234.vcores", "4");
    drConf.set(PREFIX + "h1:1234.memory", "4096");
    uploadConfiguration(drConf, "dynamic-resources.xml");

    rm.adminService.refreshNodesResources(
        RefreshNodesResourcesRequest.newInstance());

    try {
      // NM-RM heartbeat, validate that this will get new resource back.
      nm.nodeHeartbeat(true);
    } catch (Exception ex) {
      fail("Should not get any exceptions");
    }

    RMNode niAfter = rm.getRMContext().getRMNodes().get(nid);
    Resource resourceAfter = niAfter.getTotalCapability();
    Assert.assertEquals("<memory:4096, vCores:4>", resourceAfter.toString());

    Assert.assertEquals(4096, nm.getMemory());
    Assert.assertEquals(4, nm.getvCores());
  }

  @Test
  public void testResourcePersistentForNMRegistrationWithNewResource()
      throws IOException, YarnException {
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");

    //upload default configurations
    uploadDefaultConfiguration();

    try {
      rm = new MockRM(configuration);
      rm.init(configuration);
      rm.start();
      rm.registerNode("h1:1234", 5120);
    } catch(Exception ex) {
      fail("Should not get any exceptions");
    }

    NodeId nid = NodeId.fromString("h1:1234");
    RMNode ni = rm.getRMContext().getRMNodes().get(nid);
    Resource resource = ni.getTotalCapability();
    Assert.assertEquals("<memory:5120, vCores:5>", resource.toString());

    DynamicResourceConfiguration drConf =
        new DynamicResourceConfiguration();
    drConf.set(PREFIX + NODES, "h1:1234");
    drConf.set(PREFIX + "h1:1234.vcores", "4");
    drConf.set(PREFIX + "h1:1234.memory", "4096");
    uploadConfiguration(drConf, "dynamic-resources.xml");

    rm.adminService.refreshNodesResources(
        RefreshNodesResourcesRequest.newInstance());

    try {
      // register the same node again with a different resource.
      // validate this won't work as resource cached in RM side.
      rm.registerNode("h1:1234", 8192, 8);
    } catch (Exception ex) {
      fail("Should not get any exceptions");
    }

    RMNode niAfter = rm.getRMContext().getRMNodes().get(nid);
    Resource resourceAfter = niAfter.getTotalCapability();
    Assert.assertEquals("<memory:4096, vCores:4>", resourceAfter.toString());

    // Replace original dr file with an empty dr file, and validate node
    // registration with new resources will take effective now.
    deleteOnRemoteFileSystem("dynamic-resources.xml");
    DynamicResourceConfiguration emptyDRConf =
        new DynamicResourceConfiguration();

    uploadConfiguration(emptyDRConf, "dynamic-resources.xml");
    rm.adminService.refreshNodesResources(
        RefreshNodesResourcesRequest.newInstance());
    try {
      // register the same node third time, this time the register resource
      // should work.
      rm.registerNode("h1:1234", 8192, 8);
    } catch (Exception ex) {
      fail("Should not get any exceptions");
    }

    niAfter = rm.getRMContext().getRMNodes().get(nid);
    resourceAfter = niAfter.getTotalCapability();
    // new resource in registration should take effective as we empty
    // dynamic resource file already.
    Assert.assertEquals("<memory:8192, vCores:8>", resourceAfter.toString());
  }

  @Test
  public void testAdminAclsWithLocalConfigurationProvider() {
    rm = new MockRM(configuration);
    rm.init(configuration);
    rm.start();

    try {
      rm.adminService.refreshAdminAcls(RefreshAdminAclsRequest.newInstance());
    } catch (Exception ex) {
      fail("Using localConfigurationProvider. Should not get any exception.");
    }
  }

  @Test
  public void testAdminAclsWithFileSystemBasedConfigurationProvider()
      throws IOException, YarnException {
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");

    //upload default configurations
    uploadDefaultConfiguration();

    try {
      rm = new MockRM(configuration);
      rm.init(configuration);
      rm.start();
    } catch(Exception ex) {
      fail("Should not get any exceptions");
    }

    String aclStringBefore =
        rm.adminService.getAccessControlList().getAclString().trim();

    YarnConfiguration yarnConf = new YarnConfiguration();
    yarnConf.set(YarnConfiguration.YARN_ADMIN_ACL, "world:anyone:rwcda");
    uploadConfiguration(yarnConf, "yarn-site.xml");

    rm.adminService.refreshAdminAcls(RefreshAdminAclsRequest.newInstance());

    String aclStringAfter =
        rm.adminService.getAccessControlList().getAclString().trim();

    Assert.assertTrue(!aclStringAfter.equals(aclStringBefore));
    Assert.assertEquals(aclStringAfter, "world:anyone:rwcda," +
        UserGroupInformation.getCurrentUser().getShortUserName());
  }

  @Test
  public void testServiceAclsRefreshWithLocalConfigurationProvider() {
    configuration.setBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, true);
    ResourceManager resourceManager = null;

    try {
      resourceManager = new ResourceManager();
      resourceManager.init(configuration);
      resourceManager.start();
      resourceManager.adminService.refreshServiceAcls(RefreshServiceAclsRequest
          .newInstance());
    } catch (Exception ex) {
      fail("Using localConfigurationProvider. Should not get any exception.");
    } finally {
      if (resourceManager != null) {
        resourceManager.stop();
      }
    }
  }

  @Test
  public void testServiceAclsRefreshWithFileSystemBasedConfigurationProvider()
      throws IOException, YarnException {
    configuration.setBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, true);
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");
    ResourceManager resourceManager = null;
    try {

      //upload default configurations
      uploadDefaultConfiguration();
      Configuration conf = new Configuration();
      conf.setBoolean(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, true);
      uploadConfiguration(conf, "core-site.xml");
      try {
        resourceManager = new ResourceManager();
        resourceManager.init(configuration);
        resourceManager.start();
      } catch (Exception ex) {
        fail("Should not get any exceptions");
      }

      String aclsString = "alice,bob users,wheel";
      Configuration newConf = new Configuration();
      newConf.set("security.applicationclient.protocol.acl", aclsString);
      uploadConfiguration(newConf, "hadoop-policy.xml");

      resourceManager.adminService.refreshServiceAcls(RefreshServiceAclsRequest
          .newInstance());

      // verify service Acls refresh for AdminService
      ServiceAuthorizationManager adminServiceServiceManager =
          resourceManager.adminService.getServer()
              .getServiceAuthorizationManager();
      verifyServiceACLsRefresh(adminServiceServiceManager,
          org.apache.hadoop.yarn.api.ApplicationClientProtocolPB.class,
          aclsString);

      // verify service ACLs refresh for ClientRMService
      ServiceAuthorizationManager clientRMServiceServiceManager =
          resourceManager.getRMContext().getClientRMService().getServer()
              .getServiceAuthorizationManager();
      verifyServiceACLsRefresh(clientRMServiceServiceManager,
          org.apache.hadoop.yarn.api.ApplicationClientProtocolPB.class,
          aclsString);

      // verify service ACLs refresh for ApplicationMasterService
      ServiceAuthorizationManager appMasterService =
          resourceManager.getRMContext().getApplicationMasterService()
              .getServer().getServiceAuthorizationManager();
      verifyServiceACLsRefresh(appMasterService,
          org.apache.hadoop.yarn.api.ApplicationClientProtocolPB.class,
          aclsString);

      // verify service ACLs refresh for ResourceTrackerService
      ServiceAuthorizationManager RTService =
          resourceManager.getRMContext().getResourceTrackerService()
              .getServer().getServiceAuthorizationManager();
      verifyServiceACLsRefresh(RTService,
          org.apache.hadoop.yarn.api.ApplicationClientProtocolPB.class,
          aclsString);
    } finally {
      if (resourceManager != null) {
        resourceManager.stop();
      }
    }
  }

  private void verifyServiceACLsRefresh(ServiceAuthorizationManager manager,
      Class<?> protocol, String aclString) {
    for (Class<?> protocolClass : manager.getProtocolsWithAcls()) {
      AccessControlList accessList =
          manager.getProtocolsAcls(protocolClass);
      if (protocolClass == protocol) {
        Assert.assertEquals(accessList.getAclString(),
            aclString);
      } else {
        Assert.assertEquals(accessList.getAclString(), "*");
      }
    }
  }

  @Test
  public void
      testRefreshSuperUserGroupsWithLocalConfigurationProvider() {
    rm = new MockRM(configuration);
    rm.init(configuration);
    rm.start();

    try {
      rm.adminService.refreshSuperUserGroupsConfiguration(
          RefreshSuperUserGroupsConfigurationRequest.newInstance());
    } catch (Exception ex) {
      fail("Using localConfigurationProvider. Should not get any exception.");
    }
  }

  @Test
  public void
      testRefreshSuperUserGroupsWithFileSystemBasedConfigurationProvider()
      throws IOException, YarnException {
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");

    //upload default configurations
    uploadDefaultConfiguration();

    try {
      rm = new MockRM(configuration);
      rm.init(configuration);
      rm.start();
    } catch(Exception ex) {
      fail("Should not get any exceptions");
    }

    Configuration coreConf = new Configuration(false);
    coreConf.set("hadoop.proxyuser.test.groups", "test_groups");
    coreConf.set("hadoop.proxyuser.test.hosts", "test_hosts");
    uploadConfiguration(coreConf, "core-site.xml");

    rm.adminService.refreshSuperUserGroupsConfiguration(
        RefreshSuperUserGroupsConfigurationRequest.newInstance());
    Assert.assertTrue(ProxyUsers.getDefaultImpersonationProvider().getProxyGroups()
        .get("hadoop.proxyuser.test.groups").size() == 1);
    Assert.assertTrue(ProxyUsers.getDefaultImpersonationProvider().getProxyGroups()
        .get("hadoop.proxyuser.test.groups").contains("test_groups"));

    Assert.assertTrue(ProxyUsers.getDefaultImpersonationProvider().getProxyHosts()
        .get("hadoop.proxyuser.test.hosts").size() == 1);
    Assert.assertTrue(ProxyUsers.getDefaultImpersonationProvider().getProxyHosts()
        .get("hadoop.proxyuser.test.hosts").contains("test_hosts"));

    Configuration yarnConf = new Configuration(false);
    yarnConf.set(RM_PROXY_USER_PREFIX + "test.groups", "test_groups_1");
    yarnConf.set(RM_PROXY_USER_PREFIX + "test.hosts", "test_hosts_1");
    uploadConfiguration(yarnConf, "yarn-site.xml");

    // RM specific configs will overwrite the common ones
    rm.adminService.refreshSuperUserGroupsConfiguration(
        RefreshSuperUserGroupsConfigurationRequest.newInstance());
    Assert.assertTrue(ProxyUsers.getDefaultImpersonationProvider().getProxyGroups()
        .get("hadoop.proxyuser.test.groups").size() == 1);
    Assert.assertTrue(ProxyUsers.getDefaultImpersonationProvider().getProxyGroups()
        .get("hadoop.proxyuser.test.groups").contains("test_groups_1"));

    Assert.assertTrue(ProxyUsers.getDefaultImpersonationProvider().getProxyHosts()
        .get("hadoop.proxyuser.test.hosts").size() == 1);
    Assert.assertTrue(ProxyUsers.getDefaultImpersonationProvider().getProxyHosts()
        .get("hadoop.proxyuser.test.hosts").contains("test_hosts_1"));
  }

  @Test
  public void testRefreshUserToGroupsMappingsWithLocalConfigurationProvider() {
    rm = new MockRM(configuration);
    rm.init(configuration);
    rm.start();
    try {
      rm.adminService
          .refreshUserToGroupsMappings(RefreshUserToGroupsMappingsRequest
              .newInstance());
    } catch (Exception ex) {
      fail("Using localConfigurationProvider. Should not get any exception.");
    }
  }

  @Test
  public void
      testRefreshUserToGroupsMappingsWithFileSystemBasedConfigurationProvider()
          throws IOException, YarnException {
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");

    String[] defaultTestUserGroups = {"dummy_group1", "dummy_group2"};
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting
        ("dummyUser", defaultTestUserGroups);

    String user = ugi.getUserName();
    List<String> groupWithInit = new ArrayList<String>(2);
     for(int i = 0; i < ugi.getGroupNames().length; i++ ) {
       groupWithInit.add(ugi.getGroupNames()[i]);
     }

    // upload default configurations
    uploadDefaultConfiguration();
    Configuration conf = new Configuration();
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        MockUnixGroupsMapping.class,
        GroupMappingServiceProvider.class);
    uploadConfiguration(conf, "core-site.xml");

    try {
      rm = new MockRM(configuration);
      rm.init(configuration);
      rm.start();
    } catch (Exception ex) {
      fail("Should not get any exceptions");
    }

    // Make sure RM will use the updated GroupMappingServiceProvider
    Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(conf).refresh();
    List<String> groupBefore =
        new ArrayList<String>(Groups.getUserToGroupsMappingService(
            configuration).getGroups(user));
    Assert.assertTrue(groupBefore.contains("test_group_A")
        && groupBefore.contains("test_group_B")
        && groupBefore.contains("test_group_C") && groupBefore.size() == 3);
    Assert.assertTrue(groupWithInit.size() != groupBefore.size());
    Assert.assertFalse(groupWithInit.contains("test_group_A")
        || groupWithInit.contains("test_group_B")
        || groupWithInit.contains("test_group_C"));

    // update the groups
    MockUnixGroupsMapping.updateGroups();

    rm.adminService
        .refreshUserToGroupsMappings(RefreshUserToGroupsMappingsRequest
            .newInstance());
    List<String> groupAfter =
        Groups.getUserToGroupsMappingService(configuration).getGroups(user);

    // should get the updated groups
    Assert.assertTrue(groupAfter.contains("test_group_D")
        && groupAfter.contains("test_group_E")
        && groupAfter.contains("test_group_F") && groupAfter.size() == 3);

  }

  @Test
  public void testRefreshNodesWithLocalConfigurationProvider() {
    rm = new MockRM(configuration);
    rm.init(configuration);
    rm.start();

    try {
      rm.adminService.refreshNodes(RefreshNodesRequest
          .newInstance(DecommissionType.NORMAL));
    } catch (Exception ex) {
      fail("Using localConfigurationProvider. Should not get any exception.");
    }
  }

  @Test
  public void testRefreshNodesWithFileSystemBasedConfigurationProvider()
      throws IOException, YarnException {
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");

    // upload default configurations
    uploadDefaultConfiguration();

    try {
      rm = new MockRM(configuration);
      rm.init(configuration);
      rm.start();
    } catch (Exception ex) {
      fail("Should not get any exceptions");
    }

    final File excludeHostsFile = new File(tmpDir.toString(), "excludeHosts");
    if (excludeHostsFile.exists()) {
      excludeHostsFile.delete();
    }
    if (!excludeHostsFile.createNewFile()) {
      Assert.fail("Can not create " + "excludeHosts");
    }
    PrintWriter fileWriter = new PrintWriter(excludeHostsFile);
    fileWriter.write("0.0.0.0:123");
    fileWriter.close();

    uploadToRemoteFileSystem(new Path(excludeHostsFile.getAbsolutePath()));

    Configuration yarnConf = new YarnConfiguration();
    yarnConf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH, this.workingPath
        + "/excludeHosts");
    uploadConfiguration(yarnConf, YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);

    rm.adminService.refreshNodes(RefreshNodesRequest
        .newInstance(DecommissionType.NORMAL));
    Set<String> excludeHosts =
        rm.getNodesListManager().getHostsReader().getExcludedHosts();
    Assert.assertTrue(excludeHosts.size() == 1);
    Assert.assertTrue(excludeHosts.contains("0.0.0.0:123"));
  }

  @Test
  public void testRMHAWithFileSystemBasedConfiguration() throws IOException,
      YarnException {
    StateChangeRequestInfo requestInfo = new StateChangeRequestInfo(
        HAServiceProtocol.RequestSource.REQUEST_BY_USER);
    updateConfigurationForRMHA();
    Configuration conf1 = new Configuration(configuration);
    conf1.set(YarnConfiguration.RM_HA_ID, "rm1");
    Configuration conf2 = new Configuration(configuration);
    conf2.set(YarnConfiguration.RM_HA_ID, "rm2");

    // upload default configurations
    uploadDefaultConfiguration();

    MockRM rm1 = null;
    MockRM rm2 = null;
    try {
      rm1 = new MockRM(conf1);
      rm1.init(conf1);
      rm1.start();
      Assert.assertTrue(rm1.getRMContext().getHAServiceState()
          == HAServiceState.STANDBY);

      rm2 = new MockRM(conf2);
      rm2.init(conf1);
      rm2.start();
      Assert.assertTrue(rm2.getRMContext().getHAServiceState()
          == HAServiceState.STANDBY);

      rm1.adminService.transitionToActive(requestInfo);
      Assert.assertTrue(rm1.getRMContext().getHAServiceState()
          == HAServiceState.ACTIVE);

      CapacitySchedulerConfiguration csConf =
          new CapacitySchedulerConfiguration();
      csConf.set(CapacitySchedulerConfiguration.MAXIMUM_SYSTEM_APPLICATIONS,
          "5000");
      uploadConfiguration(csConf, "capacity-scheduler.xml");

      rm1.adminService.refreshQueues(RefreshQueuesRequest.newInstance());

      int maxApps =
          ((CapacityScheduler) rm1.getRMContext().getScheduler())
              .getConfiguration().getMaximumSystemApplications();
      Assert.assertEquals(maxApps, 5000);

      // Before failover happens, the maxApps is
      // still the default value on the standby rm : rm2
      int maxAppsBeforeFailOver =
          ((CapacityScheduler) rm2.getRMContext().getScheduler())
              .getConfiguration().getMaximumSystemApplications();
      Assert.assertEquals(maxAppsBeforeFailOver, 10000);

      // Do the failover
      rm1.adminService.transitionToStandby(requestInfo);
      rm2.adminService.transitionToActive(requestInfo);
      Assert.assertTrue(rm1.getRMContext().getHAServiceState()
          == HAServiceState.STANDBY);
      Assert.assertTrue(rm2.getRMContext().getHAServiceState()
          == HAServiceState.ACTIVE);

      int maxAppsAfter =
          ((CapacityScheduler) rm2.getRMContext().getScheduler())
              .getConfiguration().getMaximumSystemApplications();

      Assert.assertEquals(maxAppsAfter, 5000);
    } finally {
      if (rm1 != null) {
        rm1.stop();
      }
      if (rm2 != null) {
        rm2.stop();
      }
    }
  }

  /**
   * Test that a configuration with no leader election configured fails.
   */
  @Test
  public void testHAConfWithoutLeaderElection() {
    Configuration conf = new Configuration(configuration);

    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, true);
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_EMBEDDED, false);
    conf.setBoolean(YarnConfiguration.CURATOR_LEADER_ELECTOR, false);
    conf.set(YarnConfiguration.RM_HA_IDS, "rm1,rm2");

    int base = 100;

    for (String confKey :
        YarnConfiguration.getServiceAddressConfKeys(configuration)) {
      conf.set(HAUtil.addSuffix(confKey, "rm1"), "0.0.0.0:"
          + (base + 20));
      conf.set(HAUtil.addSuffix(confKey, "rm2"), "0.0.0.0:"
          + (base + 40));
      base = base * 2;
    }

    conf.set(YarnConfiguration.RM_HA_ID, "rm1");

    checkBadConfiguration(conf);
  }

  /**
   * Test that a configuration with a single RM fails.
   */
  @Test
  public void testHAConfWithSingleRMID() {
    Configuration conf = new Configuration(configuration);

    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, true);
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_EMBEDDED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, "rm1");

    int base = 100;

    for (String confKey :
        YarnConfiguration.getServiceAddressConfKeys(configuration)) {
      conf.set(HAUtil.addSuffix(confKey, "rm1"), "0.0.0.0:"
          + (base + 20));
      base = base * 2;
    }

    conf.set(YarnConfiguration.RM_HA_ID, "rm1");

    checkBadConfiguration(conf);
  }

  /**
   * Test that a configuration with no service information fails.
   */
  @Test
  public void testHAConfWithoutServiceInfo() {
    Configuration conf = new Configuration(configuration);

    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, true);
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_EMBEDDED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, "rm1,rm2");

    conf.set(YarnConfiguration.RM_HA_ID, "rm1");

    checkBadConfiguration(conf);
  }

  @Test
  public void testRMStartsWithoutConfigurationFilesProvided() {
    // enable FileSystemBasedConfigurationProvider without uploading
    // any configuration files into Remote File System.
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");

    // The configurationProvider will return NULL instead of
    // throwing out Exceptions, if there are no configuration files provided.
    // RM will not load the remote Configuration files,
    // and should start successfully.
    try {
      rm = new MockRM(configuration);
      rm.init(configuration);
      rm.start();
    } catch (Exception ex) {
      fail("Should not get any exceptions");
    }

  }

  @Test
  public void testRMInitialsWithFileSystemBasedConfigurationProvider()
      throws Exception {
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");

    // upload configurations
    final File excludeHostsFile = new File(tmpDir.toString(), "excludeHosts");
    if (excludeHostsFile.exists()) {
      excludeHostsFile.delete();
    }
    if (!excludeHostsFile.createNewFile()) {
      Assert.fail("Can not create " + "excludeHosts");
    }
    PrintWriter fileWriter = new PrintWriter(excludeHostsFile);
    fileWriter.write("0.0.0.0:123");
    fileWriter.close();
    uploadToRemoteFileSystem(new Path(excludeHostsFile.getAbsolutePath()));

    YarnConfiguration yarnConf = new YarnConfiguration();
    yarnConf.set(YarnConfiguration.YARN_ADMIN_ACL, "world:anyone:rwcda");
    yarnConf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH, this.workingPath
        + "/excludeHosts");
    uploadConfiguration(yarnConf, "yarn-site.xml");

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.set(CapacitySchedulerConfiguration.MAXIMUM_SYSTEM_APPLICATIONS,
        "5000");
    uploadConfiguration(csConf, "capacity-scheduler.xml");

    String aclsString = "alice,bob users,wheel";
    Configuration newConf = new Configuration();
    newConf.set("security.applicationclient.protocol.acl", aclsString);
    uploadConfiguration(newConf, "hadoop-policy.xml");

    Configuration conf = new Configuration();
    conf.setBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, true);
    conf.set("hadoop.proxyuser.test.groups", "test_groups");
    conf.set("hadoop.proxyuser.test.hosts", "test_hosts");
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        MockUnixGroupsMapping.class,
        GroupMappingServiceProvider.class);
    uploadConfiguration(conf, "core-site.xml");

    // update the groups
    MockUnixGroupsMapping.updateGroups();

    ResourceManager resourceManager = null;
    try {
      try {
        resourceManager = new ResourceManager();
        resourceManager.init(configuration);
        resourceManager.start();
      } catch (Exception ex) {
        fail("Should not get any exceptions");
      }

      // validate values for excludeHosts
      Set<String> excludeHosts =
          resourceManager.getRMContext().getNodesListManager()
              .getHostsReader().getExcludedHosts();
      Assert.assertTrue(excludeHosts.size() == 1);
      Assert.assertTrue(excludeHosts.contains("0.0.0.0:123"));

      // validate values for admin-acls
      String aclStringAfter =
          resourceManager.adminService.getAccessControlList()
              .getAclString().trim();
      Assert.assertEquals(aclStringAfter, "world:anyone:rwcda," +
          UserGroupInformation.getCurrentUser().getShortUserName());

      // validate values for queue configuration
      CapacityScheduler cs =
          (CapacityScheduler) resourceManager.getRMContext().getScheduler();
      int maxAppsAfter = cs.getConfiguration().getMaximumSystemApplications();
      Assert.assertEquals(maxAppsAfter, 5000);

      // verify service Acls for AdminService
      ServiceAuthorizationManager adminServiceServiceManager =
          resourceManager.adminService.getServer()
              .getServiceAuthorizationManager();
      verifyServiceACLsRefresh(adminServiceServiceManager,
          org.apache.hadoop.yarn.api.ApplicationClientProtocolPB.class,
          aclsString);

      // verify service ACLs for ClientRMService
      ServiceAuthorizationManager clientRMServiceServiceManager =
          resourceManager.getRMContext().getClientRMService().getServer()
              .getServiceAuthorizationManager();
      verifyServiceACLsRefresh(clientRMServiceServiceManager,
          org.apache.hadoop.yarn.api.ApplicationClientProtocolPB.class,
          aclsString);

      // verify service ACLs for ApplicationMasterService
      ServiceAuthorizationManager appMasterService =
          resourceManager.getRMContext().getApplicationMasterService()
              .getServer().getServiceAuthorizationManager();
      verifyServiceACLsRefresh(appMasterService,
          org.apache.hadoop.yarn.api.ApplicationClientProtocolPB.class,
          aclsString);

      // verify service ACLs for ResourceTrackerService
      ServiceAuthorizationManager RTService =
          resourceManager.getRMContext().getResourceTrackerService()
              .getServer().getServiceAuthorizationManager();
      verifyServiceACLsRefresh(RTService,
          org.apache.hadoop.yarn.api.ApplicationClientProtocolPB.class,
          aclsString);

      // verify ProxyUsers and ProxyHosts
      ProxyUsers.refreshSuperUserGroupsConfiguration(configuration);
      Assert.assertTrue(ProxyUsers.getDefaultImpersonationProvider().getProxyGroups()
          .get("hadoop.proxyuser.test.groups").size() == 1);
      Assert.assertTrue(ProxyUsers.getDefaultImpersonationProvider().getProxyGroups()
          .get("hadoop.proxyuser.test.groups").contains("test_groups"));

      Assert.assertTrue(ProxyUsers.getDefaultImpersonationProvider().getProxyHosts()
          .get("hadoop.proxyuser.test.hosts").size() == 1);
      Assert.assertTrue(ProxyUsers.getDefaultImpersonationProvider().getProxyHosts()
          .get("hadoop.proxyuser.test.hosts").contains("test_hosts"));

      // verify UserToGroupsMappings
      Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(conf).refresh();
      List<String> groupAfter =
          Groups.getUserToGroupsMappingService(configuration).getGroups(
              UserGroupInformation.getCurrentUser().getUserName());
      Assert.assertTrue(groupAfter.contains("test_group_D")
          && groupAfter.contains("test_group_E")
          && groupAfter.contains("test_group_F") && groupAfter.size() == 3);
    } finally {
      if (resourceManager != null) {
        resourceManager.stop();
      }
    }
  }

  /* For verifying fix for YARN-3804 */
  @Test
  public void testRefreshAclWithDaemonUser() throws Exception {
    String daemonUser =
        UserGroupInformation.getCurrentUser().getShortUserName();
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");

    uploadDefaultConfiguration();
    YarnConfiguration yarnConf = new YarnConfiguration();
    yarnConf.set(YarnConfiguration.YARN_ADMIN_ACL, daemonUser + "xyz");
    uploadConfiguration(yarnConf, "yarn-site.xml");

    try {
      rm = new MockRM(configuration);
      rm.init(configuration);
      rm.start();
    } catch(Exception ex) {
      fail("Should not get any exceptions");
    }

    assertEquals(daemonUser + "xyz," + daemonUser,
        rm.adminService.getAccessControlList().getAclString().trim());

    yarnConf = new YarnConfiguration();
    yarnConf.set(YarnConfiguration.YARN_ADMIN_ACL, daemonUser + "abc");
    uploadConfiguration(yarnConf, "yarn-site.xml");
    try {
      rm.adminService.refreshAdminAcls(RefreshAdminAclsRequest.newInstance());
    } catch (YarnException e) {
      if (e.getCause() != null &&
          e.getCause() instanceof AccessControlException) {
        fail("Refresh should not have failed due to incorrect ACL");
      }
      throw e;
    }

    assertEquals(daemonUser + "abc," + daemonUser,
        rm.adminService.getAccessControlList().getAclString().trim());
  }

  @Test
  public void testModifyLabelsOnNodesWithDistributedConfigurationDisabled()
      throws IOException, YarnException {
    // create RM and set it's ACTIVE
    MockRM rm = new MockRM();
    ((RMContextImpl) rm.getRMContext())
        .setHAServiceState(HAServiceState.ACTIVE);
    RMNodeLabelsManager labelMgr = rm.rmContext.getNodeLabelManager();

    // by default, distributed configuration for node label is disabled, this
    // should pass
    labelMgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    rm.adminService.replaceLabelsOnNode(ReplaceLabelsOnNodeRequest
        .newInstance(ImmutableMap.of(NodeId.newInstance("host", 0),
            (Set<String>) ImmutableSet.of("x"))));
    rm.close();
  }

  @Test(expected = YarnException.class)
  public void testModifyLabelsOnNodesWithCentralizedConfigurationDisabled()
      throws IOException, YarnException {
    // create RM and set it's ACTIVE, and set distributed node label
    // configuration to true
    MockRM rm = new MockRM();
    rm.adminService.isCentralizedNodeLabelConfiguration = false;

    ((RMContextImpl) rm.getRMContext())
        .setHAServiceState(HAServiceState.ACTIVE);
    RMNodeLabelsManager labelMgr = rm.rmContext.getNodeLabelManager();

    // by default, distributed configuration for node label is disabled, this
    // should pass
    labelMgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    rm.adminService.replaceLabelsOnNode(ReplaceLabelsOnNodeRequest
        .newInstance(ImmutableMap.of(NodeId.newInstance("host", 0),
            (Set<String>) ImmutableSet.of("x"))));
    rm.close();
  }

  @Test
  public void testModifyLabelsOnUnknownNodes() throws IOException,
      YarnException {
    // create RM and set it's ACTIVE, and set distributed node label
    // configuration to true
    rm = new MockRM();

    ((RMContextImpl) rm.getRMContext())
        .setHAServiceState(HAServiceState.ACTIVE);
    Map<NodeId, RMNode> rmNodes = rm.getRMContext().getRMNodes();
    rmNodes.put(NodeId.newInstance("host1", 1111),
        new RMNodeImpl(null, rm.getRMContext(), "host1", 0, 0, null, null,
                null));
    rmNodes.put(NodeId.newInstance("host2", 2222),
            new RMNodeImpl(null, rm.getRMContext(), "host2", 0, 0, null, null,
                null));
    rmNodes.put(NodeId.newInstance("host3", 3333),
            new RMNodeImpl(null, rm.getRMContext(), "host3", 0, 0, null, null,
                null));
    Map<NodeId, RMNode> rmInactiveNodes = rm.getRMContext()
        .getInactiveRMNodes();
    rmInactiveNodes.put(NodeId.newInstance("host4", 4444),
        new RMNodeImpl(null, rm.getRMContext(), "host4", 0, 0, null, null,
                null));
    RMNodeLabelsManager labelMgr = rm.rmContext.getNodeLabelManager();

    // by default, distributed configuration for node label is disabled, this
    // should pass
    labelMgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x",
        "y"));
    // replace known node
    ReplaceLabelsOnNodeRequest request1 = ReplaceLabelsOnNodeRequest
        .newInstance(ImmutableMap.of(NodeId.newInstance("host1", 1111),
            (Set<String>) ImmutableSet.of("x")));
    request1.setFailOnUnknownNodes(true);
    try {
      rm.adminService.replaceLabelsOnNode(request1);
    } catch (Exception ex) {
      fail("should not fail on known node");
    }

    // replace known node with wildcard port
    ReplaceLabelsOnNodeRequest request2 = ReplaceLabelsOnNodeRequest
        .newInstance(ImmutableMap.of(NodeId.newInstance("host1", 0),
            (Set<String>) ImmutableSet.of("x")));
    request2.setFailOnUnknownNodes(true);
    try {
      rm.adminService.replaceLabelsOnNode(request2);
    } catch (Exception ex) {
      fail("should not fail on known node");
    }

    // replace unknown node
    ReplaceLabelsOnNodeRequest request3 = ReplaceLabelsOnNodeRequest
        .newInstance(ImmutableMap.of(NodeId.newInstance("host5", 0),
            (Set<String>) ImmutableSet.of("x")));
    request3.setFailOnUnknownNodes(true);
    try {
      rm.adminService.replaceLabelsOnNode(request3);
      fail("Should fail on unknown node");
    } catch (Exception ex) {
    }

    // replace known node but wrong port
    ReplaceLabelsOnNodeRequest request4 = ReplaceLabelsOnNodeRequest
        .newInstance(ImmutableMap.of(NodeId.newInstance("host2", 1111),
            (Set<String>) ImmutableSet.of("x")));
    request4.setFailOnUnknownNodes(true);
    try {
      rm.adminService.replaceLabelsOnNode(request4);
      fail("Should fail on node with wrong port");
    } catch (Exception ex) {
    }

    // replace non-exist node but not check
    ReplaceLabelsOnNodeRequest request5 = ReplaceLabelsOnNodeRequest
        .newInstance(ImmutableMap.of(NodeId.newInstance("host5", 0),
            (Set<String>) ImmutableSet.of("x")));
    request5.setFailOnUnknownNodes(false);
    try {
      rm.adminService.replaceLabelsOnNode(request5);
    } catch (Exception ex) {
      fail("Should not fail on unknown node when "
          + "fail-on-unkown-nodes is set false");
    }

    // replace on inactive node
    ReplaceLabelsOnNodeRequest request6 = ReplaceLabelsOnNodeRequest
        .newInstance(ImmutableMap.of(NodeId.newInstance("host4", 0),
            (Set<String>) ImmutableSet.of("x")));
    request6.setFailOnUnknownNodes(true);
    try {
      rm.adminService.replaceLabelsOnNode(request6);
    } catch (Exception ex) {
      fail("should not fail on inactive node");
    }

    rm.close();
  }

  @Test
  public void testRemoveClusterNodeLabelsWithCentralizedConfigurationDisabled()
      throws IOException, YarnException {
    // create RM and set it's ACTIVE
    MockRM rm = new MockRM();
    ((RMContextImpl) rm.getRMContext())
        .setHAServiceState(HAServiceState.ACTIVE);
    RMNodeLabelsManager labelMgr = rm.rmContext.getNodeLabelManager();
    rm.adminService.isCentralizedNodeLabelConfiguration = false;

    // by default, distributed configuration for node label is disabled, this
    // should pass
    labelMgr.addToCluserNodeLabelsWithDefaultExclusivity(ImmutableSet.of("x", "y"));
    rm.adminService
        .removeFromClusterNodeLabels(RemoveFromClusterNodeLabelsRequest
            .newInstance((Set<String>) ImmutableSet.of("x")));

    Set<String> clusterNodeLabels = labelMgr.getClusterNodeLabelNames();
    assertEquals(1,clusterNodeLabels.size());
    rm.close();
  }

  @Test(timeout = 30000)
  public void testAdminRefreshClusterMaxPriority() throws Exception,
      YarnException {
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");

    uploadDefaultConfiguration();
    YarnConfiguration yarnConf = new YarnConfiguration();
    yarnConf.set(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY, "5");
    uploadConfiguration(yarnConf, "yarn-site.xml");

    rm = new MockRM(configuration);
    rm.init(configuration);
    rm.start();

    CapacityScheduler cs = (CapacityScheduler) rm.getRMContext().getScheduler();
    Assert.assertEquals(5, cs.getMaxClusterLevelAppPriority().getPriority());

    yarnConf = new YarnConfiguration();
    yarnConf
        .set(YarnConfiguration.MAX_CLUSTER_LEVEL_APPLICATION_PRIORITY, "10");
    uploadConfiguration(yarnConf, "yarn-site.xml");

    try {
      rm.adminService
          .refreshClusterMaxPriority(RefreshClusterMaxPriorityRequest
              .newInstance());
      Assert.assertEquals(10, cs.getMaxClusterLevelAppPriority().getPriority());
    } catch (Exception ex) {
      fail("Could not refresh cluster max priority.");
    }
  }

  private String writeConfigurationXML(Configuration conf, String confXMLName)
      throws IOException {
    DataOutputStream output = null;
    try {
      final File confFile = new File(tmpDir.toString(), confXMLName);
      if (confFile.exists()) {
        confFile.delete();
      }
      if (!confFile.createNewFile()) {
        Assert.fail("Can not create " + confXMLName);
      }
      output = new DataOutputStream(
          new FileOutputStream(confFile));
      conf.writeXml(output);
      return confFile.getAbsolutePath();
    } finally {
      if (output != null) {
        output.close();
      }
    }
  }

  private void uploadToRemoteFileSystem(Path filePath)
      throws IOException {
    fs.copyFromLocalFile(filePath, workingPath);
  }

  private void uploadConfiguration(Configuration conf, String confFileName)
      throws IOException {
    String csConfFile = writeConfigurationXML(conf, confFileName);
    // upload the file into Remote File System
    uploadToRemoteFileSystem(new Path(csConfFile));
  }

  private void deleteOnRemoteFileSystem(String fileName)
      throws IOException {
    fs.delete(new Path(workingPath, fileName));
  }

  private void uploadDefaultConfiguration() throws IOException {
    Configuration conf = new Configuration();
    uploadConfiguration(conf, "core-site.xml");

    YarnConfiguration yarnConf = new YarnConfiguration();
    yarnConf.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");
    uploadConfiguration(yarnConf, "yarn-site.xml");

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    uploadConfiguration(csConf, "capacity-scheduler.xml");

    Configuration hadoopPolicyConf = new Configuration(false);
    hadoopPolicyConf
        .addResource(YarnConfiguration.HADOOP_POLICY_CONFIGURATION_FILE);
    uploadConfiguration(hadoopPolicyConf, "hadoop-policy.xml");
  }

  private static class MockUnixGroupsMapping implements
      GroupMappingServiceProvider {

    private static List<String> group = new ArrayList<String>();

    @Override
    public List<String> getGroups(String user) throws IOException {
      return group;
    }

    @Override
    public void cacheGroupsRefresh() throws IOException {
      // Do nothing
    }

    @Override
    public void cacheGroupsAdd(List<String> groups) throws IOException {
      // Do nothing
    }

    public static void updateGroups() {
      group.clear();
      group.add("test_group_D");
      group.add("test_group_E");
      group.add("test_group_F");
    }
    
    public static void resetGroups() {
      group.clear();
      group.add("test_group_A");
      group.add("test_group_B");
      group.add("test_group_C");
    }
  }

  @Test
  public void testSecureRMBecomeActive() throws IOException,
      YarnException {
    StateChangeRequestInfo requestInfo = new StateChangeRequestInfo(
        HAServiceProtocol.RequestSource.REQUEST_BY_USER);
    updateConfigurationForRMHA();
    configuration.setBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, true);
    configuration.set(YarnConfiguration.RM_HA_ID, "rm1");
    // upload default configurations
    uploadDefaultConfiguration();

    ResourceManager resourceManager = new ResourceManager();
    try {
      resourceManager.init(configuration);
      resourceManager.start();
      Assert.assertTrue(resourceManager.getRMContext().getHAServiceState()
          == HAServiceState.STANDBY);
      resourceManager.adminService.transitionToActive(requestInfo);
    } finally {
        resourceManager.close();
    }
  }

  private void updateConfigurationForRMHA() {
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");
    configuration.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    configuration.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    configuration.set(YarnConfiguration.RM_HA_IDS, "rm1,rm2");

    int base = 1500;
    for (String confKey : YarnConfiguration
        .getServiceAddressConfKeys(configuration)) {
      configuration.set(HAUtil.addSuffix(confKey, "rm1"),
          "0.0.0.0:" + (base + 20));
      configuration.set(HAUtil.addSuffix(confKey, "rm2"),
          "0.0.0.1:" + (base + 40));
      base = base * 2;
    }
  }

  /**
   * This method initializes an RM with the given configuration and expects it
   * to fail with a configuration error.
   *
   * @param conf the {@link Configuration} to use
   */
  private void checkBadConfiguration(Configuration conf) {
    MockRM rm1 = null;

    conf.set(YarnConfiguration.RM_HA_ID, "rm1");

    try {
      rm1 = new MockRM(conf);
      rm1.init(conf);
      fail("The RM allowed an invalid configuration");
    } catch (YarnRuntimeException e) {
      assertTrue("The RM initialization threw an unexpected exception",
          e.getMessage().startsWith(HAUtil.BAD_CONFIG_MESSAGE_PREFIX));
    }
  }

  @Test(timeout = 30000)
  public void testAdminAddToClusterNodeLabelsWithDeprecatedAPIs()
      throws Exception, YarnException {
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");

    uploadDefaultConfiguration();

    rm = new MockRM(configuration) {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(this.rmContext, scheduler, this.rmAppManager,
            this.applicationACLsManager, this.queueACLsManager,
            this.getRMContext().getRMDelegationTokenSecretManager());
      };
    };
    rm.init(configuration);
    rm.start();

    try {
      List<String> list = new ArrayList<String>();
      list.add("a");
      list.add("b");
      AddToClusterNodeLabelsRequestProto proto = AddToClusterNodeLabelsRequestProto
          .newBuilder().addAllDeprecatedNodeLabels(list).build();
      AddToClusterNodeLabelsRequestPBImpl protoImpl = new AddToClusterNodeLabelsRequestPBImpl(
          proto);
      rm.adminService
          .addToClusterNodeLabels((AddToClusterNodeLabelsRequest) protoImpl);
    } catch (Exception ex) {
      fail("Could not update node labels." + ex);
    }

    // Create a client.
    Configuration conf = new Configuration();
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = rm.getClientRMService().getBindAddress();
    ApplicationClientProtocol client = (ApplicationClientProtocol) rpc
        .getProxy(ApplicationClientProtocol.class, rmAddress, conf);

    // Get node labels collection
    GetClusterNodeLabelsResponse response = client
        .getClusterNodeLabels(GetClusterNodeLabelsRequest.newInstance());
    NodeLabel labelX = NodeLabel.newInstance("a");
    NodeLabel labelY = NodeLabel.newInstance("b");
    Assert.assertTrue(
        response.getNodeLabelList().containsAll(Arrays.asList(labelX, labelY)));
  }
}
