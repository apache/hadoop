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

package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.hdfs.tools.GetGroups;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.DefaultImpersonationProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test RefreshUserMappingsProtocol and GetUserMappingsProtocol with Routers.
 */
public class TestRouterUserMappings {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestRouterUserMappings.class);

  private MiniRouterDFSCluster cluster;
  private Router router;
  private Configuration conf;
  private static final long GROUP_REFRESH_TIMEOUT_SEC = 1L;
  private static final String ROUTER_NS = "rbfns";
  private static final String HDFS_SCHEMA = "hdfs://";
  private static final String LOOPBACK_ADDRESS = "127.0.0.1";

  private String tempResource = null;

  /**
   * Mock class to get group mapping for fake users.
   */
  public static class MockUnixGroupsMapping
      implements GroupMappingServiceProvider {
    private static int i = 0;

    @Override
    public List<String> getGroups(String user) throws IOException {
      LOG.info("Getting groups in MockUnixGroupsMapping");
      String g1 = user + (10 * i + 1);
      String g2 = user + (10 * i + 2);
      List<String> l = new ArrayList<String>(2);
      l.add(g1);
      l.add(g2);
      i++;
      return l;
    }

    @Override
    public void cacheGroupsRefresh() throws IOException {
      LOG.info("Refreshing groups in MockUnixGroupsMapping");
    }

    @Override
    public void cacheGroupsAdd(List<String> groups) throws IOException {
    }
  }

  @Before
  public void setUp() {
    conf = new Configuration(false);
    conf.setClass("hadoop.security.group.mapping",
        TestRouterUserMappings.MockUnixGroupsMapping.class,
        GroupMappingServiceProvider.class);
    conf.setLong("hadoop.security.groups.cache.secs",
        GROUP_REFRESH_TIMEOUT_SEC);
    conf = new RouterConfigBuilder(conf)
        .rpc()
        .admin()
        .build();
    Groups.getUserToGroupsMappingService(conf);
  }

  /**
   * Setup a multi-routers mini dfs cluster with two nameservices
   * and four routers.
   * For dfsadmin clients to use the federated namespace, we need to create a
   * new namespace that points to the routers.
   * For example, a cluster with 2 namespaces ns0, ns1, can add a new one to
   * hdfs-site.xml called {@link #ROUTER_NS}, which points to four of the
   * routers. With this setting dfsadmin client can interact with routers
   * as a regular namespace and reconginze multi-routers.
   * @return fs.defaultFS for multi-routers
   * @throws Exception
   */
  private String setUpMultiRoutersAndReturnDefaultFs() throws Exception {
    //setup a miniroutercluster with 2 nameservices, 4 routers.
    cluster = new MiniRouterDFSCluster(true, 2);
    cluster.addRouterOverrides(conf);
    cluster.startRouters();

    //construct client conf.
    conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "ns0,ns1");
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns0,ns1,"+ ROUTER_NS);
    conf.set(HdfsClientConfigKeys.Failover.
        PROXY_PROVIDER_KEY_PREFIX +"." + ROUTER_NS,
        ConfiguredFailoverProxyProvider.class.getCanonicalName());
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
        HDFS_SCHEMA + ROUTER_NS);
    conf.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX+
        "."+ ROUTER_NS, "r1,r2");
    List<MiniRouterDFSCluster.RouterContext> routers = cluster.getRouters();
    for(int i = 0; i < routers.size(); i++) {
      MiniRouterDFSCluster.RouterContext context = routers.get(i);
      conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY +
          "." + ROUTER_NS+".r" +(i+1), LOOPBACK_ADDRESS +
          ":" +context.getRouter().getRpcServerAddress().getPort());
    }
    return HDFS_SCHEMA + ROUTER_NS;
  }

  @Test
  public void testRefreshSuperUserGroupsConfiguration()
      throws Exception {
    testRefreshSuperUserGroupsConfigurationInternal(
        setUpMultiRoutersAndReturnDefaultFs());
  }

  @Test
  public void testGroupMappingRefresh() throws Exception {
    testGroupMappingRefreshInternal(
        setUpMultiRoutersAndReturnDefaultFs());
  }

  /**
   * Test refreshSuperUserGroupsConfiguration action.
   */
  private void testRefreshSuperUserGroupsConfigurationInternal(
      String defaultFs) throws Exception {
    final String superUser = "super_user";
    final List<String> groupNames1 = new ArrayList<>();
    groupNames1.add("gr1");
    groupNames1.add("gr2");
    final List<String> groupNames2 = new ArrayList<>();
    groupNames2.add("gr3");
    groupNames2.add("gr4");

    //keys in conf
    String userKeyGroups = DefaultImpersonationProvider.getTestProvider().
        getProxySuperuserGroupConfKey(superUser);
    String userKeyHosts = DefaultImpersonationProvider.getTestProvider().
        getProxySuperuserIpConfKey(superUser);

    // superuser can proxy for this group
    conf.set(userKeyGroups, "gr3,gr4,gr5");
    conf.set(userKeyHosts, LOOPBACK_ADDRESS);
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);

    UserGroupInformation ugi1 = mock(UserGroupInformation.class);
    UserGroupInformation ugi2 = mock(UserGroupInformation.class);
    UserGroupInformation suUgi = mock(UserGroupInformation.class);
    when(ugi1.getRealUser()).thenReturn(suUgi);
    when(ugi2.getRealUser()).thenReturn(suUgi);

    // mock  super user
    when(suUgi.getShortUserName()).thenReturn(superUser);
    when(suUgi.getUserName()).thenReturn(superUser+"L");

    when(ugi1.getShortUserName()).thenReturn("user1");
    when(ugi2.getShortUserName()).thenReturn("user2");

    when(ugi1.getUserName()).thenReturn("userL1");
    when(ugi2.getUserName()).thenReturn("userL2");

    // set groups for users
    when(ugi1.getGroups()).thenReturn(groupNames1);
    when(ugi2.getGroups()).thenReturn(groupNames2);

    // check before refresh
    LambdaTestUtils.intercept(AuthorizationException.class,
        () -> ProxyUsers.authorize(ugi1, LOOPBACK_ADDRESS));
    try {
      ProxyUsers.authorize(ugi2, LOOPBACK_ADDRESS);
      LOG.info("auth for {} succeeded", ugi2.getUserName());
      // expected
    } catch (AuthorizationException e) {
      fail("first auth for " + ugi2.getShortUserName() +
          " should've succeeded: " + e.getLocalizedMessage());
    }

    // refresh will look at configuration on the server side
    // add additional resource with the new value
    // so the server side will pick it up
    String rsrc = "testGroupMappingRefresh_rsrc.xml";
    tempResource = addNewConfigResource(rsrc, userKeyGroups, "gr2",
        userKeyHosts, LOOPBACK_ADDRESS);

    conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, defaultFs);
    DFSAdmin admin = new DFSAdmin(conf);
    String[] args = new String[]{"-refreshSuperUserGroupsConfiguration"};
    admin.run(args);

    LambdaTestUtils.intercept(AuthorizationException.class,
        () -> ProxyUsers.authorize(ugi2, LOOPBACK_ADDRESS));
    try {
      ProxyUsers.authorize(ugi1, LOOPBACK_ADDRESS);
      LOG.info("auth for {} succeeded", ugi1.getUserName());
      // expected
    } catch (AuthorizationException e) {
      fail("second auth for " + ugi1.getShortUserName() +
          " should've succeeded: " + e.getLocalizedMessage());
    }

    // get groups
    testGroupsForUserCLI(conf, "user");
    testGroupsForUserProtocol(conf, "user");
  }

  /**
   * Use the command line to get the groups.
   * @param config Configuration containing the default filesystem.
   * @param username Username to check.
   * @throws Exception If it cannot get the groups.
   */
  private void testGroupsForUserCLI(Configuration config, String username)
      throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream oldOut = System.out;
    System.setOut(new PrintStream(out));
    new GetGroups(config).run(new String[]{username});
    assertTrue("Wrong output: " + out,
        out.toString().startsWith(username + " : " + username));
    out.reset();
    System.setOut(oldOut);
  }

  /**
   * Use the GetUserMappingsProtocol protocol to get the groups.
   * @param config Configuration containing the default filesystem.
   * @param username Username to check.
   * @throws IOException If it cannot get the groups.
   */
  private void testGroupsForUserProtocol(Configuration config, String username)
      throws IOException {
    GetUserMappingsProtocol proto = NameNodeProxies.createProxy(
        config, FileSystem.getDefaultUri(config),
        GetUserMappingsProtocol.class).getProxy();
    String[] groups = proto.getGroupsForUser(username);
    assertArrayEquals(new String[] {"user1", "user2"}, groups);
  }

  /**
   * Test refreshUserToGroupsMappings action.
   */
  private void testGroupMappingRefreshInternal(String defaultFs)
      throws Exception {
    Groups groups = Groups.getUserToGroupsMappingService(conf);
    String user = "test_user123";

    LOG.info("First attempt:");
    List<String> g1 = groups.getGroups(user);
    LOG.info("Group 1 :{}", g1);

    LOG.info("Second attempt, should be the same:");
    List<String> g2 = groups.getGroups(user);
    LOG.info("Group 2 :{}", g2);
    for(int i = 0; i < g2.size(); i++) {
      assertEquals("Should be same group ", g1.get(i), g2.get(i));
    }

    // set fs.defaultFS point to router(s).
    conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, defaultFs);
    // Test refresh command
    DFSAdmin admin = new DFSAdmin(conf);
    String[] args =  new String[]{"-refreshUserToGroupsMappings"};
    admin.run(args);

    LOG.info("Third attempt(after refresh command), should be different:");
    List<String> g3 = groups.getGroups(user);
    LOG.info("Group 3:{}", g3);
    for(int i = 0; i < g3.size(); i++) {
      assertNotEquals("Should be different group: "
          + g1.get(i) + " and " + g3.get(i), g1.get(i), g3.get(i));
    }

    // Test timeout
    LOG.info("Fourth attempt(after timeout), should be different:");
    GenericTestUtils.waitFor(() -> {
      List<String> g4;
      try {
        g4 = groups.getGroups(user);
      } catch (IOException e) {
        LOG.debug("Failed to get groups for user:{}", user);
        return false;
      }
      LOG.info("Group 4 : {}", g4);
      // if g4 is the same as g3, wait and retry
      return !g3.equals(g4);
    }, 50, Math.toIntExact(TimeUnit.SECONDS.toMillis(
        GROUP_REFRESH_TIMEOUT_SEC * 30)));
  }

  public static String addNewConfigResource(String rsrcName, String keyGroup,
      String groups, String keyHosts, String hosts)
      throws FileNotFoundException, UnsupportedEncodingException {
    // location for temp resource should be in CLASSPATH
    Configuration conf = new Configuration();
    URL url = conf.getResource("hdfs-site.xml");

    String urlPath = URLDecoder.decode(url.getPath(), "UTF-8");
    Path p = new Path(urlPath);
    Path dir = p.getParent();
    String tmp = dir.toString() + "/" + rsrcName;

    StringBuilder newResource = new StringBuilder()
        .append("<configuration>")
        .append("<property>")
        .append("<name>").append(keyGroup).append("</name>")
        .append("<value>").append(groups).append("</value>")
        .append("</property>")
        .append("<property>")
        .append("<name>").append(keyHosts).append("</name>")
        .append("<value>").append(hosts).append("</value>")
        .append("</property>")
        .append("</configuration>");
    PrintWriter writer = new PrintWriter(new FileOutputStream(tmp));
    writer.println(newResource.toString());
    writer.close();
    Configuration.addDefaultResource(rsrcName);
    return tmp;
  }

  @After
  public void tearDown() {
    if (router != null) {
      router.shutDown();
      router = null;
    }

    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }

    if (tempResource != null) {
      File f = new File(tempResource);
      f.delete();
      tempResource = null;
    }
  }
}
