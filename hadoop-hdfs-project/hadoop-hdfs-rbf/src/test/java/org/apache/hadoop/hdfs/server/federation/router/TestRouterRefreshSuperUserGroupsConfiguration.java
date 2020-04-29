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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.hdfs.tools.federation.RouterAdmin;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;

import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test refreshSuperUserGroupsConfiguration on Routers.
 * Notice that this test is intended to test
 * {@link RouterAdminServer#refreshSuperUserGroupsConfiguration}
 * which invoked by {@link RouterAdmin}
 */
public class TestRouterRefreshSuperUserGroupsConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestRouterRefreshSuperUserGroupsConfiguration.class);

  private MiniRouterDFSCluster cluster;
  private static final String ROUTER_NS = "rbfns";
  private static final String HDFS_SCHEMA = "hdfs://";
  private static final String LOOPBACK_ADDRESS = "127.0.0.1";

  private String tempResource = null;
  @Before
  public void setUpCluster() throws Exception {
    Configuration conf = new RouterConfigBuilder()
        .rpc()
        .admin()
        .build();
    cluster = new MiniRouterDFSCluster(false, 1);
    cluster.addRouterOverrides(conf);
    cluster.startRouters();
  }

  @After
  public void tearDown() {
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

  private Configuration initializeClientConfig() throws Exception {

    Configuration conf = new Configuration(false);
    Router router = cluster.getRouters().get(0).getRouter();

    conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns0,ns1,"+ ROUTER_NS);
    conf.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "."+ ROUTER_NS, "r1");
    conf.set(RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY,
        LOOPBACK_ADDRESS + ":" + router.getAdminServerAddress().getPort());
    conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY
            + "." + ROUTER_NS + ".r1",
        LOOPBACK_ADDRESS + ":" + router.getRpcServerAddress().getPort());
    conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX
            + "." + ROUTER_NS,
        ConfiguredFailoverProxyProvider.class.getCanonicalName());
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
        HDFS_SCHEMA + ROUTER_NS);
    conf.setBoolean("dfs.client.failover.random.order", false);
    conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, HDFS_SCHEMA + ROUTER_NS);

    return conf;
  }

  @Test
  public void testRefreshSuperUserGroupsConfiguration()
      throws Exception {
    Configuration conf = initializeClientConfig();
    testRefreshSuperUserGroupsConfigurationInternal(conf);
  }

  private void testRefreshSuperUserGroupsConfigurationInternal(
      Configuration conf) throws Exception {

    UserGroupInformation ugi = mock(UserGroupInformation.class);
    UserGroupInformation impersonator = mock(UserGroupInformation.class);

    // Setting for impersonator
    when(impersonator.getShortUserName()).thenReturn("impersonator");
    when(impersonator.getUserName()).thenReturn("impersonator");

    // Setting for victim
    when(ugi.getRealUser()).thenReturn(impersonator);
    when(ugi.getUserName()).thenReturn("victim");
    when(ugi.getGroups()).thenReturn(Arrays.asList("groupVictim"));

    // Exception should be thrown before applying config
    LambdaTestUtils.intercept(
        AuthorizationException.class,
        "User: impersonator is not allowed to impersonate victim",
        () -> ProxyUsers.authorize(ugi, LOOPBACK_ADDRESS));

    // refresh will look at configuration on the server side
    // add additional resource with the new value
    // so the server side will pick it up
    String tfile = "testRouterRefreshSuperUserGroupsConfiguration_rsrc.xml";
    ArrayList<String> keys = new ArrayList<>(
        Arrays.asList(
            "hadoop.proxyuser.impersonator.groups",
            "hadoop.proxyuser.impersonator.hosts"));
    ArrayList<String> values = new ArrayList<>(
        Arrays.asList(
            "groupVictim",
            LOOPBACK_ADDRESS));
    tempResource = addFileBasedConfigResource(tfile, keys, values);
    Configuration.addDefaultResource(tfile);

    // Mimic CLI Access
    RouterAdmin routerAdmin = new RouterAdmin(conf);
    int clientRes =
        routerAdmin.run(new String[]{"-refreshSuperUserGroupsConfiguration"});

    assertEquals("CLI command was not successful", 0, clientRes);
    ProxyUsers.authorize(ugi, LOOPBACK_ADDRESS);
  }

  private static String addFileBasedConfigResource(String configFileName,
      ArrayList<String> keyArray,
      ArrayList<String> valueArray)
      throws IOException {

    if(keyArray.size() != valueArray.size()) {
      throw new IOException("keyArray and valueArray should be equal in size");
    }

    URL url = new Configuration().getResource("hdfs-site.xml");
    Path dir = new Path(URLDecoder.decode(url.getPath(), "UTF-8")).getParent();
    String tmp = dir.toString() + "/" + configFileName;

    StringBuilder configItems = new StringBuilder();
    configItems.append("<configuration>");
    for (int i = 0; i < keyArray.size(); i++){
      configItems
          .append("<property>")
          .append("<name>").append(keyArray.get(i)).append("</name>")
          .append("<value>").append(valueArray.get(i)).append("</value>")
          .append("</property>");
    }
    configItems.append("</configuration>");

    PrintWriter writer = new PrintWriter(new FileOutputStream(tmp));
    writer.println(configItems.toString());
    writer.close();
    return tmp;
  }
}
