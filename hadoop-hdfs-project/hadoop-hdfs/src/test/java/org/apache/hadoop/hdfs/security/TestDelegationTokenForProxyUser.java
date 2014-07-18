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

package org.apache.hadoop.hdfs.security;



import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Enumeration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.security.TestDoAsEffectiveUser;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.DefaultImpersonationProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.Token;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

public class TestDelegationTokenForProxyUser {
  private static MiniDFSCluster cluster;
  private static Configuration config;
  final private static String GROUP1_NAME = "group1";
  final private static String GROUP2_NAME = "group2";
  final private static String[] GROUP_NAMES = new String[] { GROUP1_NAME,
      GROUP2_NAME };
  final private static String REAL_USER = "RealUser";
  final private static String PROXY_USER = "ProxyUser";
  private static UserGroupInformation ugi;
  private static UserGroupInformation proxyUgi;
  
  private static final Log LOG = LogFactory.getLog(TestDoAsEffectiveUser.class);
  
  private static void configureSuperUserIPAddresses(Configuration conf,
      String superUserShortName) throws IOException {
    ArrayList<String> ipList = new ArrayList<String>();
    Enumeration<NetworkInterface> netInterfaceList = NetworkInterface
        .getNetworkInterfaces();
    while (netInterfaceList.hasMoreElements()) {
      NetworkInterface inf = netInterfaceList.nextElement();
      Enumeration<InetAddress> addrList = inf.getInetAddresses();
      while (addrList.hasMoreElements()) {
        InetAddress addr = addrList.nextElement();
        ipList.add(addr.getHostAddress());
      }
    }
    StringBuilder builder = new StringBuilder();
    for (String ip : ipList) {
      builder.append(ip);
      builder.append(',');
    }
    builder.append("127.0.1.1,");
    builder.append(InetAddress.getLocalHost().getCanonicalHostName());
    LOG.info("Local Ip addresses: " + builder.toString());
    conf.setStrings(DefaultImpersonationProvider.getTestProvider().
            getProxySuperuserIpConfKey(superUserShortName),
        builder.toString());
  }
  
  @BeforeClass
  public static void setUp() throws Exception {
    config = new HdfsConfiguration();
    config.setBoolean(DFSConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    config.setLong(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY, 10000);
    config.setLong(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY, 5000);
    config.setStrings(DefaultImpersonationProvider.getTestProvider().
            getProxySuperuserGroupConfKey(REAL_USER),
        "group1");
    config.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, true);
    configureSuperUserIPAddresses(config, REAL_USER);
    FileSystem.setDefaultUri(config, "hdfs://localhost:" + "0");
    cluster = new MiniDFSCluster.Builder(config).build();
    cluster.waitActive();
    ProxyUsers.refreshSuperUserGroupsConfiguration(config);
    ugi = UserGroupInformation.createRemoteUser(REAL_USER);
    proxyUgi = UserGroupInformation.createProxyUserForTesting(PROXY_USER, ugi,
        GROUP_NAMES);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if(cluster!=null) {
      cluster.shutdown();
    }
  }
 
  @Test(timeout=20000)
  public void testDelegationTokenWithRealUser() throws IOException {
    try {
      Token<?>[] tokens = proxyUgi
          .doAs(new PrivilegedExceptionAction<Token<?>[]>() {
            @Override
            public Token<?>[] run() throws IOException {
              return cluster.getFileSystem().addDelegationTokens("RenewerUser", null);
            }
          });
      DelegationTokenIdentifier identifier = new DelegationTokenIdentifier();
      byte[] tokenId = tokens[0].getIdentifier();
      identifier.readFields(new DataInputStream(new ByteArrayInputStream(
          tokenId)));
      Assert.assertEquals(identifier.getUser().getUserName(), PROXY_USER);
      Assert.assertEquals(identifier.getUser().getRealUser().getUserName(),
          REAL_USER);
    } catch (InterruptedException e) {
      //Do Nothing
    }
  }
  
  @Test(timeout=5000)
  public void testWebHdfsDoAs() throws Exception {
    WebHdfsTestUtil.LOG.info("START: testWebHdfsDoAs()");
    WebHdfsTestUtil.LOG.info("ugi.getShortUserName()=" + ugi.getShortUserName());
    final WebHdfsFileSystem webhdfs = WebHdfsTestUtil.getWebHdfsFileSystemAs(ugi, config, WebHdfsFileSystem.SCHEME);
    
    final Path root = new Path("/");
    cluster.getFileSystem().setPermission(root, new FsPermission((short)0777));

    Whitebox.setInternalState(webhdfs, "ugi", proxyUgi);

    {
      Path responsePath = webhdfs.getHomeDirectory();
      WebHdfsTestUtil.LOG.info("responsePath=" + responsePath);
      Assert.assertEquals(webhdfs.getUri() + "/user/" + PROXY_USER, responsePath.toString());
    }

    final Path f = new Path("/testWebHdfsDoAs/a.txt");
    {
      FSDataOutputStream out = webhdfs.create(f);
      out.write("Hello, webhdfs user!".getBytes());
      out.close();
  
      final FileStatus status = webhdfs.getFileStatus(f);
      WebHdfsTestUtil.LOG.info("status.getOwner()=" + status.getOwner());
      Assert.assertEquals(PROXY_USER, status.getOwner());
    }

    {
      final FSDataOutputStream out = webhdfs.append(f);
      out.write("\nHello again!".getBytes());
      out.close();
  
      final FileStatus status = webhdfs.getFileStatus(f);
      WebHdfsTestUtil.LOG.info("status.getOwner()=" + status.getOwner());
      WebHdfsTestUtil.LOG.info("status.getLen()  =" + status.getLen());
      Assert.assertEquals(PROXY_USER, status.getOwner());
    }
  }
}
