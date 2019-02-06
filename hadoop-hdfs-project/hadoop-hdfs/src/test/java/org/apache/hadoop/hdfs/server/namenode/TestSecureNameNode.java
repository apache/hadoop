/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferTestCase;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

public class TestSecureNameNode extends SaslDataTransferTestCase {
  final static private int NUM_OF_DATANODES = 0;

  @Rule
  public ExpectedException exception = ExpectedException.none();


  @Test
  public void testName() throws Exception {
    MiniDFSCluster cluster = null;
    HdfsConfiguration conf = createSecureConfig(
        "authentication,privacy");
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_OF_DATANODES)
          .build();
      final MiniDFSCluster clusterRef = cluster;
      cluster.waitActive();
      FileSystem fsForSuperUser = UserGroupInformation
          .loginUserFromKeytabAndReturnUGI(getHdfsPrincipal(), getHdfsKeytab()).doAs(new PrivilegedExceptionAction<FileSystem>() {
            @Override
            public FileSystem run() throws Exception {
              return clusterRef.getFileSystem();
            }
          });
      fsForSuperUser.mkdirs(new Path("/tmp"));
      fsForSuperUser.setPermission(new Path("/tmp"), new FsPermission(
          (short) 511));

      UserGroupInformation ugi = UserGroupInformation
          .loginUserFromKeytabAndReturnUGI(getUserPrincipal(), getUserKeyTab());
      FileSystem fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return clusterRef.getFileSystem();
        }
      });
      Path p = new Path("/mydir");
      exception.expect(IOException.class);
      fs.mkdirs(p);

      Path tmp = new Path("/tmp/alpha");
      fs.mkdirs(tmp);
      assertNotNull(fs.listStatus(tmp));
      assertEquals(AuthenticationMethod.KERBEROS,
          ugi.getAuthenticationMethod());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Verify the following scenario.
   * 1. Kerberos is enabled.
   * 2. HDFS block tokens are not enabled.
   * 3. Start the NN.
   * 4. NN should throw an IOException and abort
   * @throws Exception
   */
  @Test
  public void testKerberosHdfsBlockTokenInconsistencyNNStartup() throws Exception {
    MiniDFSCluster dfsCluster = null;
    HdfsConfiguration conf = createSecureConfig(
        "authentication,privacy");
    try {
      conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, false);
      exception.expect(IOException.class);
      exception.expectMessage("Security is enabled but block access tokens");
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      dfsCluster.waitActive();
    } finally {
      if (dfsCluster != null) {
        dfsCluster.shutdown();
      }
    }
    return;
  }

  /**
   * Test NameNodeStatusMXBean with security enabled and disabled.
   *
   * @throws Exception
   */
  @Test
  public void testNameNodeStatusMXBeanSecurityEnabled() throws Exception {
    Configuration simpleConf = new Configuration();
    Configuration secureConf = createSecureConfig("authentication");

    // disabling security
    UserGroupInformation.setConfiguration(simpleConf);

    // get attribute "SecurityEnabled" with simple configuration
    try (MiniDFSCluster cluster =
                 new MiniDFSCluster.Builder(simpleConf).build()) {
      cluster.waitActive();
      NameNode namenode = cluster.getNameNode();

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName = new ObjectName(
              "Hadoop:service=NameNode,name=NameNodeStatus");

      boolean securityEnabled = (boolean) mbs.getAttribute(mxbeanName,
              "SecurityEnabled");
      Assert.assertFalse(securityEnabled);
      Assert.assertEquals(namenode.isSecurityEnabled(), securityEnabled);
    }

    // get attribute "SecurityEnabled" with secure configuration
    try (MiniDFSCluster cluster =
                 new MiniDFSCluster.Builder(secureConf).build()) {
      cluster.waitActive();
      NameNode namenode = cluster.getNameNode();

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName = new ObjectName(
              "Hadoop:service=NameNode,name=NameNodeStatus");

      boolean securityEnabled = (boolean) mbs.getAttribute(mxbeanName,
              "SecurityEnabled");
      Assert.assertTrue(securityEnabled);
      Assert.assertEquals(namenode.isSecurityEnabled(), securityEnabled);
    }
  }

}
