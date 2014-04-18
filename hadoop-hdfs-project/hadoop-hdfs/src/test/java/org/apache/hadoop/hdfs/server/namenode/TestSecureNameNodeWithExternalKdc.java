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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import static org.apache.hadoop.security.SecurityUtilTestHelper.isExternalKdcRunning;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * This test brings up a MiniDFSCluster with 1 NameNode and 0
 * DataNodes with kerberos authentication enabled using user-specified
 * KDC, principals, and keytabs.
 *
 * To run, users must specify the following system properties:
 *   externalKdc=true
 *   java.security.krb5.conf
 *   dfs.namenode.kerberos.principal
 *   dfs.namenode.kerberos.internal.spnego.principal
 *   dfs.namenode.keytab.file
 *   user.principal (do not specify superuser!)
 *   user.keytab
 */
public class TestSecureNameNodeWithExternalKdc {
  final static private int NUM_OF_DATANODES = 0;

  @Before
  public void testExternalKdcRunning() {
    // Tests are skipped if external KDC is not running.
    Assume.assumeTrue(isExternalKdcRunning());
  }

  @Test
  public void testSecureNameNode() throws IOException, InterruptedException {
    MiniDFSCluster cluster = null;
    try {
      String nnPrincipal =
        System.getProperty("dfs.namenode.kerberos.principal");
      String nnSpnegoPrincipal =
        System.getProperty("dfs.namenode.kerberos.internal.spnego.principal");
      String nnKeyTab = System.getProperty("dfs.namenode.keytab.file");
      assertNotNull("NameNode principal was not specified", nnPrincipal);
      assertNotNull("NameNode SPNEGO principal was not specified",
        nnSpnegoPrincipal);
      assertNotNull("NameNode keytab was not specified", nnKeyTab);

      Configuration conf = new HdfsConfiguration();
      conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
          "kerberos");
      conf.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, nnPrincipal);
      conf.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY,
          nnSpnegoPrincipal);
      conf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, nnKeyTab);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_OF_DATANODES)
          .build();
      final MiniDFSCluster clusterRef = cluster;
      cluster.waitActive();
      FileSystem fsForCurrentUser = cluster.getFileSystem();
      fsForCurrentUser.mkdirs(new Path("/tmp"));
      fsForCurrentUser.setPermission(new Path("/tmp"), new FsPermission(
          (short) 511));

      // The user specified should not be a superuser
      String userPrincipal = System.getProperty("user.principal");
      String userKeyTab = System.getProperty("user.keytab");
      assertNotNull("User principal was not specified", userPrincipal);
      assertNotNull("User keytab was not specified", userKeyTab);

      UserGroupInformation ugi = UserGroupInformation
          .loginUserFromKeytabAndReturnUGI(userPrincipal, userKeyTab);
      FileSystem fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return clusterRef.getFileSystem();
        }
      });
      try {
        Path p = new Path("/users");
        fs.mkdirs(p);
        fail("User must not be allowed to write in /");
      } catch (IOException expected) {
      }

      Path p = new Path("/tmp/alpha");
      fs.mkdirs(p);
      assertNotNull(fs.listStatus(p));
      assertEquals(AuthenticationMethod.KERBEROS,
          ugi.getAuthenticationMethod());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
