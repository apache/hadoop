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

package org.apache.hadoop.hdfs.server.datanode;

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
 * This test starts a 1 NameNode 1 DataNode MiniDFSCluster with
 * kerberos authentication enabled using user-specified KDC,
 * principals, and keytabs.
 *
 * A secure DataNode has to be started by root, so this test needs to
 * be run by root.
 *
 * To run, users must specify the following system properties:
 *   externalKdc=true
 *   java.security.krb5.conf
 *   dfs.namenode.kerberos.principal
 *   dfs.namenode.kerberos.internal.spnego.principal
 *   dfs.namenode.keytab.file
 *   dfs.datanode.kerberos.principal
 *   dfs.datanode.keytab.file
 */
public class TestStartSecureDataNode {
  final static private int NUM_OF_DATANODES = 1;

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

      String dnPrincipal = System.getProperty("dfs.datanode.kerberos.principal");
      String dnKeyTab = System.getProperty("dfs.datanode.keytab.file");
      assertNotNull("DataNode principal was not specified", dnPrincipal);
      assertNotNull("DataNode keytab was not specified", dnKeyTab);

      Configuration conf = new HdfsConfiguration();
      conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
      conf.set(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY, nnPrincipal);
      conf.set(DFSConfigKeys.DFS_NAMENODE_INTERNAL_SPNEGO_USER_NAME_KEY,
        nnSpnegoPrincipal);
      conf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, nnKeyTab);
      conf.set(DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY, dnPrincipal);
      conf.set(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY, dnKeyTab);
      // Secure DataNode requires using ports lower than 1024.
      conf.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, "127.0.0.1:1004");
      conf.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, "127.0.0.1:1006");
      conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, "700");

      cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_OF_DATANODES)
        .checkDataNodeAddrConfig(true)
        .build();
      cluster.waitActive();
      assertTrue(cluster.isDataNodeUp());

    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
