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
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.TestUGIWithSecurityOn;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class TestSecureNameNode {
  final static private int NUM_OF_DATANODES = 0;

  @Before
  public void testKdcRunning() {
    // Tests are skipped if KDC is not running
    Assume.assumeTrue(TestUGIWithSecurityOn.isKdcRunning());
  }

  @Test
  public void testName() throws IOException, InterruptedException {
    MiniDFSCluster cluster = null;
    try {
      String keyTabDir = System.getProperty("kdc.resource.dir") + "/keytabs";
      String nn1KeytabPath = keyTabDir + "/nn1.keytab";
      String user1KeyTabPath = keyTabDir + "/user1.keytab";
      Configuration conf = new HdfsConfiguration();
      conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
          "kerberos");
      conf.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY,
          "nn1/localhost@EXAMPLE.COM");
      conf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, nn1KeytabPath);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_OF_DATANODES)
          .build();
      final MiniDFSCluster clusterRef = cluster;
      cluster.waitActive();
      FileSystem fsForCurrentUser = cluster.getFileSystem();
      fsForCurrentUser.mkdirs(new Path("/tmp"));
      fsForCurrentUser.setPermission(new Path("/tmp"), new FsPermission(
          (short) 511));

      UserGroupInformation ugi = UserGroupInformation
          .loginUserFromKeytabAndReturnUGI("user1@EXAMPLE.COM", user1KeyTabPath);
      FileSystem fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          return clusterRef.getFileSystem();
        }
      });
      try {
        Path p = new Path("/users");
        fs.mkdirs(p);
        fail("user1 must not be allowed to write in /");
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
