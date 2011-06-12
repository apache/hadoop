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
package org.apache.hadoop.hdfs;

import java.io.DataOutputStream;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestLease {
  static boolean hasLease(MiniDFSCluster cluster, Path src) {
    return cluster.getNamesystem().leaseManager.getLeaseByPath(src.toString()) != null;
  }
  
  final Path dir = new Path("/test/lease/");

  @Test
  public void testLease() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    try {
      FileSystem fs = cluster.getFileSystem();
      Assert.assertTrue(fs.mkdirs(dir));
      
      Path a = new Path(dir, "a");
      Path b = new Path(dir, "b");

      DataOutputStream a_out = fs.create(a);
      a_out.writeBytes("something");

      Assert.assertTrue(hasLease(cluster, a));
      Assert.assertTrue(!hasLease(cluster, b));
      
      DataOutputStream b_out = fs.create(b);
      b_out.writeBytes("something");

      Assert.assertTrue(hasLease(cluster, a));
      Assert.assertTrue(hasLease(cluster, b));

      a_out.close();
      b_out.close();

      Assert.assertTrue(!hasLease(cluster, a));
      Assert.assertTrue(!hasLease(cluster, b));
      
      fs.delete(dir, true);
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  @Test
  public void testFactory() throws Exception {
    final String[] groups = new String[]{"supergroup"};
    final UserGroupInformation[] ugi = new UserGroupInformation[3];
    for(int i = 0; i < ugi.length; i++) {
      ugi[i] = UserGroupInformation.createUserForTesting("user" + i, groups);
    }

    final Configuration conf = new Configuration();
    final DFSClient c1 = createDFSClientAs(ugi[0], conf);
    final DFSClient c2 = createDFSClientAs(ugi[0], conf);
    Assert.assertEquals(c1.leaserenewer, c2.leaserenewer);
    final DFSClient c3 = createDFSClientAs(ugi[1], conf);
    Assert.assertTrue(c1.leaserenewer != c3.leaserenewer);
    final DFSClient c4 = createDFSClientAs(ugi[1], conf);
    Assert.assertEquals(c3.leaserenewer, c4.leaserenewer);
    final DFSClient c5 = createDFSClientAs(ugi[2], conf);
    Assert.assertTrue(c1.leaserenewer != c5.leaserenewer);
    Assert.assertTrue(c3.leaserenewer != c5.leaserenewer);
  }
  
  static final ClientProtocol mcp = Mockito.mock(ClientProtocol.class);
  static public DFSClient createDFSClientAs(UserGroupInformation ugi, 
      final Configuration conf) throws Exception {
    return ugi.doAs(new PrivilegedExceptionAction<DFSClient>() {
      @Override
      public DFSClient run() throws Exception {
        return new DFSClient(null, mcp, conf, null);
      }
    });
  }
}
