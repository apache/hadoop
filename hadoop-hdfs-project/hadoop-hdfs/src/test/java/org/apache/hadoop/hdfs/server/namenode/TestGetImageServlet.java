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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import javax.servlet.ServletContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

public class TestGetImageServlet {
  
  @Test
  public void testIsValidRequestor() throws IOException {
    Configuration conf = new HdfsConfiguration();
    KerberosName.setRules("RULE:[1:$1]\nRULE:[2:$1]");
    
    // Set up generic HA configs.
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1");
    conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX,
        "ns1"), "nn1,nn2");
    
    // Set up NN1 HA configs.
    conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY,
        "ns1", "nn1"), "host1:1234");
    conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY,
        "ns1", "nn1"), "hdfs/_HOST@TEST-REALM.COM");
    
    // Set up NN2 HA configs.
    conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY,
        "ns1", "nn2"), "host2:1234");
    conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY,
        "ns1", "nn2"), "hdfs/_HOST@TEST-REALM.COM");
    
    // Initialize this conf object as though we're running on NN1.
    NameNode.initializeGenericKeys(conf, "ns1", "nn1");
    
    AccessControlList acls = Mockito.mock(AccessControlList.class);
    Mockito.when(acls.isUserAllowed(Mockito.<UserGroupInformation>any())).thenReturn(false);
    ServletContext context = Mockito.mock(ServletContext.class);
    Mockito.when(context.getAttribute(HttpServer2.ADMINS_ACL)).thenReturn(acls);
    
    // Make sure that NN2 is considered a valid fsimage/edits requestor.
    assertTrue(ImageServlet.isValidRequestor(context,
        "hdfs/host2@TEST-REALM.COM", conf));
    
    // Mark atm as an admin.
    Mockito.when(acls.isUserAllowed(Mockito.argThat(new ArgumentMatcher<UserGroupInformation>() {
      @Override
      public boolean matches(Object argument) {
        return ((UserGroupInformation) argument).getShortUserName().equals("atm");
      }
    }))).thenReturn(true);
    
    // Make sure that NN2 is still considered a valid requestor.
    assertTrue(ImageServlet.isValidRequestor(context,
        "hdfs/host2@TEST-REALM.COM", conf));
    
    // Make sure an admin is considered a valid requestor.
    assertTrue(ImageServlet.isValidRequestor(context,
        "atm@TEST-REALM.COM", conf));
    
    // Make sure other users are *not* considered valid requestors.
    assertFalse(ImageServlet.isValidRequestor(context,
        "todd@TEST-REALM.COM", conf));
  }
}
