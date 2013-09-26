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


import static org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase.LOADING_EDITS;
import static org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase.LOADING_FSIMAGE;
import static org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase.SAFEMODE;
import static org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase.SAVING_CHECKPOINT;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.jsp.JspWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class TestNameNodeJspHelper {

  private MiniDFSCluster cluster = null;
  Configuration conf = null;

  @Before
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
    cluster  = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null)
      cluster.shutdown();
  }

  @Test
  public void testDelegationToken() throws IOException, InterruptedException {
    NamenodeProtocols nn = cluster.getNameNodeRpc();
    HttpServletRequest request = mock(HttpServletRequest.class);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("auser");
    String tokenString = NamenodeJspHelper.getDelegationToken(nn, request,
        conf, ugi);
    //tokenString returned must be null because security is disabled
    Assert.assertEquals(null, tokenString);
  }
  
  @Test
  public void  tesSecurityModeText() {
    conf.set(DFSConfigKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    String securityOnOff = NamenodeJspHelper.getSecurityModeText();
    Assert.assertTrue("security mode doesn't match. Should be ON", 
        securityOnOff.contains("ON"));
    //Security is enabled
    conf.set(DFSConfigKeys.HADOOP_SECURITY_AUTHENTICATION, "simple");
    UserGroupInformation.setConfiguration(conf);
    
    securityOnOff = NamenodeJspHelper.getSecurityModeText();
    Assert.assertTrue("security mode doesn't match. Should be OFF", 
        securityOnOff.contains("OFF"));
  }

  @Test
  public void testGenerateStartupProgress() throws Exception {
    cluster.waitClusterUp();
    NamenodeJspHelper.HealthJsp jsp = new NamenodeJspHelper.HealthJsp();
    StartupProgress prog = NameNode.getStartupProgress();
    JspWriter out = mock(JspWriter.class);
    jsp.generateStartupProgress(out, prog);
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(out, atLeastOnce()).println(captor.capture());
    List<String> contents = captor.getAllValues();

    // Verify 100% overall completion and all phases mentioned in output.
    Assert.assertTrue(containsMatch(contents, "Elapsed Time\\:"));
    Assert.assertTrue(containsMatch(contents, "Percent Complete\\:.*?100\\.00%"));
    Assert.assertTrue(containsMatch(contents, LOADING_FSIMAGE.getDescription()));
    Assert.assertTrue(containsMatch(contents, LOADING_EDITS.getDescription()));
    Assert.assertTrue(containsMatch(contents,
      SAVING_CHECKPOINT.getDescription()));
    Assert.assertTrue(containsMatch(contents, SAFEMODE.getDescription()));
  }

  @Test
  public void testGetRollingUpgradeText() {
    Assert.assertEquals("", NamenodeJspHelper.getRollingUpgradeText(null));
  }

  /**
   * Checks if the list contains any string that partially matches the regex.
   * 
   * @param list List<String> containing strings to check
   * @param regex String regex to check
   * @return boolean true if some string in list partially matches regex
   */
  private static boolean containsMatch(List<String> list, String regex) {
    Pattern pattern = Pattern.compile(regex);
    for (String str: list) {
      if (pattern.matcher(str).find()) {
        return true;
      }
    }
    return false;
  }
}
