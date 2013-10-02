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
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
import org.znerd.xmlenc.XMLOutputter;

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
   * Tests for non-null, non-empty NameNode label.
   */
  @Test
  public void testGetNameNodeLabel() {
    String nameNodeLabel = NamenodeJspHelper.getNameNodeLabel(
      cluster.getNameNode());
    Assert.assertNotNull(nameNodeLabel);
    Assert.assertFalse(nameNodeLabel.isEmpty());
  }

  /**
   * Tests for non-null, non-empty NameNode label when called before
   * initialization of the NameNode RPC server.
   */
  @Test
  public void testGetNameNodeLabelNullRpcServer() {
    NameNode nn = mock(NameNode.class);
    when(nn.getRpcServer()).thenReturn(null);
    String nameNodeLabel = NamenodeJspHelper.getNameNodeLabel(
      cluster.getNameNode());
    Assert.assertNotNull(nameNodeLabel);
    Assert.assertFalse(nameNodeLabel.isEmpty());
  }

  /**
   * Tests that redirectToRandomDataNode does not throw NullPointerException if
   * it finds a null FSNamesystem.
   */
  @Test(expected=IOException.class)
  public void testRedirectToRandomDataNodeNullNamesystem() throws Exception {
    NameNode nn = mock(NameNode.class);
    when(nn.getNamesystem()).thenReturn(null);
    ServletContext context = mock(ServletContext.class);
    when(context.getAttribute("name.node")).thenReturn(nn);
    NamenodeJspHelper.redirectToRandomDataNode(context,
      mock(HttpServletRequest.class), mock(HttpServletResponse.class));
  }

  /**
   * Tests that XMLBlockInfo does not throw NullPointerException if it finds a
   * null FSNamesystem.
   */
  @Test
  public void testXMLBlockInfoNullNamesystem() throws IOException {
    XMLOutputter doc = new XMLOutputter(mock(JspWriter.class), "UTF-8");
    new NamenodeJspHelper.XMLBlockInfo(null, 1L).toXML(doc);
  }

  /**
   * Tests that XMLCorruptBlockInfo does not throw NullPointerException if it
   * finds a null FSNamesystem.
   */
  @Test
  public void testXMLCorruptBlockInfoNullNamesystem() throws IOException {
    XMLOutputter doc = new XMLOutputter(mock(JspWriter.class), "UTF-8");
    new NamenodeJspHelper.XMLCorruptBlockInfo(null, mock(Configuration.class),
      10, 1L).toXML(doc);
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
