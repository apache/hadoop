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
package org.apache.hadoop.hdfs.qjournal.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestGetJournalEditServlet {

  private final static Configuration CONF = new HdfsConfiguration();

  private final static GetJournalEditServlet SERVLET = new GetJournalEditServlet();

  @BeforeClass
  public static void setUp() throws ServletException {
    // Configure Hadoop
    CONF.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "hdfs://localhost:4321/");
    CONF.set(DFSConfigKeys.HADOOP_SECURITY_AUTH_TO_LOCAL,
        "RULE:[2:$1/$2@$0]([nsdj]n/.*@REALM\\.TLD)s/.*/hdfs/\nDEFAULT");
    CONF.set(DFSConfigKeys.DFS_NAMESERVICES, "ns");
    CONF.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, "nn/_HOST@REALM.TLD");

    // Configure Kerberos UGI
    UserGroupInformation.setConfiguration(CONF);
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(
        "jn/somehost@REALM.TLD"));

    // Initialize the servlet
    ServletConfig config = mock(ServletConfig.class);
    SERVLET.init(config);
  }

  /**
   * Unauthenticated user should be rejected.
   *
   * @throws IOException for unexpected validation failures
   */
  @Test
  public void testWithoutUser() throws IOException {
    // Test: Make a request without specifying a user
    HttpServletRequest request = mock(HttpServletRequest.class);
    boolean isValid = SERVLET.isValidRequestor(request, CONF);

    // Verify: The request is invalid
    assertThat(isValid).isFalse();
  }

  /**
   * Namenode requests should be authorized, since it will match the configured namenode.
   *
   * @throws IOException for unexpected validation failures
   */
  @Test
  public void testRequestNameNode() throws IOException, ServletException {
    // Test: Make a request from a namenode
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getParameter(UserParam.NAME)).thenReturn("nn/localhost@REALM.TLD");
    boolean isValid = SERVLET.isValidRequestor(request, CONF);

    assertThat(isValid).isTrue();
  }

  /**
   * There is a fallback using the short name, which is used by journalnodes.
   *
   * @throws IOException for unexpected validation failures
   */
  @Test
  public void testRequestShortName() throws IOException {
    // Test: Make a request from a namenode
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getParameter(UserParam.NAME)).thenReturn("jn/localhost@REALM.TLD");
    boolean isValid = SERVLET.isValidRequestor(request, CONF);

    assertThat(isValid).isTrue();
  }

}