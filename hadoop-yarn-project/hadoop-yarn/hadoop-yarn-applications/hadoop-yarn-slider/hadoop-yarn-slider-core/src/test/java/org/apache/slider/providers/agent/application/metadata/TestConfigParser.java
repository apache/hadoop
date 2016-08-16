/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.providers.agent.application.metadata;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public class TestConfigParser {
  protected static final Logger log =
      LoggerFactory.getLogger(TestConfigParser.class);
  private static final String config_1_str = "<configuration>\n"
                                             + "  <property>\n"
                                             + "    <name>security.client.protocol.acl</name>\n"
                                             + "    <value>*</value>\n"
                                             + "    <description>ACL for HRegionInterface protocol implementations (ie. \n"
                                             + "    clients talking to HRegionServers)\n"
                                             + "    The ACL is a comma-separated list of user and group names. The user and \n"
                                             + "    group list is separated by a blank. For e.g. \"alice,bob users,wheel\". \n"
                                             + "    A special value of \"*\" means all users are allowed.</description>\n"
                                             + "  </property>\n"
                                             + "\n"
                                             + "  <property>\n"
                                             + "    <name>security.admin.protocol.acl</name>\n"
                                             + "    <value>*</value>\n"
                                             + "    <description>ACL for HMasterInterface protocol implementation (ie. \n"
                                             + "    clients talking to HMaster for admin operations).\n"
                                             + "    The ACL is a comma-separated list of user and group names. The user and \n"
                                             + "    group list is separated by a blank. For e.g. \"alice,bob users,wheel\". \n"
                                             + "    A special value of \"*\" means all users are allowed.</description>\n"
                                             + "  </property>\n"
                                             + "\n"
                                             + "  <property>\n"
                                             + "    <name>security.masterregion.protocol.acl</name>\n"
                                             + "    <value>*</value>\n"
                                             + "    <description>ACL for HMasterRegionInterface protocol implementations\n"
                                             + "    (for HRegionServers communicating with HMaster)\n"
                                             + "    The ACL is a comma-separated list of user and group names. The user and \n"
                                             + "    group list is separated by a blank. For e.g. \"alice,bob users,wheel\". \n"
                                             + "    A special value of \"*\" means all users are allowed.</description>\n"
                                             + "  </property>\n"
                                             + "  <property>\n"
                                             + "    <name>emptyVal</name>\n"
                                             + "    <value></value>\n"
                                             + "    <description>non-empty-desc</description>\n"
                                             + "  </property>\n"
                                             + "  <property>\n"
                                             + "    <name>emptyDesc</name>\n"
                                             + "    <value></value>\n"
                                             + "    <description></description>\n"
                                             + "  </property>\n"
                                             + "  <property>\n"
                                             + "    <name>noDesc</name>\n"
                                             + "    <value></value>\n"
                                             + "  </property>\n"
                                             + "</configuration>";

  @Test
  public void testParse() throws IOException {

    InputStream config_1 = new ByteArrayInputStream(config_1_str.getBytes());
    DefaultConfig config = new DefaultConfigParser().parse(config_1);
    Assert.assertNotNull(config);
    Assert.assertNotNull(config.getPropertyInfos());
    Assert.assertEquals(6, config.getPropertyInfos().size());
    for (PropertyInfo pInfo : config.getPropertyInfos()) {
      if (pInfo.getName().equals("security.client.protocol.acl")) {
        Assert.assertEquals("*", pInfo.getValue());
        Assert.assertTrue(pInfo.getDescription().startsWith("ACL for HRegionInterface "));
      }
      if (pInfo.getName().equals("emptyVal")) {
        Assert.assertEquals("", pInfo.getValue());
        Assert.assertEquals("non-empty-desc", pInfo.getDescription());
      }
      if (pInfo.getName().equals("emptyDesc")) {
        Assert.assertEquals("", pInfo.getValue());
        Assert.assertEquals("", pInfo.getDescription());
      }
      if (pInfo.getName().equals("noDesc")) {
        Assert.assertEquals("", pInfo.getValue());
        Assert.assertNull(pInfo.getDescription());
      }
    }
  }
}
