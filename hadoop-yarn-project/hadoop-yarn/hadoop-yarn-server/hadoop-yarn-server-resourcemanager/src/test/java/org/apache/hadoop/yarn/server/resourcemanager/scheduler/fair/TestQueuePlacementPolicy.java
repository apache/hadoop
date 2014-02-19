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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.google.common.collect.Sets;

public class TestQueuePlacementPolicy {
  private final static Configuration conf = new Configuration();
  private final static Set<String> configuredQueues = Sets.newHashSet("root.someuser");
  
  @BeforeClass
  public static void setup() {
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);
  }
  
  @Test
  public void testSpecifiedUserPolicy() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' />");
    sb.append("  <rule name='user' />");
    sb.append("</queuePlacementPolicy>");
    QueuePlacementPolicy policy = parse(sb.toString());
    assertEquals("root.specifiedq",policy.assignAppToQueue("specifiedq", "someuser"));
    assertEquals("root.someuser", policy.assignAppToQueue("default", "someuser"));
    assertEquals("root.otheruser", policy.assignAppToQueue("default", "otheruser"));
  }
  
  @Test
  public void testNoCreate() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' />");
    sb.append("  <rule name='user' create=\"false\" />");
    sb.append("  <rule name='default' />");
    sb.append("</queuePlacementPolicy>");
    QueuePlacementPolicy policy = parse(sb.toString());
    assertEquals("root.specifiedq", policy.assignAppToQueue("specifiedq", "someuser"));
    assertEquals("root.someuser", policy.assignAppToQueue("default", "someuser"));
    assertEquals("root.specifiedq", policy.assignAppToQueue("specifiedq", "otheruser"));
    assertEquals("root.default", policy.assignAppToQueue("default", "otheruser"));
  }
  
  @Test
  public void testSpecifiedThenReject() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' />");
    sb.append("  <rule name='reject' />");
    sb.append("</queuePlacementPolicy>");
    QueuePlacementPolicy policy = parse(sb.toString());
    assertEquals("root.specifiedq", policy.assignAppToQueue("specifiedq", "someuser"));
    assertEquals(null, policy.assignAppToQueue("default", "someuser"));
  }
  
  @Test (expected = AllocationConfigurationException.class)
  public void testOmittedTerminalRule() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' />");
    sb.append("  <rule name='user' create=\"false\" />");
    sb.append("</queuePlacementPolicy>");
    parse(sb.toString());
  }
  
  @Test (expected = AllocationConfigurationException.class)
  public void testTerminalRuleInMiddle() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='specified' />");
    sb.append("  <rule name='default' />");
    sb.append("  <rule name='user' />");
    sb.append("</queuePlacementPolicy>");
    parse(sb.toString());
  }
  
  @Test
  public void testTerminals() throws Exception {
    // Should make it through without an exception
    StringBuffer sb = new StringBuffer();
    sb.append("<queuePlacementPolicy>");
    sb.append("  <rule name='secondaryGroupExistingQueue' create='true'/>");
    sb.append("  <rule name='default' create='false'/>");
    sb.append("</queuePlacementPolicy>");
    parse(sb.toString());
  }
  
  private QueuePlacementPolicy parse(String str) throws Exception {
    // Read and parse the allocations file.
    DocumentBuilderFactory docBuilderFactory =
      DocumentBuilderFactory.newInstance();
    docBuilderFactory.setIgnoringComments(true);
    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    Document doc = builder.parse(IOUtils.toInputStream(str));
    Element root = doc.getDocumentElement();
    return QueuePlacementPolicy.fromXml(root, configuredQueues, conf);
  }
}
