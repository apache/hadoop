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
package org.apache.hadoop.mapred;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.ByteArrayInputStream;


public class TestPoolPlacementPolicy {
  private final static Configuration conf = new Configuration();
  private final static Set<String> configuredPools = new HashSet<String>();

  @BeforeClass
  public static void setup() {
    conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        SimpleGroupsMapping.class, GroupMappingServiceProvider.class);
    configuredPools.add("someuser");
  }
  
  @Test
  public void testSpecifiedUserPolicy() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<poolPlacementPolicy>");
    sb.append("  <specified />");
    sb.append("  <user />");
    sb.append("</poolPlacementPolicy>");
    PoolPlacementPolicy policy = parse(sb.toString());
    assertEquals("specifiedq",policy.assignJobToPool("specifiedq", "someuser"));
    assertEquals("someuser", policy.assignJobToPool("default", "someuser"));
    assertEquals("otheruser", policy.assignJobToPool("default", "otheruser"));
  }
  
  @Test
  public void testNoCreate() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<poolPlacementPolicy>");
    sb.append("  <specified />");
    sb.append("  <user create=\"false\" />");
    sb.append("  <default />");
    sb.append("</poolPlacementPolicy>");
    PoolPlacementPolicy policy = parse(sb.toString());
    assertEquals("specifiedq", policy.assignJobToPool("specifiedq", "someuser"));
    assertEquals("someuser", policy.assignJobToPool("default", "someuser"));
    assertEquals("specifiedq", policy.assignJobToPool("specifiedq", "otheruser"));
    assertEquals("default", policy.assignJobToPool("default", "otheruser"));
  }
  
  @Test
  public void testSpecifiedThenReject() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<poolPlacementPolicy>");
    sb.append("  <specified />");
    sb.append("  <reject />");
    sb.append("</poolPlacementPolicy>");
    PoolPlacementPolicy policy = parse(sb.toString());
    assertEquals("specifiedq", policy.assignJobToPool("specifiedq", "someuser"));
    assertEquals(null, policy.assignJobToPool("default", "someuser"));
  }
  
  @Test (expected = AllocationConfigurationException.class)
  public void testOmittedTerminalRule() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<poolPlacementPolicy>");
    sb.append("  <specified />");
    sb.append("  <user create=\"false\" />");
    sb.append("</poolPlacementPolicy>");
    parse(sb.toString());
  }
  
  @Test (expected = AllocationConfigurationException.class)
  public void testTerminalRuleInMiddle() throws Exception {
    StringBuffer sb = new StringBuffer();
    sb.append("<poolPlacementPolicy>");
    sb.append("  <specified />");
    sb.append("  <default />");
    sb.append("  <user />");
    sb.append("</poolPlacementPolicy>");
    parse(sb.toString());
  }
  
  private PoolPlacementPolicy parse(String str) throws Exception {
    // Read and parse the allocations file.
    DocumentBuilderFactory docBuilderFactory =
      DocumentBuilderFactory.newInstance();
    docBuilderFactory.setIgnoringComments(true);
    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    Document doc = builder.parse(new ByteArrayInputStream(str.getBytes()));
    Element root = doc.getDocumentElement();
    return PoolPlacementPolicy.fromXml(root, configuredPools, conf);
  }
}
