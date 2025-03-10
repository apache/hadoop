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
package org.apache.hadoop.yarn.server.nodemanager.nodelabels;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeKey;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test class for node configuration node attributes provider.
 */
public class TestConfigurationNodeAttributesProvider {

  private static File testRootDir = new File("target",
      TestConfigurationNodeAttributesProvider.class.getName() + "-localDir")
      .getAbsoluteFile();

  private ConfigurationNodeAttributesProvider nodeAttributesProvider;

  @BeforeAll
  public static void create() {
    testRootDir.mkdirs();
  }

  @BeforeEach
  public void setup() {
    nodeAttributesProvider = new ConfigurationNodeAttributesProvider();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (nodeAttributesProvider != null) {
      nodeAttributesProvider.close();
      nodeAttributesProvider.stop();
    }
  }

  @AfterAll
  public static void remove() throws Exception {
    if (testRootDir.exists()) {
      FileContext.getLocalFSFileContext()
          .delete(new Path(testRootDir.getAbsolutePath()), true);
    }
  }

  @Test
  @Timeout(30)
  public void testNodeAttributesFetchInterval()
      throws IOException, InterruptedException {
    Set<NodeAttribute> expectedAttributes1 = new HashSet<>();
    expectedAttributes1.add(NodeAttribute
        .newInstance("test.io", "host",
            NodeAttributeType.STRING, "host1"));

    Configuration conf = new Configuration();
    // Set fetch interval to 1s for testing
    conf.setLong(
        YarnConfiguration.NM_NODE_ATTRIBUTES_PROVIDER_FETCH_INTERVAL_MS, 1000);
    ConfigurationNodeAttributesProvider spyProvider =
        Mockito.spy(nodeAttributesProvider);
    Mockito.when(spyProvider.parseAttributes(Mockito.any()))
        .thenReturn(expectedAttributes1);

    spyProvider.init(conf);
    spyProvider.start();

    // Verify init value is honored.
    assertEquals(expectedAttributes1, spyProvider.getDescriptors());

    // Configuration provider provides a different set of attributes.
    Set<NodeAttribute> expectedAttributes2 = new HashSet<>();
    expectedAttributes2.add(NodeAttribute
        .newInstance("test.io", "os",
            NodeAttributeType.STRING, "windows"));
    Mockito.when(spyProvider.parseAttributes(Mockito.any()))
        .thenReturn(expectedAttributes2);

    // Since we set fetch interval to 1s, it needs to wait for 1s until
    // the updated attributes is updated to the provider. So we are expecting
    // to see some old values for a short window.
    ArrayList<String> keysMet = new ArrayList<>();
    int numOfOldValue = 0;
    int numOfNewValue = 0;
    // Run 5 times in 500ms interval
    int times=5;
    while(times>0) {
      Set<NodeAttribute> current = spyProvider.getDescriptors();
      assertEquals(1, current.size());
      String attributeName =
          current.iterator().next().getAttributeKey().getAttributeName();
      if ("host".equals(attributeName)){
        numOfOldValue++;
      } else if ("os".equals(attributeName)) {
        numOfNewValue++;
      }
      Thread.sleep(500);
      times--;
    }
    // We should either see the old value or the new value.
    assertEquals(5, numOfNewValue + numOfOldValue);
    // Both values should be more than 0.
    assertTrue(numOfOldValue > 0);
    assertTrue(numOfNewValue > 0);
  }

  @Test
  public void testDisableFetchNodeAttributes() throws IOException,
      InterruptedException {
    Set<NodeAttribute> expectedAttributes1 = new HashSet<>();
    expectedAttributes1.add(NodeAttribute
        .newInstance("test.io", "host",
            NodeAttributeType.STRING, "host1"));

    Configuration conf = new Configuration();
    // Set fetch interval to -1 to disable refresh.
    conf.setLong(
        YarnConfiguration.NM_NODE_ATTRIBUTES_PROVIDER_FETCH_INTERVAL_MS, -1);
    ConfigurationNodeAttributesProvider spyProvider =
        Mockito.spy(nodeAttributesProvider);
    Mockito.when(spyProvider.parseAttributes(Mockito.any()))
        .thenReturn(expectedAttributes1);
    spyProvider.init(conf);
    spyProvider.start();
    assertEquals(expectedAttributes1,
        spyProvider.getDescriptors());

    // The configuration added another attribute,
    // as we disabled the fetch interval, this value cannot be
    // updated to the provider.
    Set<NodeAttribute> expectedAttributes2 = new HashSet<>();
    expectedAttributes2.add(NodeAttribute
        .newInstance("test.io", "os",
            NodeAttributeType.STRING, "windows"));
    Mockito.when(spyProvider.parseAttributes(Mockito.anyString()))
        .thenReturn(expectedAttributes2);

    // Wait a few seconds until we get the value update, expecting a failure.
    try {
      GenericTestUtils.waitFor(() -> {
        Set<NodeAttribute> attributes = spyProvider.getDescriptors();
        return "os".equalsIgnoreCase(attributes
            .iterator().next().getAttributeKey().getAttributeName());
      }, 500, 1000);
    } catch (Exception e) {
      // Make sure we get the timeout exception.
      assertTrue(e instanceof TimeoutException);
      return;
    }

    fail("Expecting a failure in previous check!");
  }

  @Test
  public void testFetchAttributesFromConfiguration() {
    Configuration conf = new Configuration();
    // Set fetch interval to -1 to disable refresh.
    conf.setLong(
        YarnConfiguration.NM_NODE_ATTRIBUTES_PROVIDER_FETCH_INTERVAL_MS, -1);
    conf.setStrings(
        YarnConfiguration.NM_PROVIDER_CONFIGURED_NODE_ATTRIBUTES, "");
  }

  @Test
  public void testParseConfiguration() throws IOException {
    // ATTRIBUTE_NAME,ATTRIBUTE_TYPE,ATTRIBUTE_VALUE
    String attributesStr = "hostname,STRING,host1234:uptime,STRING,321543";
    Set<NodeAttribute> attributes = nodeAttributesProvider
        .parseAttributes(attributesStr);
    assertEquals(2, attributes.size());
    Iterator<NodeAttribute> ait = attributes.iterator();

    while(ait.hasNext()) {
      NodeAttribute attr = ait.next();
      NodeAttributeKey at = attr.getAttributeKey();
      if (at.getAttributeName().equals("hostname")) {
        assertEquals("hostname", at.getAttributeName());
        assertEquals(NodeAttribute.PREFIX_DISTRIBUTED,
            at.getAttributePrefix());
        assertEquals(NodeAttributeType.STRING,
            attr.getAttributeType());
        assertEquals("host1234", attr.getAttributeValue());
      } else if (at.getAttributeName().equals("uptime")) {
        assertEquals("uptime", at.getAttributeName());
        assertEquals(NodeAttribute.PREFIX_DISTRIBUTED,
            at.getAttributePrefix());
        assertEquals(NodeAttributeType.STRING,
            attr.getAttributeType());
        assertEquals("321543", attr.getAttributeValue());
      } else {
        fail("Unexpected attribute");
      }
    }
    // Missing type
    attributesStr = "hostname,host1234";
    try {
      nodeAttributesProvider.parseAttributes(attributesStr);
      fail("Expecting a parsing failure");
    } catch (IOException e) {
      assertNotNull(e);
      assertTrue(e.getMessage().contains("Invalid value"));
    }

    // Extra prefix
    attributesStr = "prefix/hostname,STRING,host1234";
    try {
      nodeAttributesProvider.parseAttributes(attributesStr);
      fail("Expecting a parsing failure");
    } catch (IOException e) {
      assertNotNull(e);
      assertTrue(e.getMessage()
          .contains("should not contain any prefix."));
    }

    // Invalid type
    attributesStr = "hostname,T,host1234";
    try {
      nodeAttributesProvider.parseAttributes(attributesStr);
      fail("Expecting a parsing failure");
    } catch (IOException e) {
      e.printStackTrace();
      assertNotNull(e);
      assertTrue(e.getMessage()
          .contains("Invalid node attribute type"));
    }
  }
}
