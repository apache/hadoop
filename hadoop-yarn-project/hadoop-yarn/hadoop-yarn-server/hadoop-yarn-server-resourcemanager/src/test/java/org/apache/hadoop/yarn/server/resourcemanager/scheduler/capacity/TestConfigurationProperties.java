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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestConfigurationProperties {
  private static final Map<String, String> PROPERTIES = new HashMap<>();

  @BeforeClass
  public static void setUpClass() throws Exception {
    PROPERTIES.put("root.1.2.3", "TEST_VALUE_1");
    PROPERTIES.put("root.1", "TEST_VALUE_2");
    PROPERTIES.put("root.1.2", "TEST_VALUE_3");
    PROPERTIES.put("root.1.2.4", "TEST_VALUE_3_1");
    PROPERTIES.put("root.1.2.4.5", "TEST_VALUE_3_2");
    PROPERTIES.put("root", "TEST_VALUE_4");
    PROPERTIES.put("2", "TEST_VALUE_5");
    PROPERTIES.put("2.3", "TEST_VALUE_5");
  }

  @Test
  public void testGetPropertiesWithPrefix() {
    ConfigurationProperties configurationProperties =
        new ConfigurationProperties(PROPERTIES);

    Map<String, String> props = configurationProperties
        .getPropertiesWithPrefix("root.1.2");

    Assert.assertEquals(4, props.size());
    Assert.assertTrue(props.containsKey(""));
    Assert.assertEquals("TEST_VALUE_3", props.get(""));
    Assert.assertTrue(props.containsKey("4"));
    Assert.assertEquals("TEST_VALUE_3_1", props.get("4"));
    Assert.assertTrue(props.containsKey("3"));
    Assert.assertEquals("TEST_VALUE_1", props.get("3"));
    Assert.assertTrue(props.containsKey("4.5"));
    Assert.assertEquals("TEST_VALUE_3_2", props.get("4.5"));

    // Test the scenario where the prefix has a dot appended to it
    // (see CapacitySchedulerConfiguration.getQueuePrefix(String queue)).
    // The dot is disregarded.
    props = configurationProperties
        .getPropertiesWithPrefix("root.1.2.4.");

    Assert.assertEquals(2, props.size());
    Assert.assertTrue(props.containsKey(""));
    Assert.assertEquals("TEST_VALUE_3_1", props.get(""));
    Assert.assertTrue(props.containsKey("5"));
    Assert.assertEquals("TEST_VALUE_3_2", props.get("5"));

    Map<String, String> propsWithRootPrefix = configurationProperties
        .getPropertiesWithPrefix("root");

    Assert.assertEquals(6, propsWithRootPrefix.size());
    Assert.assertTrue(propsWithRootPrefix.containsKey(""));
    Assert.assertEquals("TEST_VALUE_4", propsWithRootPrefix.get(""));
    Assert.assertTrue(propsWithRootPrefix.containsKey("1.2.3"));
    Assert.assertEquals("TEST_VALUE_1", propsWithRootPrefix.get("1.2.3"));
    Assert.assertTrue(propsWithRootPrefix.containsKey("1"));
    Assert.assertEquals("TEST_VALUE_2", propsWithRootPrefix.get("1"));
    Assert.assertTrue(propsWithRootPrefix.containsKey("1.2"));
    Assert.assertEquals("TEST_VALUE_3", propsWithRootPrefix.get("1.2"));
    Assert.assertTrue(propsWithRootPrefix.containsKey("1.2.4"));
    Assert.assertEquals("TEST_VALUE_3_1", propsWithRootPrefix.get("1.2.4"));
    Assert.assertTrue(propsWithRootPrefix.containsKey("1.2.4.5"));
    Assert.assertEquals("TEST_VALUE_3_2", propsWithRootPrefix.get("1.2.4.5"));
  }

  @Test
  public void testGetPropertiesWithFullyQualifiedName() {
    ConfigurationProperties configurationProperties =
        new ConfigurationProperties(PROPERTIES);

    Map<String, String> props = configurationProperties
        .getPropertiesWithPrefix("root.1.2", true);

    Assert.assertEquals(4, props.size());
    Assert.assertTrue(props.containsKey("root.1.2.3"));
    Assert.assertEquals("TEST_VALUE_1", props.get("root.1.2.3"));
    Assert.assertTrue(props.containsKey("root.1.2"));
    Assert.assertEquals("TEST_VALUE_3", props.get("root.1.2"));
    Assert.assertTrue(props.containsKey("root.1.2.4.5"));
    Assert.assertEquals("TEST_VALUE_3_2", props.get("root.1.2.4.5"));
    Assert.assertTrue(props.containsKey("root.1.2.4"));
    Assert.assertEquals("TEST_VALUE_3_1", props.get("root.1.2.4"));
  }

  @Test
  public void testGetPropertiesWithPrefixEmptyResult() {
    ConfigurationProperties configurationProperties =
        new ConfigurationProperties(PROPERTIES);

    Map<String, String> propsEmptyPrefix = configurationProperties.getPropertiesWithPrefix("");
    Map<String, String> propsLongPrefix = configurationProperties
        .getPropertiesWithPrefix("root.1.2.4.5.6");
    Map<String, String> propsNonExistingRootPrefix = configurationProperties
        .getPropertiesWithPrefix("3");

    Assert.assertEquals(0, propsEmptyPrefix.size());
    Assert.assertEquals(0, propsLongPrefix.size());
    Assert.assertEquals(0, propsNonExistingRootPrefix.size());
  }
}