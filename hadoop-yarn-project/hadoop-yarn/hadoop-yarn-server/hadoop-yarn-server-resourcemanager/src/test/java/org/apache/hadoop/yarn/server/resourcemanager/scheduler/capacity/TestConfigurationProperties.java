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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class TestConfigurationProperties {
  private static final Map<String, String> PROPERTIES = new HashMap<>();

  @BeforeAll
  static void setUpClass() throws Exception {
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
  void testGetPropertiesWithPrefix() {
    ConfigurationProperties configurationProperties =
        new ConfigurationProperties(PROPERTIES);

    Map<String, String> props = configurationProperties
        .getPropertiesWithPrefix("root.1.2");

    Assertions.assertEquals(4, props.size());
    Assertions.assertTrue(props.containsKey(""));
    Assertions.assertEquals("TEST_VALUE_3", props.get(""));
    Assertions.assertTrue(props.containsKey("4"));
    Assertions.assertEquals("TEST_VALUE_3_1", props.get("4"));
    Assertions.assertTrue(props.containsKey("3"));
    Assertions.assertEquals("TEST_VALUE_1", props.get("3"));
    Assertions.assertTrue(props.containsKey("4.5"));
    Assertions.assertEquals("TEST_VALUE_3_2", props.get("4.5"));

    // Test the scenario where the prefix has a dot appended to it
    // (see CapacitySchedulerConfiguration.getQueuePrefix(String queue)).
    // The dot is disregarded.
    props = configurationProperties
        .getPropertiesWithPrefix("root.1.2.4.");

    Assertions.assertEquals(2, props.size());
    Assertions.assertTrue(props.containsKey(""));
    Assertions.assertEquals("TEST_VALUE_3_1", props.get(""));
    Assertions.assertTrue(props.containsKey("5"));
    Assertions.assertEquals("TEST_VALUE_3_2", props.get("5"));

    Map<String, String> propsWithRootPrefix = configurationProperties
        .getPropertiesWithPrefix("root");

    Assertions.assertEquals(6, propsWithRootPrefix.size());
    Assertions.assertTrue(propsWithRootPrefix.containsKey(""));
    Assertions.assertEquals("TEST_VALUE_4", propsWithRootPrefix.get(""));
    Assertions.assertTrue(propsWithRootPrefix.containsKey("1.2.3"));
    Assertions.assertEquals("TEST_VALUE_1", propsWithRootPrefix.get("1.2.3"));
    Assertions.assertTrue(propsWithRootPrefix.containsKey("1"));
    Assertions.assertEquals("TEST_VALUE_2", propsWithRootPrefix.get("1"));
    Assertions.assertTrue(propsWithRootPrefix.containsKey("1.2"));
    Assertions.assertEquals("TEST_VALUE_3", propsWithRootPrefix.get("1.2"));
    Assertions.assertTrue(propsWithRootPrefix.containsKey("1.2.4"));
    Assertions.assertEquals("TEST_VALUE_3_1", propsWithRootPrefix.get("1.2.4"));
    Assertions.assertTrue(propsWithRootPrefix.containsKey("1.2.4.5"));
    Assertions.assertEquals("TEST_VALUE_3_2", propsWithRootPrefix.get("1.2.4.5"));
  }

  @Test
  void testGetPropertiesWithFullyQualifiedName() {
    ConfigurationProperties configurationProperties =
        new ConfigurationProperties(PROPERTIES);

    Map<String, String> props = configurationProperties
        .getPropertiesWithPrefix("root.1.2", true);

    Assertions.assertEquals(4, props.size());
    Assertions.assertTrue(props.containsKey("root.1.2.3"));
    Assertions.assertEquals("TEST_VALUE_1", props.get("root.1.2.3"));
    Assertions.assertTrue(props.containsKey("root.1.2"));
    Assertions.assertEquals("TEST_VALUE_3", props.get("root.1.2"));
    Assertions.assertTrue(props.containsKey("root.1.2.4.5"));
    Assertions.assertEquals("TEST_VALUE_3_2", props.get("root.1.2.4.5"));
    Assertions.assertTrue(props.containsKey("root.1.2.4"));
    Assertions.assertEquals("TEST_VALUE_3_1", props.get("root.1.2.4"));
  }

  @Test
  void testGetPropertiesWithPrefixEmptyResult() {
    ConfigurationProperties configurationProperties =
        new ConfigurationProperties(PROPERTIES);

    Map<String, String> propsEmptyPrefix = configurationProperties.getPropertiesWithPrefix("");
    Map<String, String> propsLongPrefix = configurationProperties
        .getPropertiesWithPrefix("root.1.2.4.5.6");
    Map<String, String> propsNonExistingRootPrefix = configurationProperties
        .getPropertiesWithPrefix("3");

    Assertions.assertEquals(0, propsEmptyPrefix.size());
    Assertions.assertEquals(0, propsLongPrefix.size());
    Assertions.assertEquals(0, propsNonExistingRootPrefix.size());
  }
}