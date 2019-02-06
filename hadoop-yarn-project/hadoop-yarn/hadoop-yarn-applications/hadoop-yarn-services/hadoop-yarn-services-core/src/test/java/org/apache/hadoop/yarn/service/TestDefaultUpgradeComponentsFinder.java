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
package org.apache.hadoop.yarn.service;

import com.google.common.collect.Lists;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.ConfigFile;
import org.apache.hadoop.yarn.service.api.records.Configuration;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link UpgradeComponentsFinder.DefaultUpgradeComponentsFinder}.
 */
public class TestDefaultUpgradeComponentsFinder {

  private UpgradeComponentsFinder.DefaultUpgradeComponentsFinder finder =
      new UpgradeComponentsFinder.DefaultUpgradeComponentsFinder();

  @Test
  public void testServiceArtifactChange() {
    Service currentDef = ServiceTestUtils.createExampleApplication();
    Service targetDef =  ServiceTestUtils.createExampleApplication();
    targetDef.getComponents().forEach(x -> x.setArtifact(
        TestServiceManager.createTestArtifact("v1")));

    assertEquals("all components need upgrade",
        targetDef.getComponents(), finder.findTargetComponentSpecs(currentDef,
            targetDef));
  }

  @Test
  public void testServiceUpgradeWithNewComponentAddition() {
    Service currentDef = ServiceTestUtils.createExampleApplication();
    Service targetDef = ServiceTestUtils.createExampleApplication();
    Iterator<Component> targetComponentsIter =
        targetDef.getComponents().iterator();
    Component firstComponent = targetComponentsIter.next();
    firstComponent.setName("newComponentA");

    try {
      finder.findTargetComponentSpecs(currentDef, targetDef);
      Assert.fail("Expected error since component does not exist in service "
          + "definition");
    } catch (UnsupportedOperationException usoe) {
      assertEquals(
          "addition/deletion of components not supported by upgrade. Could "
              + "not find component newComponentA in current service "
              + "definition.",
          usoe.getMessage());
      //Expected
    }
  }

  @Test
  public void testComponentArtifactChange() {
    Service currentDef = TestServiceManager.createBaseDef("test");
    Service targetDef =  TestServiceManager.createBaseDef("test");

    targetDef.getComponents().get(0).setArtifact(
        TestServiceManager.createTestArtifact("v2"));

    List<Component> expected = new ArrayList<>();
    expected.add(targetDef.getComponents().get(0));

    assertEquals("single components needs upgrade",
        expected, finder.findTargetComponentSpecs(currentDef,
            targetDef));
  }

  @Test
  public void testChangeInConfigFileProperty() {
    ConfigFile file = new ConfigFile().srcFile("src").destFile("dest")
        .type(ConfigFile.TypeEnum.HADOOP_XML);

    Map<String, String> props = new HashMap<>();
    props.put("k1", "v1");
    file.setProperties(props);

    Configuration conf = new Configuration().files(Lists.newArrayList(file));

    Service currentDef = TestServiceManager.createBaseDef("test");
    currentDef.setConfiguration(conf);

    // new spec has changes in config file property
    file = new ConfigFile().srcFile("src").destFile("dest")
        .type(ConfigFile.TypeEnum.HADOOP_XML);
    Map<String, String> changedProps = new HashMap<>();
    changedProps.put("k1", "v2");
    file.setProperties(changedProps);

    conf = new Configuration().files(Lists.newArrayList(file));

    Service targetDef =  TestServiceManager.createBaseDef("test");
    targetDef.setConfiguration(conf);

    List<Component> expected = new ArrayList<>();
    expected.addAll(targetDef.getComponents());

    assertEquals("all components needs upgrade",
        expected, finder.findTargetComponentSpecs(currentDef, targetDef));
  }
}