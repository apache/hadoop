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
package org.apache.hadoop.hdds.scm.net;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test the node schema loader. */
@RunWith(Parameterized.class)
public class TestNodeSchemaLoader {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestNodeSchemaLoader.class);
  private ClassLoader classLoader =
      Thread.currentThread().getContextClassLoader();

  public TestNodeSchemaLoader(String schemaFile, String errMsg) {
    try {
      String filePath = classLoader.getResource(
          "./networkTopologyTestFiles/" + schemaFile).getPath();
      NodeSchemaLoader.getInstance().loadSchemaFromXml(filePath);
      fail("expect exceptions");
    } catch (Throwable e) {
      assertTrue(e.getMessage().contains(errMsg));
    }
  }

  @Rule
  public Timeout testTimeout = new Timeout(30000);

  @Parameters
  public static Collection<Object[]> getSchemaFiles() {
    Object[][] schemaFiles = new Object[][]{
        {"enforce-error.xml", "layer without prefix defined"},
        {"invalid-cost.xml", "Cost should be positive number or 0"},
        {"multiple-leaf.xml", "Multiple LEAF layers are found"},
        {"multiple-root.xml", "Multiple ROOT layers are found"},
        {"no-leaf.xml", "No LEAF layer is found"},
        {"no-root.xml", "No ROOT layer is found"},
        {"path-layers-size-mismatch.xml",
            "Topology path depth doesn't match layer element numbers"},
        {"path-with-id-reference-failure.xml",
            "No layer found for id"},
        {"unknown-layer-type.xml", "Unsupported layer type"},
        {"wrong-path-order-1.xml",
            "Topology path doesn't start with ROOT layer"},
        {"wrong-path-order-2.xml", "Topology path doesn't end with LEAF layer"},
        {"no-topology.xml", "no or multiple <topology> element"},
        {"multiple-topology.xml", "no or multiple <topology> element"},
        {"invalid-version.xml", "Bad layoutversion value"},
    };
    return Arrays.asList(schemaFiles);
  }

  @Test
  public void testGood() {
    try {
      String filePath = classLoader.getResource(
          "./networkTopologyTestFiles/good.xml").getPath();
      NodeSchemaLoader.getInstance().loadSchemaFromXml(filePath);
    } catch (Throwable e) {
      fail("should succeed");
    }
  }

  @Test
  public void testNotExist() {
    String filePath = classLoader.getResource(
        "./networkTopologyTestFiles/good.xml").getPath() + ".backup";
    try {
      NodeSchemaLoader.getInstance().loadSchemaFromXml(filePath);
      fail("should fail");
    } catch (Throwable e) {
      assertTrue(e.getMessage().contains("file " + filePath + " is not found"));
    }
  }
}
