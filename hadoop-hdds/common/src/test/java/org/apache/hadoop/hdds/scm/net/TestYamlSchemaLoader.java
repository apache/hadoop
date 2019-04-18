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
public class TestYamlSchemaLoader {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestYamlSchemaLoader.class);
  private ClassLoader classLoader =
      Thread.currentThread().getContextClassLoader();

  public TestYamlSchemaLoader(String schemaFile, String errMsg) {
    try {
      String filePath = classLoader.getResource(
          "./networkTopologyTestFiles/" + schemaFile).getPath();
      NodeSchemaLoader.getInstance().loadSchemaFromYaml(filePath);
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
        {"multiple-root.yaml", "Multiple root"},
        {"middle-leaf.yaml", "Leaf node in the middle"},
    };
    return Arrays.asList(schemaFiles);
  }


  @Test
  public void testGood() {
    try {
      String filePath = classLoader.getResource(
              "./networkTopologyTestFiles/good.yaml").getPath();
      NodeSchemaLoader.getInstance().loadSchemaFromYaml(filePath);
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
