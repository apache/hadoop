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
package org.apache.hadoop.net;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.conf.Configuration;

public class TestScriptBasedMappingWithDependency {

  
  public TestScriptBasedMappingWithDependency() {

  }

  @Test
  public void testNoArgsMeansNoResult() {
    Configuration conf = new Configuration();
    conf.setInt(ScriptBasedMapping.SCRIPT_ARG_COUNT_KEY,
                ScriptBasedMapping.MIN_ALLOWABLE_ARGS - 1);
    conf.set(ScriptBasedMapping.SCRIPT_FILENAME_KEY, "any-filename-1");
    conf.set(ScriptBasedMappingWithDependency.DEPENDENCY_SCRIPT_FILENAME_KEY, 
        "any-filename-2");
    conf.setInt(ScriptBasedMapping.SCRIPT_ARG_COUNT_KEY, 10);

    ScriptBasedMappingWithDependency mapping = createMapping(conf);
    List<String> names = new ArrayList<String>();
    names.add("some.machine.name");
    names.add("other.machine.name");
    List<String> result = mapping.resolve(names);
    assertNull(result, "Expected an empty list for resolve");
    result = mapping.getDependency("some.machine.name");
    assertNull(result, "Expected an empty list for getDependency");
  }

  @Test
  public void testNoFilenameMeansSingleSwitch() throws Throwable {
    Configuration conf = new Configuration();
    ScriptBasedMapping mapping = createMapping(conf);
    assertTrue(mapping.isSingleSwitch(), "Expected to be single switch");
    assertTrue(AbstractDNSToSwitchMapping.isMappingSingleSwitch(mapping),
        "Expected to be single switch");
  }

  @Test
  public void testFilenameMeansMultiSwitch() throws Throwable {
    Configuration conf = new Configuration();
    conf.set(ScriptBasedMapping.SCRIPT_FILENAME_KEY, "any-filename");
    ScriptBasedMapping mapping = createMapping(conf);
    assertFalse(mapping.isSingleSwitch(), "Expected to be multi switch");
    mapping.setConf(new Configuration());
    assertTrue(mapping.isSingleSwitch(), "Expected to be single switch");
  }

  @Test
  public void testNullConfig() throws Throwable {
    ScriptBasedMapping mapping = createMapping(null);
    assertTrue(mapping.isSingleSwitch(), "Expected to be single switch");
  }

  private ScriptBasedMappingWithDependency createMapping(Configuration conf) {
    ScriptBasedMappingWithDependency mapping = 
        new ScriptBasedMappingWithDependency();
    mapping.setConf(conf);
    return mapping;
  }
}
