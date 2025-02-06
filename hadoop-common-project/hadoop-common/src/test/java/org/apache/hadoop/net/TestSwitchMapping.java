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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Test some other details of the switch mapping
 */
public class TestSwitchMapping extends Assertions {


  /**
   * Verify the switch mapping query handles arbitrary DNSToSwitchMapping
   * implementations
   *
   * @throws Throwable on any problem
   */
  @Test
  public void testStandaloneClassesAssumedMultiswitch() throws Throwable {
    DNSToSwitchMapping mapping = new StandaloneSwitchMapping();
    assertFalse(AbstractDNSToSwitchMapping.isMappingSingleSwitch(mapping),
        "Expected to be multi switch " + mapping);
  }


  /**
   * Verify the cached mapper delegates the switch mapping query to the inner
   * mapping, which again handles arbitrary DNSToSwitchMapping implementations
   *
   * @throws Throwable on any problem
   */
  @Test
  public void testCachingRelays() throws Throwable {
    CachedDNSToSwitchMapping mapping =
        new CachedDNSToSwitchMapping(new StandaloneSwitchMapping());
    assertFalse(mapping.isSingleSwitch(),
        "Expected to be multi switch " + mapping);
  }


  /**
   * Verify the cached mapper delegates the switch mapping query to the inner
   * mapping, which again handles arbitrary DNSToSwitchMapping implementations
   *
   * @throws Throwable on any problem
   */
  @Test
  public void testCachingRelaysStringOperations() throws Throwable {
    Configuration conf = new Configuration();
    String scriptname = "mappingscript.sh";
    conf.set(CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
             scriptname);
    ScriptBasedMapping scriptMapping = new ScriptBasedMapping(conf);
    assertTrue(scriptMapping.toString().contains(scriptname),
        "Did not find " + scriptname + " in " + scriptMapping);
    CachedDNSToSwitchMapping mapping =
        new CachedDNSToSwitchMapping(scriptMapping);
    assertTrue(mapping.toString().contains(scriptname),
        "Did not find " + scriptname + " in " + mapping);
  }

  /**
   * Verify the cached mapper delegates the switch mapping query to the inner
   * mapping, which again handles arbitrary DNSToSwitchMapping implementations
   *
   * @throws Throwable on any problem
   */
  @Test
  public void testCachingRelaysStringOperationsToNullScript() throws Throwable {
    Configuration conf = new Configuration();
    ScriptBasedMapping scriptMapping = new ScriptBasedMapping(conf);
    assertTrue(scriptMapping.toString().contains(ScriptBasedMapping.NO_SCRIPT),
        "Did not find " + ScriptBasedMapping.NO_SCRIPT + " in " + scriptMapping);
    CachedDNSToSwitchMapping mapping =
        new CachedDNSToSwitchMapping(scriptMapping);
    assertTrue(mapping.toString().contains(ScriptBasedMapping.NO_SCRIPT),
        "Did not find " + ScriptBasedMapping.NO_SCRIPT + " in " + mapping);
  }

  @Test
  public void testNullMapping() {
    assertFalse(AbstractDNSToSwitchMapping.isMappingSingleSwitch(null));
  }

  /**
   * This class does not extend the abstract switch mapping, and verifies that
   * the switch mapping logic assumes that this is multi switch
   */

  private static class StandaloneSwitchMapping implements DNSToSwitchMapping {
    @Override
    public List<String> resolve(List<String> names) {
      return names;
    }

    @Override
    public void reloadCachedMappings() {
    }

    @Override
    public void reloadCachedMappings(List<String> names) {
    }
  }
}
