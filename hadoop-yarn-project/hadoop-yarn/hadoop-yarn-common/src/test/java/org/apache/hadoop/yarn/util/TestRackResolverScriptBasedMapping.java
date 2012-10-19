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

package org.apache.hadoop.yarn.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.junit.Assert;
import org.junit.Test;

public class TestRackResolverScriptBasedMapping {

  @Test
  public void testScriptName() {
    Configuration conf = new Configuration();
    conf
        .setClass(
            CommonConfigurationKeysPublic.
                NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
            ScriptBasedMapping.class, DNSToSwitchMapping.class);
    conf.set(CommonConfigurationKeysPublic.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
        "testScript");
    RackResolver.init(conf);
    Assert.assertEquals(RackResolver.getDnsToSwitchMapping().toString(),
        "script-based mapping with script testScript");
  }
}
