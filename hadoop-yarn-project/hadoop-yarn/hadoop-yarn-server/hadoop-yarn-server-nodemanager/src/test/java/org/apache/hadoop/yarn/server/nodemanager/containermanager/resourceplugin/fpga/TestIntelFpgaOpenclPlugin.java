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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestIntelFpgaOpenclPlugin {
  private IntelFpgaOpenclPlugin plugin;

  @Before
  public void setup() {
    plugin = new IntelFpgaOpenclPlugin();
  }

  @Test
  public void testLocalizedIPfileFound() {
    Map<Path, List<String>> resources = createResources();

    String path = plugin.retrieveIPfilePath("fpga", "workDir", resources);

    assertEquals("Retrieved IP file path", "/test/fpga.aocx", path);
  }

  @Test
  public void testLocalizedIPfileNotFound() {
    Map<Path, List<String>> resources = createResources();

    String path = plugin.retrieveIPfilePath("dummy", "workDir", resources);

    assertNull("Retrieved IP file path", path);
  }

  @Test
  public void testLocalizedIpfileNotFoundWithNoLocalResources() {
    String path = plugin.retrieveIPfilePath("fpga", "workDir", null);

    assertNull("Retrieved IP file path", path);
  }

  @Test
  public void testIPfileNotDefined() {
    Map<Path, List<String>> resources = createResources();

    String path = plugin.retrieveIPfilePath(null, "workDir", resources);

    assertNull("Retrieved IP file path", path);
  }

  private Map<Path, List<String>> createResources() {
    Map<Path, List<String>> resources = new HashMap<>();
    resources.put(new Path("/test/fpga.aocx"),
        Lists.newArrayList("/symlinked/fpga.aocx"));
    return resources;
  }
}
