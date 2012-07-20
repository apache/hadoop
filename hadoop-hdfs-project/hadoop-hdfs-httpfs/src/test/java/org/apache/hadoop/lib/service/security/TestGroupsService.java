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

package org.apache.hadoop.lib.service.security;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.lib.server.Server;
import org.apache.hadoop.lib.service.Groups;
import org.apache.hadoop.test.HTestCase;
import org.apache.hadoop.test.TestDir;
import org.apache.hadoop.test.TestDirHelper;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

public class TestGroupsService extends HTestCase {

  @Test
  @TestDir
  public void service() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set("server.services", StringUtils.join(",", Arrays.asList(GroupsService.class.getName())));
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    Groups groups = server.get(Groups.class);
    assertNotNull(groups);
    List<String> g = groups.getGroups(System.getProperty("user.name"));
    assertNotSame(g.size(), 0);
    server.destroy();
  }

  @Test(expected = RuntimeException.class)
  @TestDir
  public void invalidGroupsMapping() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set("server.services", StringUtils.join(",", Arrays.asList(GroupsService.class.getName())));
    conf.set("server.groups.hadoop.security.group.mapping", String.class.getName());
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
  }

}
