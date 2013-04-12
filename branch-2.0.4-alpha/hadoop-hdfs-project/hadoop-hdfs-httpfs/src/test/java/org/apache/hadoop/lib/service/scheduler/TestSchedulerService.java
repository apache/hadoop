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

package org.apache.hadoop.lib.service.scheduler;

import static org.junit.Assert.assertNotNull;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.lib.server.Server;
import org.apache.hadoop.lib.service.Scheduler;
import org.apache.hadoop.lib.service.instrumentation.InstrumentationService;
import org.apache.hadoop.test.HTestCase;
import org.apache.hadoop.test.TestDir;
import org.apache.hadoop.test.TestDirHelper;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

public class TestSchedulerService extends HTestCase {

  @Test
  @TestDir
  public void service() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    Configuration conf = new Configuration(false);
    conf.set("server.services", StringUtils.join(",", Arrays.asList(InstrumentationService.class.getName(),
                                                                    SchedulerService.class.getName())));
    Server server = new Server("server", dir, dir, dir, dir, conf);
    server.init();
    assertNotNull(server.get(Scheduler.class));
    server.destroy();
  }

}
