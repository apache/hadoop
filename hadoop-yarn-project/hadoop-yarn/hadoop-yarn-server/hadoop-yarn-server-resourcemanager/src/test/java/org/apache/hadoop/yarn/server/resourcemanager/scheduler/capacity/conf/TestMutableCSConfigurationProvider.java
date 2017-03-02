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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link MutableCSConfigurationProvider}.
 */
public class TestMutableCSConfigurationProvider {

  private MutableCSConfigurationProvider confProvider;
  private RMContext rmContext;
  private Map<String, String> goodUpdate;
  private Map<String, String> badUpdate;
  private CapacityScheduler cs;

  private static final String TEST_USER = "testUser";

  @Before
  public void setUp() {
    cs = mock(CapacityScheduler.class);
    rmContext = mock(RMContext.class);
    when(rmContext.getScheduler()).thenReturn(cs);
    confProvider = new MutableCSConfigurationProvider(rmContext);
    goodUpdate = new HashMap<>();
    goodUpdate.put("goodKey", "goodVal");
    badUpdate = new HashMap<>();
    badUpdate.put("badKey", "badVal");
  }

  @Test
  public void testInMemoryBackedProvider() throws IOException {
    Configuration conf = new Configuration();
    confProvider.init(conf);
    assertNull(confProvider.loadConfiguration(conf)
        .get("goodKey"));

    doNothing().when(cs).reinitialize(any(Configuration.class),
        any(RMContext.class));
    confProvider.mutateConfiguration(TEST_USER, goodUpdate);
    assertEquals("goodVal", confProvider.loadConfiguration(conf)
        .get("goodKey"));

    assertNull(confProvider.loadConfiguration(conf).get("badKey"));
    doThrow(new IOException()).when(cs).reinitialize(any(Configuration.class),
        any(RMContext.class));
    confProvider.mutateConfiguration(TEST_USER, badUpdate);
    assertNull(confProvider.loadConfiguration(conf).get("badKey"));
  }
}
