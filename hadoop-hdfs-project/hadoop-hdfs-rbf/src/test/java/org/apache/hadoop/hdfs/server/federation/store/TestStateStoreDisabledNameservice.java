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
package org.apache.hadoop.hdfs.server.federation.store;

import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.clearRecords;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.hdfs.server.federation.store.records.DisabledNameservice;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the basic {@link StateStoreService}
 * {@link DisabledNameserviceStore} functionality.
 */
public class TestStateStoreDisabledNameservice extends TestStateStoreBase {

  private static DisabledNameserviceStore disabledStore;

  @Before
  public void setup() throws IOException, InterruptedException {
    disabledStore = getStateStore()
        .getRegisteredRecordStore(DisabledNameserviceStore.class);
    // Clear disabled nameservice registrations
    assertTrue(clearRecords(getStateStore(), DisabledNameservice.class));
  }

  @Test
  public void testDisableNameservice() throws IOException {
    // no nameservices disabled firstly
    Set<String> disabledNameservices = disabledStore.getDisabledNameservices();
    assertEquals(0, disabledNameservices.size());

    // disable two nameservices
    disabledStore.disableNameservice("ns0");
    disabledStore.disableNameservice("ns1");
    disabledStore.loadCache(true);
    // verify if the nameservices are disabled
    disabledNameservices = disabledStore.getDisabledNameservices();
    assertEquals(2, disabledNameservices.size());
    assertTrue(disabledNameservices.contains("ns0")
        && disabledNameservices.contains("ns1"));

    // enable one nameservice
    disabledStore.enableNameservice("ns0");
    disabledStore.loadCache(true);
    // verify the disabled nameservice again
    disabledNameservices = disabledStore.getDisabledNameservices();
    assertEquals(1, disabledNameservices.size());
    assertTrue(disabledNameservices.contains("ns1"));
  }
}