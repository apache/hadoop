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
package org.apache.hadoop.hdfs.server.federation.store.records;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.federation.router.RouterServiceState;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;
import org.junit.Test;

/**
 * Test the Router State records.
 */
public class TestRouterState {

  private static final String ADDRESS = "address";
  private static final String VERSION = "version";
  private static final String COMPILE_INFO = "compileInfo";
  private static final long START_TIME = 100;
  private static final long DATE_MODIFIED = 200;
  private static final long DATE_CREATED = 300;
  private static final long FILE_RESOLVER_VERSION = 500;
  private static final RouterServiceState STATE = RouterServiceState.RUNNING;


  private RouterState generateRecord() throws IOException {
    RouterState record = RouterState.newInstance(ADDRESS, START_TIME, STATE);
    record.setVersion(VERSION);
    record.setCompileInfo(COMPILE_INFO);
    record.setDateCreated(DATE_CREATED);
    record.setDateModified(DATE_MODIFIED);

    StateStoreVersion version = StateStoreVersion.newInstance();
    version.setMountTableVersion(FILE_RESOLVER_VERSION);
    record.setStateStoreVersion(version);
    return record;
  }

  private void validateRecord(RouterState record) throws IOException {
    assertEquals(ADDRESS, record.getAddress());
    assertEquals(START_TIME, record.getDateStarted());
    assertEquals(STATE, record.getStatus());
    assertEquals(COMPILE_INFO, record.getCompileInfo());
    assertEquals(VERSION, record.getVersion());

    StateStoreVersion version = record.getStateStoreVersion();
    assertEquals(FILE_RESOLVER_VERSION, version.getMountTableVersion());
  }

  @Test
  public void testGetterSetter() throws IOException {
    RouterState record = generateRecord();
    validateRecord(record);
  }

  @Test
  public void testSerialization() throws IOException {

    RouterState record = generateRecord();

    StateStoreSerializer serializer = StateStoreSerializer.getSerializer();
    String serializedString = serializer.serializeString(record);
    RouterState newRecord =
        serializer.deserialize(serializedString, RouterState.class);

    validateRecord(newRecord);
  }
}
