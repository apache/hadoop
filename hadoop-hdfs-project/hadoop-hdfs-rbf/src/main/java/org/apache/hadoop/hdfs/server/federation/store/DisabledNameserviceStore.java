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

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.records.DisabledNameservice;

/**
 * State store record to track disabled name services.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class DisabledNameserviceStore
    extends CachedRecordStore<DisabledNameservice> {

  public DisabledNameserviceStore(StateStoreDriver driver) {
    super(DisabledNameservice.class, driver);
  }

  /**
   * Disable a name service.
   *
   * @param nsId Identifier of the name service.
   * @return If the name service was successfully disabled.
   * @throws IOException If the state store could not be queried.
   */
  public abstract boolean disableNameservice(String nsId) throws IOException;

  /**
   * Enable a name service.
   *
   * @param nsId Identifier of the name service.
   * @return If the name service was successfully brought back.
   * @throws IOException If the state store could not be queried.
   */
  public abstract boolean enableNameservice(String nsId) throws IOException;

  /**
   * Get a list of disabled name services.
   *
   * @return List of disabled name services.
   * @throws IOException If the state store could not be queried.
   */
  public abstract Set<String> getDisabledNameservices() throws IOException;
}