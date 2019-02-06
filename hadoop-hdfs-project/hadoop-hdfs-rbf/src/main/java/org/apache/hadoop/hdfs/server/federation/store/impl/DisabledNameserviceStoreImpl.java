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
package org.apache.hadoop.hdfs.server.federation.store.impl;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.store.DisabledNameserviceStore;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.records.DisabledNameservice;

/**
 * Implementation of {@link DisabledNameserviceStore}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DisabledNameserviceStoreImpl extends DisabledNameserviceStore {

  public DisabledNameserviceStoreImpl(StateStoreDriver driver) {
    super(driver);
  }

  @Override
  public boolean disableNameservice(String nsId)
      throws IOException {

    DisabledNameservice record =
        DisabledNameservice.newInstance(nsId);
    return getDriver().put(record, false, false);
  }

  @Override
  public boolean enableNameservice(String nsId)
      throws IOException {

    DisabledNameservice record =
        DisabledNameservice.newInstance(nsId);
    return getDriver().remove(record);
  }

  @Override
  public Set<String> getDisabledNameservices() throws IOException {
    Set<String> disabledNameservices = new TreeSet<>();
    for (DisabledNameservice record : getCachedRecords()) {
      String nsId = record.getNameserviceId();
      disabledNameservices.add(nsId);
    }
    return disabledNameservices;
  }
}
