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

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;

/**
 * Data record indicating a specific name service ID has been disabled and
 * is no longer valid. Allows quick disabling of name services.
 */
public abstract class DisabledNameservice extends BaseRecord {

  public DisabledNameservice() {
    super();
  }

  public static DisabledNameservice newInstance() throws IOException {
    DisabledNameservice record =
        StateStoreSerializer.newRecord(DisabledNameservice.class);
    record.init();
    return record;
  }

  public static DisabledNameservice newInstance(String nsId)
      throws IOException {
    DisabledNameservice record = newInstance();
    record.setNameserviceId(nsId);
    return record;
  }

  /**
   * Get the identifier of the name service to disable.
   *
   * @return Identifier of the name service to disable.
   */
  public abstract String getNameserviceId();

  /**
   * Set the identifier of the name service to disable.
   *
   * @param nameServiceId Identifier of the name service to disable.
   */
  public abstract void setNameserviceId(String nameServiceId);

  @Override
  public SortedMap<String, String> getPrimaryKeys() {
    SortedMap<String, String> keyMap = new TreeMap<>();
    keyMap.put("nameServiceId", this.getNameserviceId());
    return keyMap;
  }

  @Override
  public boolean hasOtherFields() {
    // We don't have fields other than the primary keys
    return false;
  }

  @Override
  public long getExpirationMs() {
    return -1;
  }
}
