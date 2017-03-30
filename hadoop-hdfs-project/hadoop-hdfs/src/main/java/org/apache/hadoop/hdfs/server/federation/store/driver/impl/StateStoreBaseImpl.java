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
package org.apache.hadoop.hdfs.server.federation.store.driver.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdfs.server.federation.store.StateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;

/**
 * Base implementation of a State Store driver. It contains default
 * implementations for the optional functions. These implementations use an
 * uncached read/write all algorithm for all changes. In most cases it is
 * recommended to override the optional functions.
 * <p>
 * Drivers may optionally override additional routines for performance
 * optimization, such as custom get/put/remove queries, depending on the
 * capabilities of the data store.
 * <p>
 */
public abstract class StateStoreBaseImpl extends StateStoreDriver {

  @Override
  public <T extends BaseRecord> T get(
      Class<T> clazz, Map<String, String> query) throws IOException {
    List<T> records = getMultiple(clazz, query);
    if (records.size() > 1) {
      throw new IOException("Found more than one object in collection");
    } else if (records.size() == 1) {
      return records.get(0);
    } else {
      return null;
    }
  }

  @Override
  public <T extends BaseRecord> boolean put(
      T record, boolean allowUpdate, boolean errorIfExists) throws IOException {
    List<T> singletonList = new ArrayList<T>();
    singletonList.add(record);
    return putAll(singletonList, allowUpdate, errorIfExists);
  }

  @Override
  public <T extends BaseRecord> boolean remove(T record) throws IOException {
    Map<String, String> primaryKeys = record.getPrimaryKeys();
    Class<? extends BaseRecord> clazz = StateStoreUtils.getRecordClass(record);
    return remove(clazz, primaryKeys) == 1;
  }
}
