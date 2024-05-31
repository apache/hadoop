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

import static org.apache.hadoop.hdfs.server.federation.store.StateStoreUtils.filterMultiple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUtils;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.Query;
import org.apache.hadoop.hdfs.server.federation.store.records.QueryResult;

/**
 * Base implementation of a State Store driver. It contains default
 * implementations for the optional functions. These implementations use an
 * uncached read/write all algorithm for all changes. In most cases it is
 * recommended to override the optional functions.
 * <p>
 * Drivers may optionally override additional routines for performance
 * optimization, such as custom get/put/remove queries, depending on the
 * capabilities of the data store.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class StateStoreBaseImpl extends StateStoreDriver {

  @Override
  public <T extends BaseRecord> T get(
      Class<T> clazz, Query<T> query) throws IOException {
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
  public <T extends BaseRecord> List<T> getMultiple(
      Class<T> clazz, Query<T> query) throws IOException  {
    QueryResult<T> result = get(clazz);
    List<T> records = result.getRecords();
    List<T> ret = filterMultiple(query, records);
    if (ret == null) {
      throw new IOException("Cannot fetch records from the store");
    }
    return ret;
  }

  @Override
  public <T extends BaseRecord> boolean put(
      T record, boolean allowUpdate, boolean errorIfExists) throws IOException {
    List<T> singletonList = new ArrayList<>();
    singletonList.add(record);
    return putAll(singletonList, allowUpdate, errorIfExists).isOperationSuccessful();
  }

  @Override
  public <T extends BaseRecord> boolean remove(T record) throws IOException {
    final Query<T> query = new Query<T>(record);
    Class<? extends BaseRecord> clazz = record.getClass();
    @SuppressWarnings("unchecked")
    Class<T> recordClass = (Class<T>)StateStoreUtils.getRecordClass(clazz);
    return remove(recordClass, query) == 1;
  }

  @Override
  public <T extends BaseRecord> Map<T, Boolean> removeMultiple(List<T> records) throws IOException {
    assert !records.isEmpty();
    // Fall back to iterative remove() calls if all records don't share 1 class
    Class<? extends BaseRecord> expectedClazz = records.get(0).getClass();
    if (!records.stream().allMatch(x -> x.getClass() == expectedClazz)) {
      Map<T, Boolean> result = new HashMap<>();
      for (T record : records) {
        result.put(record, remove(record));
      }
      return result;
    }

    final List<Query<T>> queries = new ArrayList<>();
    for (T record : records) {
      queries.add(new Query<>(record));
    }
    @SuppressWarnings("unchecked")
    Class<T> recordClass = (Class<T>) StateStoreUtils.getRecordClass(expectedClazz);
    Map<Query<T>, Integer> result = remove(recordClass, queries);
    return result.entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().getPartial(), e -> e.getValue() == 1));
  }

  public <T extends BaseRecord> Map<Query<T>, Integer> remove(Class<T> clazz,
      List<Query<T>> queries) throws IOException {
    Map<Query<T>, Integer> result = new HashMap<>();
    for (Query<T> query : queries) {
      result.put(query, remove(clazz, query));
    }
    return result;
  }
}