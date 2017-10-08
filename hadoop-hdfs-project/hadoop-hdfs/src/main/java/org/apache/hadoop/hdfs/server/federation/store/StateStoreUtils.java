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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Set of utility functions used to work with the State Store.
 */
public final class StateStoreUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(StateStoreUtils.class);


  private StateStoreUtils() {
    // Utility class
  }

  /**
   * Get the base class for a record class. If we get an implementation of a
   * record we will return the real parent record class.
   *
   * @param clazz Class of the data record to check.
   * @return Base class for the record.
   */
  @SuppressWarnings("unchecked")
  public static <T extends BaseRecord>
      Class<? extends BaseRecord> getRecordClass(final Class<T> clazz) {

    // We ignore the Impl classes and go to the super class
    Class<? extends BaseRecord> actualClazz = clazz;
    while (actualClazz.getSimpleName().endsWith("Impl")) {
      actualClazz = (Class<? extends BaseRecord>) actualClazz.getSuperclass();
    }

    // Check if we went too far
    if (actualClazz.equals(BaseRecord.class)) {
      LOG.error("We went too far ({}) with {}", actualClazz, clazz);
      actualClazz = clazz;
    }
    return actualClazz;
  }

  /**
   * Get the base class for a record. If we get an implementation of a record we
   * will return the real parent record class.
   *
   * @param record Record to check its main class.
   * @return Base class for the record.
   */
  public static <T extends BaseRecord>
      Class<? extends BaseRecord> getRecordClass(final T record) {
    return getRecordClass(record.getClass());
  }

  /**
   * Get the base class name for a record. If we get an implementation of a
   * record we will return the real parent record class.
   *
   * @param clazz Class of the data record to check.
   * @return Name of the base class for the record.
   */
  public static <T extends BaseRecord> String getRecordName(
      final Class<T> clazz) {
    return getRecordClass(clazz).getSimpleName();
  }

  /**
   * Filters a list of records to find all records matching the query.
   *
   * @param query Map of field names and objects to use to filter results.
   * @param records List of data records to filter.
   * @return List of all records matching the query (or empty list if none
   *         match), null if the data set could not be filtered.
   */
  public static <T extends BaseRecord> List<T> filterMultiple(
      final Query<T> query, final Iterable<T> records) {

    List<T> matchingList = new ArrayList<>();
    for (T record : records) {
      if (query.matches(record)) {
        matchingList.add(record);
      }
    }
    return matchingList;
  }
}