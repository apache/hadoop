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

import java.lang.reflect.Constructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;

/**
 * Store records in the State Store. Subclasses provide interfaces to operate on
 * those records.
 *
 * @param <R> Record to store by this interface.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class RecordStore<R extends BaseRecord> {

  private static final Log LOG = LogFactory.getLog(RecordStore.class);


  /** Class of the record stored in this State Store. */
  private final Class<R> recordClass;

  /** State store driver backed by persistent storage. */
  private final StateStoreDriver driver;


  /**
   * Create a new store for records.
   *
   * @param clazz Class of the record to store.
   * @param stateStoreDriver Driver for the State Store.
   */
  protected RecordStore(Class<R> clazz, StateStoreDriver stateStoreDriver) {
    this.recordClass = clazz;
    this.driver = stateStoreDriver;
  }

  /**
   * Report a required record to the data store. The data store uses this to
   * create/maintain storage for the record.
   *
   * @return The class of the required record or null if no record is required
   *         for this interface.
   */
  public Class<R> getRecordClass() {
    return this.recordClass;
  }

  /**
   * Get the State Store driver.
   *
   * @return State Store driver.
   */
  protected StateStoreDriver getDriver() {
    return this.driver;
  }

  /**
   * Build a state store API implementation interface.
   *
   * @param clazz The specific interface implementation to create
   * @param driver The {@link StateStoreDriver} implementation in use.
   * @return An initialized instance of the specified state store API
   *         implementation.
   */
  public static <T extends RecordStore<?>> T newInstance(
      final Class<T> clazz, final StateStoreDriver driver) {

    try {
      Constructor<T> constructor = clazz.getConstructor(StateStoreDriver.class);
      T recordStore = constructor.newInstance(driver);
      return recordStore;
    } catch (Exception e) {
      LOG.error("Cannot create new instance for " + clazz, e);
      return null;
    }
  }
}
