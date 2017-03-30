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
package org.apache.hadoop.hdfs.server.federation.store.driver;

import java.net.InetAddress;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreUnavailableException;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.util.Time;

/**
 * Driver class for an implementation of a {@link StateStoreService}
 * provider. Driver implementations will extend this class and implement some of
 * the default methods.
 */
public abstract class StateStoreDriver implements StateStoreRecordOperations {

  private static final Log LOG = LogFactory.getLog(StateStoreDriver.class);


  /** State Store configuration. */
  private Configuration conf;

  /** Identifier for the driver. */
  private String identifier;


  /**
   * Initialize the state store connection.
   * @param config Configuration for the driver.
   * @param id Identifier for the driver.
   * @param records Records that are supported.
   * @return If initialized and ready, false if failed to initialize driver.
   */
  public boolean init(final Configuration config, final String id,
      final List<Class<? extends BaseRecord>> records) {

    this.conf = config;
    this.identifier = id;

    if (this.identifier == null) {
      LOG.warn("The identifier for the State Store connection is not set");
    }

    // TODO stub
    return false;
  }

  /**
   * Get the State Store configuration.
   *
   * @return Configuration for the State Store.
   */
  protected Configuration getConf() {
    return this.conf;
  }

  /**
   * Gets a unique identifier for the running task/process. Typically the
   * router address.
   *
   * @return Unique identifier for the running task.
   */
  public String getIdentifier() {
    return this.identifier;
  }

  /**
   * Prepare the driver to access data storage.
   *
   * @return True if the driver was successfully initialized. If false is
   *         returned, the state store will periodically attempt to
   *         re-initialize the driver and the router will remain in safe mode
   *         until the driver is initialized.
   */
  public abstract boolean initDriver();

  /**
   * Initialize storage for a single record class.
   *
   * @param name String reference of the record class to initialize, used to
   *             construct paths and file names for the record. Determined by
   *             configuration settings for the specific driver.
   * @param clazz Record type corresponding to the provided name.
   * @return True if successful, false otherwise.
   */
  public abstract <T extends BaseRecord> boolean initRecordStorage(
      String className, Class<T> clazz);

  /**
   * Check if the driver is currently running and the data store connection is
   * valid.
   *
   * @return True if the driver is initialized and the data store is ready.
   */
  public abstract boolean isDriverReady();

  /**
   * Check if the driver is ready to be used and throw an exception otherwise.
   *
   * @throws StateStoreUnavailableException If the driver is not ready.
   */
  public void verifyDriverReady() throws StateStoreUnavailableException {
    if (!isDriverReady()) {
      String driverName = getDriverName();
      String hostname = getHostname();
      throw new StateStoreUnavailableException("State Store driver " +
          driverName + " in " + hostname + " is not ready.");
    }
  }

  /**
   * Close the State Store driver connection.
   */
  public abstract void close() throws Exception;

  /**
   * Returns the current time synchronization from the underlying store.
   * Override for stores that supply a current date. The data store driver is
   * responsible for maintaining the official synchronization time/date for all
   * distributed components.
   *
   * @return Current time stamp, used for all synchronization dates.
   */
  public long getTime() {
    return Time.now();
  }

  /**
   * Get the name of the driver implementation for debugging.
   *
   * @return Name of the driver implementation.
   */
  private String getDriverName() {
    return this.getClass().getSimpleName();
  }

  /**
   * Get the host name of the machine running the driver for debugging.
   *
   * @return Host name of the machine running the driver.
   */
  private String getHostname() {
    String hostname = "Unknown";
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      LOG.error("Cannot get local address", e);
    }
    return hostname;
  }
}
