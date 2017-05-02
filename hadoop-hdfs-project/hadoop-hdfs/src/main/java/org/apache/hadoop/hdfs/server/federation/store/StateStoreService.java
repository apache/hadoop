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
import java.util.Collection;
import java.util.LinkedList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * A service to initialize a
 * {@link org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver
 * StateStoreDriver} and maintain the connection to the data store. There are
 * multiple state store driver connections supported:
 * <ul>
 * <li>File
 * {@link org.apache.hadoop.hdfs.server.federation.store.driver.impl.
 * StateStoreFileImpl StateStoreFileImpl}
 * <li>ZooKeeper
 * {@link org.apache.hadoop.hdfs.server.federation.store.driver.impl.
 * StateStoreZooKeeperImpl StateStoreZooKeeperImpl}
 * </ul>
 * <p>
 * The service also supports the dynamic registration of record stores like:
 * <ul>
 * <li>{@link MembershipStore}: state of the Namenodes in the
 * federation.
 * <li>{@link MountTableStore}: Mount table between to subclusters.
 * See {@link org.apache.hadoop.fs.viewfs.ViewFs ViewFs}.
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class StateStoreService extends CompositeService {

  private static final Logger LOG =
      LoggerFactory.getLogger(StateStoreService.class);


  /** State Store configuration. */
  private Configuration conf;

  /** Identifier for the service. */
  private String identifier;

  /** Driver for the back end connection. */
  private StateStoreDriver driver;

  /** Service to maintain data store connection. */
  private StateStoreConnectionMonitorService monitorService;


  public StateStoreService() {
    super(StateStoreService.class.getName());
  }

  /**
   * Initialize the State Store and the connection to the backend.
   *
   * @param config Configuration for the State Store.
   * @throws IOException
   */
  @Override
  protected void serviceInit(Configuration config) throws Exception {
    this.conf = config;

    // Create implementation of State Store
    Class<? extends StateStoreDriver> driverClass = this.conf.getClass(
        DFSConfigKeys.FEDERATION_STORE_DRIVER_CLASS,
        DFSConfigKeys.FEDERATION_STORE_DRIVER_CLASS_DEFAULT,
        StateStoreDriver.class);
    this.driver = ReflectionUtils.newInstance(driverClass, this.conf);

    if (this.driver == null) {
      throw new IOException("Cannot create driver for the State Store");
    }

    // Check the connection to the State Store periodically
    this.monitorService = new StateStoreConnectionMonitorService(this);
    this.addService(monitorService);

    super.serviceInit(this.conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    loadDriver();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    closeDriver();

    super.serviceStop();
  }

  /**
   * List of records supported by this State Store.
   *
   * @return List of supported record classes.
   */
  public Collection<Class<? extends BaseRecord>> getSupportedRecords() {
    // TODO add list of records
    return new LinkedList<>();
  }

  /**
   * Load the State Store driver. If successful, refresh cached data tables.
   */
  public void loadDriver() {
    synchronized (this.driver) {
      if (!isDriverReady()) {
        String driverName = this.driver.getClass().getSimpleName();
        if (this.driver.init(conf, getIdentifier(), getSupportedRecords())) {
          LOG.info("Connection to the State Store driver {} is open and ready",
              driverName);
        } else {
          LOG.error("Cannot initialize State Store driver {}", driverName);
        }
      }
    }
  }

  /**
   * Check if the driver is ready to be used.
   *
   * @return If the driver is ready.
   */
  public boolean isDriverReady() {
    return this.driver.isDriverReady();
  }

  /**
   * Manually shuts down the driver.
   *
   * @throws Exception If the driver cannot be closed.
   */
  @VisibleForTesting
  public void closeDriver() throws Exception {
    if (this.driver != null) {
      this.driver.close();
    }
  }

  /**
   * Get the state store driver.
   *
   * @return State store driver.
   */
  public StateStoreDriver getDriver() {
    return this.driver;
  }

  /**
   * Fetch a unique identifier for this state store instance. Typically it is
   * the address of the router.
   *
   * @return Unique identifier for this store.
   */
  public String getIdentifier() {
    return this.identifier;
  }

  /**
   * Set a unique synchronization identifier for this store.
   *
   * @param id Unique identifier, typically the router's RPC address.
   */
  public void setIdentifier(String id) {
    this.identifier = id;
  }

}