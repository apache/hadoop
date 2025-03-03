/*
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

package org.apache.hadoop.fs.s3a.impl.store;

import java.util.Arrays;
import java.util.EnumSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.store.LogExactlyOnce;
import org.apache.hadoop.service.AbstractService;

import static org.apache.hadoop.fs.s3a.impl.store.StoreConfigurationFlags.*;

/**
 * A service which handles store configurations.
 * New configuration options should be added here.
 * <p>
 *   The goal is to pull configuration flags and variables
 *   out of S3AFileSystem but not reimplement the
 *   same structure in S3AStore.
 *   Instead, configuration flags, numbers etc can
 *   be managed here.
 *   Maybe in future reflection could be used to
 *   build up the config, as done in ABFS.
 * <p>
 * Usage.
 * <ol>
 *   <li>Instantiate.</li>
 *   <li>Call {@link #init(Configuration)} to trigger config reading</lib>
 *   <li>Read loaded options.</li>
 * </ol>
 * The start and close operations are (currently) no-ops.
 */
public class StoreConfigurationService extends AbstractService
  implements StoreConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(StoreConfigurationService.class);

  private static final LogExactlyOnce LOG_CREATE_DOWNGRADE = new LogExactlyOnce(LOG);

  /** Store configuration flags. */
  private final EnumSet<StoreConfigurationFlags> storeFlags =
      EnumSet.noneOf(StoreConfigurationFlags.class);;


  public StoreConfigurationService(final String name) {
    super(name);
  }

  public StoreConfigurationService() {
    this("StoreConfigurationService");
  }

  /**
   * Initialize the service by reading in configuration settings.
   * @param conf configuration
   * @throws Exception parser failures.
   */
  @Override
  protected void serviceInit(final Configuration conf) throws Exception {
    super.serviceInit(conf);
    // build up the store flag enumset.
    storeFlags.clear();
    Arrays.stream(StoreConfigurationFlags.values())
        .filter(v -> v.evaluate(conf))
        .forEach(storeFlags::add);

    // tune some flags based on the state of others
    if (!isFlagSet(ConditionalCreateAvailable) && isFlagSet(ConditionalCreateForFiles)) {
      // only use the conditional create for files option if conditional
      // create is actually available.
      LOG_CREATE_DOWNGRADE.debug("Ignoring ConditionalCreateForFiles option");
      clearFlag(ConditionalCreateForFiles);
    }
  }

  @Override
  public boolean isFlagSet(StoreConfigurationFlags flag) {
    return storeFlags.contains(flag);
  }

  @Override
  public EnumSet<StoreConfigurationFlags> getStoreFlags() {
    return storeFlags.clone();
  }

  @Override
  public boolean hasPathCapability(final Path path, final String capability) {

    // check the configuration flags.
    if (storeFlags.stream()
        .anyMatch(f -> f.keyMatches(capability))) {
      return true;
    }

    // no match
    return false;
  }

  @Override
  public boolean setFlag(StoreConfigurationFlags flag) {
    return storeFlags.add(flag);
  }

  @Override
  public boolean clearFlag(StoreConfigurationFlags flag) {
    return storeFlags.remove(flag);
  }

}
