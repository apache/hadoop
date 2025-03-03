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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.service.AbstractService;

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
  implements StreamCapabilities {

  private EnumSet<StoreConfigurationFlags> configurationFlags;

  public StoreConfigurationService(final String name) {
    super(name);
  }

  public StoreConfigurationService() {
    super("StoreConfigurationService");
  }

  /**
   * Initialize the service by reading in configuration
   * settings.
   * @param conf configuration
   * @throws Exception parser failures.
   */
  @Override
  protected void serviceInit(final Configuration conf) throws Exception {
    super.serviceInit(conf);
    configurationFlags = EnumSet.noneOf(StoreConfigurationFlags.class);
    Arrays.stream(StoreConfigurationFlags.values())
        .filter(v -> v.evaluate(conf))
        .forEach(configurationFlags::add);
  }

  /**
   * Is a configuration flag set?
   * @param flag flag to probe for.
   * @return true iff the flag is set
   */
  public boolean isFlagSet(StoreConfigurationFlags flag) {
    return configurationFlags.contains(flag);
  }

  /**
   * Get a clone of the flags.
   * @return a copy of the flags.
   */
  public EnumSet<StoreConfigurationFlags> getConfigurationFlags() {
    return configurationFlags.clone();
  }

  /**
   * Does one of the flags have this capability?
   * @param capability what to probe for
   * @return true if the capability is implellmented.
   */
  @Override
  public boolean hasCapability(String capability) {
    return configurationFlags.stream()
        .anyMatch(f -> f.keyMatches(capability));
  }

  /**
   * Set a flag.
   * This is NOT thread safe.
   * @param flag flag to set
   * @return true if the flag enumset changed state.
   */
  public boolean setFlag(StoreConfigurationFlags flag) {
    return configurationFlags.add(flag);
  }

  /**
   * Clear a flag.
   * This is NOT thread safe.
   * @param flag flag to clear
   * @return true if the flag enumset changed state.
   */
  public boolean clearFlag(StoreConfigurationFlags flag) {
    return configurationFlags.remove(flag);
  }

}
