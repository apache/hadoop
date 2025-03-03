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

import java.util.EnumSet;

import org.apache.hadoop.fs.PathCapabilities;
import org.apache.hadoop.fs.StreamCapabilities;

public interface StoreConfiguration extends PathCapabilities {

  /**
   * Is a configuration flag set?
   * @param flag flag to probe for.
   * @return true iff the flag is set
   */
  boolean isFlagSet(StoreConfigurationFlags flag);

  /**
   * Get a clone of the flags.
   * @return a copy of the flags.
   */
  EnumSet<StoreConfigurationFlags> getStoreFlags();

  /**
   * Set a flag.
   * This is NOT thread safe.
   * @param flag flag to set
   * @return true if the flag enumset changed state.
   */
  boolean setFlag(StoreConfigurationFlags flag);

  /**
   * Clear a flag.
   * This is NOT thread safe.
   * @param flag flag to clear
   * @return true if the flag enumset changed state.
   */
  boolean clearFlag(StoreConfigurationFlags flag);
}
