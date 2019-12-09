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

package org.apache.hadoop.fs.azure;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;

/**
 * Abstract base class for the SAS Key Generator implementation
 *
 */
public abstract class SASKeyGeneratorImpl implements SASKeyGeneratorInterface {

  /**
   * Configuration key to be used to specify the expiry period for SAS keys
   * This value currently is specified in days. {@value}
   */
  public static final String KEY_SAS_KEY_EXPIRY_PERIOD =
      "fs.azure.sas.expiry.period";

  /**
   * Default value for the SAS key expiry period in days. {@value}
   */
  public static final long DEFAULT_CONTAINER_SAS_KEY_PERIOD = 90;

  private long sasKeyExpiryPeriod;

  private Configuration conf;

  public SASKeyGeneratorImpl(Configuration conf) {
    this.conf = conf;
    this.sasKeyExpiryPeriod = conf.getTimeDuration(
        KEY_SAS_KEY_EXPIRY_PERIOD, DEFAULT_CONTAINER_SAS_KEY_PERIOD,
        TimeUnit.DAYS);
  }

  public long getSasKeyExpiryPeriod() {
    return sasKeyExpiryPeriod;
  }

  public Configuration getConf() {
    return conf;
  }
}