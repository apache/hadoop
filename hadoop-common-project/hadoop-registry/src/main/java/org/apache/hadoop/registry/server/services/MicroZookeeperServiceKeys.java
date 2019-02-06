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

package org.apache.hadoop.registry.server.services;

import org.apache.hadoop.registry.client.api.RegistryConstants;

/**
 * Service keys for configuring the {@link MicroZookeeperService}.
 * These are not used in registry clients or the RM-side service,
 * so are kept separate.
 */
public interface MicroZookeeperServiceKeys {
  public static final String ZKSERVICE_PREFIX =
      RegistryConstants.REGISTRY_PREFIX + "zk.service.";
  /**
   * Key to define the JAAS context for the ZK service: {@value}.
   */
  public static final String KEY_REGISTRY_ZKSERVICE_JAAS_CONTEXT =
      ZKSERVICE_PREFIX + "service.jaas.context";

  /**
   * ZK servertick time: {@value}
   */
  public static final String KEY_ZKSERVICE_TICK_TIME =
      ZKSERVICE_PREFIX + "ticktime";

  /**
   * host to register on: {@value}.
   */
  public static final String KEY_ZKSERVICE_HOST = ZKSERVICE_PREFIX + "host";
  /**
   * Default host to serve on -this is <code>localhost</code> as it
   * is the only one guaranteed to be available: {@value}.
   */
  public static final String DEFAULT_ZKSERVICE_HOST = "localhost";
  /**
   * port; 0 or below means "any": {@value}
   */
  public static final String KEY_ZKSERVICE_PORT = ZKSERVICE_PREFIX + "port";

  /**
   * Directory containing data: {@value}
   */
  public static final String KEY_ZKSERVICE_DIR = ZKSERVICE_PREFIX + "dir";

  /**
   * Should failed SASL clients be allowed: {@value}?
   *
   * Default is the ZK default: true
   */
  public static final String KEY_ZKSERVICE_ALLOW_FAILED_SASL_CLIENTS =
      ZKSERVICE_PREFIX + "allow.failed.sasl.clients";
}
