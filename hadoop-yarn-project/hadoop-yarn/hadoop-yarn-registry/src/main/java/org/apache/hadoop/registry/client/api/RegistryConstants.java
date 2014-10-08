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

package org.apache.hadoop.registry.client.api;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Constants for the registry, including configuration keys and default
 * values.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface RegistryConstants {

  /**
   * prefix for registry configuration options: {@value}.
   * Why <code>hadoop.</code> and not YARN? It can
   * live outside YARN
   */
  String REGISTRY_PREFIX = "hadoop.registry.";

  /**
   * Prefix for zookeeper-specific options: {@value}
   *  <p>
   * For clients using other protocols, these options are not supported.
   */
  String ZK_PREFIX = REGISTRY_PREFIX + "zk.";

  /**
   * flag to indicate whether or not the registry should
   * be enabled in the RM: {@value}
   */
  String KEY_REGISTRY_ENABLED = REGISTRY_PREFIX + "rm.enabled";

  /**
   * Defaut value for enabling the registry in the RM: {@value}
   */
  boolean DEFAULT_REGISTRY_ENABLED = false;

  /**
   * Key to set if the registry is secure: {@value}.
   * Turning it on changes the permissions policy from "open access"
   * to restrictions on kerberos with the option of
   * a user adding one or more auth key pairs down their
   * own tree.
   */
  String KEY_REGISTRY_SECURE = REGISTRY_PREFIX + "secure";

  /**
   * Default registry security policy: {@value}.
   */
  boolean DEFAULT_REGISTRY_SECURE = false;

  /**
   * Root path in the ZK tree for the registry: {@value}
   */
  String KEY_REGISTRY_ZK_ROOT = ZK_PREFIX + "root";

  /**
   * Default root of the yarn registry: {@value}
   */
  String DEFAULT_ZK_REGISTRY_ROOT = "/registry";

  /**
   * Registry client authentication policy.
   *  <p>
   * This is only used in secure clusters.
   *  <p>
   * If the Factory methods of {@link RegistryOperationsFactory}
   * are used, this key does not need to be set: it is set
   * up based on the factory method used.
   */
  String KEY_REGISTRY_CLIENT_AUTH =
      REGISTRY_PREFIX + "client.auth";

  /**
   * Registry client uses Kerberos: authentication is automatic from
   * logged in user
   */
  String REGISTRY_CLIENT_AUTH_KERBEROS = "kerberos";

  /**
   * Username/password is the authentication mechanism.
   * If set then both {@link #KEY_REGISTRY_CLIENT_AUTHENTICATION_ID}
   * and {@link #KEY_REGISTRY_CLIENT_AUTHENTICATION_PASSWORD} must be set.
   */
  String REGISTRY_CLIENT_AUTH_DIGEST = "digest";

  /**
   * No authentication; client is anonymous
   */
  String REGISTRY_CLIENT_AUTH_ANONYMOUS = "";

  /**
   * Registry client authentication ID
   * <p>
   * This is only used in secure clusters with
   * {@link #KEY_REGISTRY_CLIENT_AUTH} set to
   * {@link #REGISTRY_CLIENT_AUTH_DIGEST}
   *
   */
  String KEY_REGISTRY_CLIENT_AUTHENTICATION_ID =
      KEY_REGISTRY_CLIENT_AUTH + ".id";

  /**
   * Registry client authentication password.
   * <p>
   * This is only used in secure clusters with the client set to
   * use digest (not SASL or anonymouse) authentication.
   *  <p>
   * Specifically, {@link #KEY_REGISTRY_CLIENT_AUTH} set to
   * {@link #REGISTRY_CLIENT_AUTH_DIGEST}
   *
   */
  String KEY_REGISTRY_CLIENT_AUTHENTICATION_PASSWORD =
      KEY_REGISTRY_CLIENT_AUTH + ".password";

  /**
   * List of hostname:port pairs defining the
   * zookeeper quorum binding for the registry {@value}
   */
  String KEY_REGISTRY_ZK_QUORUM = ZK_PREFIX + "quorum";

  /**
   * The default zookeeper quorum binding for the registry: {@value}
   */
  String DEFAULT_REGISTRY_ZK_QUORUM = "localhost:2181";

  /**
   * Zookeeper session timeout in milliseconds: {@value}
   */
  String KEY_REGISTRY_ZK_SESSION_TIMEOUT =
      ZK_PREFIX + "session.timeout.ms";

  /**
  * The default ZK session timeout: {@value}.
  */
  int DEFAULT_ZK_SESSION_TIMEOUT = 60000;

  /**
   * Zookeeper connection timeout in milliseconds: {@value}.
   */
  String KEY_REGISTRY_ZK_CONNECTION_TIMEOUT =
      ZK_PREFIX + "connection.timeout.ms";

  /**
   * The default ZK connection timeout: {@value}.
   */
  int DEFAULT_ZK_CONNECTION_TIMEOUT = 15000;

  /**
   * Zookeeper connection retry count before failing: {@value}.
   */
  String KEY_REGISTRY_ZK_RETRY_TIMES = ZK_PREFIX + "retry.times";

  /**
   * The default # of times to retry a ZK connection: {@value}.
   */
  int DEFAULT_ZK_RETRY_TIMES = 5;

  /**
   * Zookeeper connect interval in milliseconds: {@value}.
   */
  String KEY_REGISTRY_ZK_RETRY_INTERVAL =
      ZK_PREFIX + "retry.interval.ms";

  /**
   * The default interval between connection retries: {@value}.
   */
  int DEFAULT_ZK_RETRY_INTERVAL = 1000;

  /**
   * Zookeeper retry limit in milliseconds, during
   * exponential backoff: {@value}.
   *
   * This places a limit even
   * if the retry times and interval limit, combined
   * with the backoff policy, result in a long retry
   * period
   *
   */
  String KEY_REGISTRY_ZK_RETRY_CEILING =
      ZK_PREFIX + "retry.ceiling.ms";

  /**
   * Default limit on retries: {@value}.
   */
  int DEFAULT_ZK_RETRY_CEILING = 60000;

  /**
   * A comma separated list of Zookeeper ACL identifiers with
   * system access to the registry in a secure cluster: {@value}.
   *
   * These are given full access to all entries.
   *
   * If there is an "@" at the end of an entry it
   * instructs the registry client to append the kerberos realm as
   * derived from the login and {@link #KEY_REGISTRY_KERBEROS_REALM}.
   */
  String KEY_REGISTRY_SYSTEM_ACCOUNTS = REGISTRY_PREFIX + "system.accounts";

  /**
   * Default system accounts given global access to the registry: {@value}.
   */
  String DEFAULT_REGISTRY_SYSTEM_ACCOUNTS =
      "sasl:yarn@, sasl:mapred@, sasl:hdfs@, sasl:hadoop@";

  /**
   * A comma separated list of Zookeeper ACL identifiers with
   * system access to the registry in a secure cluster: {@value}.
   *
   * These are given full access to all entries.
   *
   * If there is an "@" at the end of an entry it
   * instructs the registry client to append the default kerberos domain.
   */
  String KEY_REGISTRY_USER_ACCOUNTS = REGISTRY_PREFIX + "user.accounts";

  /**
   * Default system acls: {@value}.
   */
  String DEFAULT_REGISTRY_USER_ACCOUNTS = "";

  /**
   * The kerberos realm: {@value}.
   *
   * This is used to set the realm of
   * system principals which do not declare their realm,
   * and any other accounts that need the value.
   *
   * If empty, the default realm of the running process
   * is used.
   *
   * If neither are known and the realm is needed, then the registry
   * service/client will fail.
   */
  String KEY_REGISTRY_KERBEROS_REALM = REGISTRY_PREFIX + "kerberos.realm";

  /**
   * Key to define the JAAS context. Used in secure registries: {@value}.
   */
  String KEY_REGISTRY_CLIENT_JAAS_CONTEXT = REGISTRY_PREFIX + "jaas.context";

  /**
   * default client-side registry JAAS context: {@value}
   */
  String DEFAULT_REGISTRY_CLIENT_JAAS_CONTEXT = "Client";

  /**
   *  path to users off the root: {@value}.
   */
  String PATH_USERS = "/users/";

  /**
   *  path to system services off the root : {@value}.
   */
  String PATH_SYSTEM_SERVICES = "/services/";

  /**
   *  path to system services under a user's home path : {@value}.
   */
  String PATH_USER_SERVICES = "/services/";

  /**
   *  path under a service record to point to components of that service:
   *  {@value}.
   */
  String SUBPATH_COMPONENTS = "/components/";
}
