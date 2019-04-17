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

package org.apache.hadoop.registry.client.impl.zk;

import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.server.ZooKeeperSaslServer;

/**
 * Configuration options which are internal to Zookeeper,
 * as well as some other ZK constants
 * <p>
 * Zookeeper options are passed via system properties prior to the ZK
 * Methods/classes being invoked. This implies that:
 * <ol>
 *   <li>There can only be one instance of a ZK client or service class
 *   in a single JVM —else their configuration options will conflict.</li>
 *   <li>It is safest to set these properties immediately before
 *   invoking ZK operations.</li>
 * </ol>
 *
 */
public interface ZookeeperConfigOptions {
  /**
   * Enable SASL secure clients: {@value}.
   * This is usually set to true, with ZK set to fall back to
   * non-SASL authentication if the SASL auth fails
   * by the property
   * {@link #PROP_ZK_SERVER_MAINTAIN_CONNECTION_DESPITE_SASL_FAILURE}.
   * <p>
   * As a result, clients will default to attempting SASL-authentication,
   * but revert to classic authentication/anonymous access on failure.
   */
  String PROP_ZK_ENABLE_SASL_CLIENT =
      "zookeeper.sasl.client";

  /**
   * Default flag for the ZK client: {@value}.
   */
  String DEFAULT_ZK_ENABLE_SASL_CLIENT = "true";

  /**
   * System property for the JAAS client context : {@value}.
   *
   * For SASL authentication to work, this must point to a
   * context within the
   *
   * <p>
   *   Default value is derived from
   *   {@link ZooKeeperSaslClient#LOGIN_CONTEXT_NAME_KEY}
   */
  String PROP_ZK_SASL_CLIENT_CONTEXT =
      ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY;

  /**
   * The SASL client username: {@value}.
   * <p>
   * Set this to the <i>short</i> name of the client, e.g, "user",
   * not {@code user/host}, or {@code user/host@REALM}
   */
  String PROP_ZK_SASL_CLIENT_USERNAME = "zookeeper.sasl.client.username";

  /**
   * The SASL Server context, referring to a context in the JVM's
   * JAAS context file: {@value}
   */
  String PROP_ZK_SERVER_SASL_CONTEXT =
      ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY;

  /**
   * Should ZK Server allow failed SASL clients to downgrade to classic
   * authentication on a SASL auth failure: {@value}.
   */
  String PROP_ZK_SERVER_MAINTAIN_CONNECTION_DESPITE_SASL_FAILURE =
      "zookeeper.maintain_connection_despite_sasl_failure";

  /**
   * should the ZK Server Allow failed SASL clients: {@value}.
   */
  String PROP_ZK_ALLOW_FAILED_SASL_CLIENTS =
      "zookeeper.allowSaslFailedClients";

  /**
   * Kerberos realm of the server: {@value}.
   */
  String PROP_ZK_SERVER_REALM = "zookeeper.server.realm";

  /**
   * Path to a kinit binary: {@value}.
   * Defaults to <code>"/usr/bin/kinit"</code>
   */
  String PROP_ZK_KINIT_PATH = "zookeeper.kinit";

  /**
   * ID scheme for SASL: {@value}.
   */
  String SCHEME_SASL = "sasl";

  /**
   * ID scheme for digest auth: {@value}.
   */
  String SCHEME_DIGEST = "digest";
}
