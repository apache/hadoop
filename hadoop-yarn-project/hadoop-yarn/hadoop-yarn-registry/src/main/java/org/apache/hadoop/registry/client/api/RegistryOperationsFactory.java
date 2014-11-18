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

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.registry.client.impl.RegistryOperationsClient;

import static org.apache.hadoop.registry.client.api.RegistryConstants.*;

/**
 * A factory for registry operation service instances.
 * <p>
 * <i>Each created instance will be returned initialized.</i>
 * <p>
 * That is, the service will have had <code>Service.init(conf)</code> applied
 * to it —possibly after the configuration has been modified to
 * support the specific binding/security mechanism used
 */
public final class RegistryOperationsFactory {

  private RegistryOperationsFactory() {
  }

  /**
   * Create and initialize a registry operations instance.
   * Access writes will be determined from the configuration
   * @param conf configuration
   * @return a registry operations instance
   * @throws ServiceStateException on any failure to initialize
   */
  public static RegistryOperations createInstance(Configuration conf) {
    return createInstance("RegistryOperations", conf);
  }

  /**
   * Create and initialize a registry operations instance.
   * Access rights will be determined from the configuration
   * @param name name of the instance
   * @param conf configuration
   * @return a registry operations instance
   * @throws ServiceStateException on any failure to initialize
   */
  public static RegistryOperations createInstance(String name, Configuration conf) {
    Preconditions.checkArgument(conf != null, "Null configuration");
    RegistryOperationsClient operations =
        new RegistryOperationsClient(name);
    operations.init(conf);
    return operations;
  }

  /**
   * Create and initialize an anonymous read/write registry operations instance.
   * In a secure cluster, this instance will only have read access to the
   * registry.
   * @param conf configuration
   * @return an anonymous registry operations instance
   *
   * @throws ServiceStateException on any failure to initialize
   */
  public static RegistryOperations createAnonymousInstance(Configuration conf) {
    Preconditions.checkArgument(conf != null, "Null configuration");
    conf.set(KEY_REGISTRY_CLIENT_AUTH, REGISTRY_CLIENT_AUTH_ANONYMOUS);
    return createInstance("AnonymousRegistryOperations", conf);
  }

  /**
   * Create and initialize an secure, Kerberos-authenticated instance.
   *
   * The user identity will be inferred from the current user
   *
   * The authentication of this instance will expire when any kerberos
   * tokens needed to authenticate with the registry infrastructure expire.
   * @param conf configuration
   * @param jaasContext the JAAS context of the account.
   * @return a registry operations instance
   * @throws ServiceStateException on any failure to initialize
   */
  public static RegistryOperations createKerberosInstance(Configuration conf,
      String jaasContext) {
    Preconditions.checkArgument(conf != null, "Null configuration");
    conf.set(KEY_REGISTRY_CLIENT_AUTH, REGISTRY_CLIENT_AUTH_KERBEROS);
    conf.set(KEY_REGISTRY_CLIENT_JAAS_CONTEXT, jaasContext);
    return createInstance("KerberosRegistryOperations", conf);
  }

  /**
   * Create and initialize an operations instance authenticated with write
   * access via an <code>id:password</code> pair.
   *
   * The instance will have the read access
   * across the registry, but write access only to that part of the registry
   * to which it has been give the relevant permissions.
   * @param conf configuration
   * @param id user ID
   * @param password password
   * @return a registry operations instance
   * @throws ServiceStateException on any failure to initialize
   * @throws IllegalArgumentException if an argument is invalid
   */
  public static RegistryOperations createAuthenticatedInstance(Configuration conf,
      String id,
      String password) {
    Preconditions.checkArgument(!StringUtils.isEmpty(id), "empty Id");
    Preconditions.checkArgument(!StringUtils.isEmpty(password), "empty Password");
    Preconditions.checkArgument(conf != null, "Null configuration");
    conf.set(KEY_REGISTRY_CLIENT_AUTH, REGISTRY_CLIENT_AUTH_DIGEST);
    conf.set(KEY_REGISTRY_CLIENT_AUTHENTICATION_ID, id);
    conf.set(KEY_REGISTRY_CLIENT_AUTHENTICATION_PASSWORD, password);
    return createInstance("DigestRegistryOperations", conf);
  }

}
