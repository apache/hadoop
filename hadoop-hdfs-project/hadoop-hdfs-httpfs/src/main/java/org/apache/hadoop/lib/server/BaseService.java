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

package org.apache.hadoop.lib.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.lib.util.ConfigurationUtils;

import java.util.Map;

/**
 * Convenience class implementing the {@link Service} interface.
 */
@InterfaceAudience.Private
public abstract class BaseService implements Service {
  private String prefix;
  private Server server;
  private Configuration serviceConfig;

  /**
   * Service constructor.
   *
   * @param prefix service prefix.
   */
  public BaseService(String prefix) {
    this.prefix = prefix;
  }

  /**
   * Initializes the service.
   * <p/>
   * It collects all service properties (properties having the
   * <code>#SERVER#.#SERVICE#.</code> prefix). The property names are then
   * trimmed from the <code>#SERVER#.#SERVICE#.</code> prefix.
   * <p/>
   * After collecting  the service properties it delegates to the
   * {@link #init()} method.
   *
   * @param server the server initializing the service, give access to the
   * server context.
   *
   * @throws ServiceException thrown if the service could not be initialized.
   */
  @Override
  public final void init(Server server) throws ServiceException {
    this.server = server;
    String servicePrefix = getPrefixedName("");
    serviceConfig = new Configuration(false);
    for (Map.Entry<String, String> entry : ConfigurationUtils.resolve(server.getConfig())) {
      String key = entry.getKey();
      if (key.startsWith(servicePrefix)) {
        serviceConfig.set(key.substring(servicePrefix.length()), entry.getValue());
      }
    }
    init();
  }


  /**
   * Post initializes the service. This method is called by the
   * {@link Server} after all services of the server have been initialized.
   * <p/>
   * This method does a NOP.
   *
   * @throws ServiceException thrown if the service could not be
   * post-initialized.
   */
  @Override
  public void postInit() throws ServiceException {
  }

  /**
   * Destroy the services.  This method is called once, when the
   * {@link Server} owning the service is being destroyed.
   * <p/>
   * This method does a NOP.
   */
  @Override
  public void destroy() {
  }

  /**
   * Returns the service dependencies of this service. The service will be
   * instantiated only if all the service dependencies are already initialized.
   * <p/>
   * This method returns an empty array (size 0)
   *
   * @return an empty array (size 0).
   */
  @Override
  public Class[] getServiceDependencies() {
    return new Class[0];
  }

  /**
   * Notification callback when the server changes its status.
   * <p/>
   * This method returns an empty array (size 0)
   *
   * @param oldStatus old server status.
   * @param newStatus new server status.
   *
   * @throws ServiceException thrown if the service could not process the status change.
   */
  @Override
  public void serverStatusChange(Server.Status oldStatus, Server.Status newStatus) throws ServiceException {
  }

  /**
   * Returns the service prefix.
   *
   * @return the service prefix.
   */
  protected String getPrefix() {
    return prefix;
  }

  /**
   * Returns the server owning the service.
   *
   * @return the server owning the service.
   */
  protected Server getServer() {
    return server;
  }

  /**
   * Returns the full prefixed name of a service property.
   *
   * @param name of the property.
   *
   * @return prefixed name of the property.
   */
  protected String getPrefixedName(String name) {
    return server.getPrefixedName(prefix + "." + name);
  }

  /**
   * Returns the service configuration properties. Property
   * names are trimmed off from its prefix.
   * <p/>
   * The sevice configuration properties are all properties
   * with names starting with <code>#SERVER#.#SERVICE#.</code>
   * in the server configuration.
   *
   * @return the service configuration properties with names
   *         trimmed off from their <code>#SERVER#.#SERVICE#.</code>
   *         prefix.
   */
  protected Configuration getServiceConfig() {
    return serviceConfig;
  }

  /**
   * Initializes the server.
   * <p/>
   * This method is called by {@link #init(Server)} after all service properties
   * (properties prefixed with
   *
   * @throws ServiceException thrown if the service could not be initialized.
   */
  protected abstract void init() throws ServiceException;

}
