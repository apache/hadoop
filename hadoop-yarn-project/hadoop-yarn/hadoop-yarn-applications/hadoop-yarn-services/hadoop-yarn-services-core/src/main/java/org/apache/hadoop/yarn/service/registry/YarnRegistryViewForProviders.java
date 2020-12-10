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

package org.apache.hadoop.yarn.service.registry;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;

import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceId;
import org.apache.hadoop.yarn.service.utils.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.registry.client.binding.RegistryPathUtils.join;

/**
 * Registry view for providers. This tracks where the service
 * is registered, offers access to the record and other things.
 */
public class YarnRegistryViewForProviders {
  private static final Logger LOG =
      LoggerFactory.getLogger(YarnRegistryViewForProviders.class);

  private final RegistryOperations registryOperations;
  private final String user;
  private final String serviceClass;
  private final String instanceName;
  /**
   * Record used where the service registered itself.
   * Null until the service is registered
   */
  private ServiceRecord selfRegistration;

  /**
   * Path where record was registered.
   * Null until the service is registered
   */
  private String selfRegistrationPath;

  public YarnRegistryViewForProviders(RegistryOperations registryOperations,
      String user,
      String serviceClass,
      String instanceName,
      ApplicationAttemptId applicationAttemptId) {
    Preconditions.checkArgument(registryOperations != null,
        "null registry operations");
    Preconditions.checkArgument(user != null, "null user");
    Preconditions.checkArgument(ServiceUtils.isSet(serviceClass),
        "unset service class");
    Preconditions.checkArgument(ServiceUtils.isSet(instanceName),
        "instanceName");
    Preconditions.checkArgument(applicationAttemptId != null,
        "null applicationAttemptId");
    this.registryOperations = registryOperations;
    this.user = user;
    this.serviceClass = serviceClass;
    this.instanceName = instanceName;
  }

  public String getUser() {
    return user;
  }


  private void setSelfRegistration(ServiceRecord selfRegistration) {
    this.selfRegistration = selfRegistration;
  }

  /**
   * Get the path to where the service has registered itself.
   * Null until the service is registered
   * @return the service registration path.
   */
  public String getSelfRegistrationPath() {
    return selfRegistrationPath;
  }

  /**
   * Get the absolute path to where the service has registered itself.
   * This includes the base registry path
   * Null until the service is registered
   * @return the service registration path.
   */
  public String getAbsoluteSelfRegistrationPath() {
    if (selfRegistrationPath == null) {
      return null;
    }
    String root = registryOperations.getConfig().getTrimmed(
        RegistryConstants.KEY_REGISTRY_ZK_ROOT,
        RegistryConstants.DEFAULT_ZK_REGISTRY_ROOT);
    return RegistryPathUtils.join(root, selfRegistrationPath);
  }

  /**
   * Add a component under the slider name/entry.
   * @param componentName component name
   * @param record record to put
   * @throws IOException
   */
  public void putComponent(String componentName,
      ServiceRecord record) throws
      IOException {
    putComponent(serviceClass, instanceName,
        componentName,
        record);
  }

  /**
   * Add a component.
   * @param serviceClass service class to use under ~user
   * @param componentName component name
   * @param record record to put
   * @throws IOException
   */
  public void putComponent(String serviceClass,
      String serviceName,
      String componentName,
      ServiceRecord record) throws IOException {
    String path = RegistryUtils.componentPath(
        user, serviceClass, serviceName, componentName);
    registryOperations.mknode(RegistryPathUtils.parentOf(path), true);
    registryOperations.bind(path, record, BindFlags.OVERWRITE);
  }

  /**
   * Get a component.
   * @param componentName component name
   * @return the service record
   * @throws IOException
   */
  public ServiceRecord getComponent(String componentName) throws IOException {
    String path = RegistryUtils.componentPath(
        user, serviceClass, instanceName, componentName);
    LOG.info("Resolving path {}", path);
    return registryOperations.resolve(path);
  }

  /**
   * List components.
   * @return a list of components
   * @throws IOException
   */
  public List<String> listComponents() throws IOException {
    String path = RegistryUtils.componentListPath(
        user, serviceClass, instanceName);
    return registryOperations.list(path);
  }

  /**
   * Add a service under a path, optionally purging any history.
   * @param username user
   * @param serviceClass service class to use under ~user
   * @param serviceName name of the service
   * @param record service record
   * @param deleteTreeFirst perform recursive delete of the path first.
   * @return the path the service was created at
   * @throws IOException
   */
  public String putService(String username,
      String serviceClass,
      String serviceName,
      ServiceRecord record,
      boolean deleteTreeFirst) throws IOException {
    String path = RegistryUtils.servicePath(
        username, serviceClass, serviceName);
    if (deleteTreeFirst) {
      registryOperations.delete(path, true);
    }
    registryOperations.mknode(RegistryPathUtils.parentOf(path), true);
    registryOperations.bind(path, record, BindFlags.OVERWRITE);
    return path;
  }

  /**
   * Add a service under a path for the current user.
   * @param record service record
   * @param deleteTreeFirst perform recursive delete of the path first
   * @return the path the service was created at
   * @throws IOException
   */
  public String registerSelf(
      ServiceRecord record,
      boolean deleteTreeFirst) throws IOException {
    selfRegistrationPath =
        putService(user, serviceClass, instanceName, record, deleteTreeFirst);
    setSelfRegistration(record);
    return selfRegistrationPath;
  }

  /**
   * Delete a component.
   * @param containerId component name
   * @throws IOException
   */
  public void deleteComponent(ComponentInstanceId instanceId,
      String containerId) throws IOException {
    String path = RegistryUtils.componentPath(
        user, serviceClass, instanceName,
        containerId);
    LOG.info(instanceId + ": Deleting registry path " + path);
    registryOperations.delete(path, false);
  }

  /**
   * Delete the children of a path -but not the path itself.
   * It is not an error if the path does not exist
   * @param path path to delete
   * @param recursive flag to request recursive deletes
   * @throws IOException IO problems
   */
  public void deleteChildren(String path, boolean recursive) throws IOException {
    List<String> childNames = null;
    try {
      childNames = registryOperations.list(path);
    } catch (PathNotFoundException e) {
      return;
    }
    for (String childName : childNames) {
      String child = join(path, childName);
      registryOperations.delete(child, recursive);
    }
  }
  
}
