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

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.api.RegistryOperations;

import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.exceptions.InvalidPathnameException;
import org.apache.hadoop.registry.client.exceptions.NoRecordException;
import org.apache.hadoop.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * The Registry operations service.
 * <p>
 * This service implements the {@link RegistryOperations}
 * API by mapping the commands to zookeeper operations, and translating
 * results and exceptions back into those specified by the API.
 * <p>
 * Factory methods should hide the detail that this has been implemented via
 * the {@link CuratorService} by returning it cast to that
 * {@link RegistryOperations} interface, rather than this implementation class.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RegistryOperationsService extends CuratorService
  implements RegistryOperations {

  private static final Logger LOG =
      LoggerFactory.getLogger(RegistryOperationsService.class);

  private final RegistryUtils.ServiceRecordMarshal serviceRecordMarshal
      = new RegistryUtils.ServiceRecordMarshal();

  public RegistryOperationsService(String name) {
    this(name, null);
  }

  public RegistryOperationsService() {
    this("RegistryOperationsService");
  }

  public RegistryOperationsService(String name,
      RegistryBindingSource bindingSource) {
    super(name, bindingSource);
  }

  /**
   * Get the aggregate set of ACLs the client should use
   * to create directories
   * @return the ACL list
   */
  public List<ACL> getClientAcls() {
    return getRegistrySecurity().getClientACLs();
  }

  /**
   * Validate a path
   * @param path path to validate
   * @throws InvalidPathnameException if a path is considered invalid
   */
  protected void validatePath(String path) throws InvalidPathnameException {
    // currently no checks are performed
  }

  @Override
  public boolean mknode(String path, boolean createParents) throws IOException {
    validatePath(path);
    return zkMkPath(path, CreateMode.PERSISTENT, createParents, getClientAcls());
  }

  @Override
  public void bind(String path,
      ServiceRecord record,
      int flags) throws IOException {
    Preconditions.checkArgument(record != null, "null record");
    validatePath(path);
    // validate the record before putting it
    RegistryTypeUtils.validateServiceRecord(path, record);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Bound at {} : ServiceRecord = {}", path, record);
    }
    CreateMode mode = CreateMode.PERSISTENT;
    byte[] bytes = serviceRecordMarshal.toBytes(record);
    zkSet(path, mode, bytes, getClientAcls(),
        ((flags & BindFlags.OVERWRITE) != 0));
  }

  @Override
  public ServiceRecord resolve(String path) throws IOException {
    byte[] bytes = zkRead(path);

    ServiceRecord record = serviceRecordMarshal.fromBytes(path,
        bytes, ServiceRecord.RECORD_TYPE);
    RegistryTypeUtils.validateServiceRecord(path, record);
    return record;
  }

  @Override
  public boolean exists(String path) throws IOException {
    validatePath(path);
    return zkPathExists(path);
  }

  @Override
  public RegistryPathStatus stat(String path) throws IOException {
    validatePath(path);
    Stat stat = zkStat(path);

    String name = RegistryPathUtils.lastPathEntry(path);
    RegistryPathStatus status = new RegistryPathStatus(
        name,
        stat.getCtime(),
        stat.getDataLength(),
        stat.getNumChildren());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stat {} => {}", path, status);
    }
    return status;
  }

  @Override
  public List<String> list(String path) throws IOException {
    validatePath(path);
    return zkList(path);
  }

  @Override
  public void delete(String path, boolean recursive) throws IOException {
    validatePath(path);
    zkDelete(path, recursive, null);
  }

}
