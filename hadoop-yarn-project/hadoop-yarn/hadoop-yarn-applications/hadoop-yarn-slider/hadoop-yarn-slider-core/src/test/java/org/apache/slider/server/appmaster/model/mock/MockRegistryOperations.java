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

package org.apache.slider.server.appmaster.model.mock;

import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.service.AbstractService;

import java.util.List;

/**
 * Simple stub registry for when one is needed for its API, but the operations
 * are not actually required.
 */
class MockRegistryOperations extends AbstractService implements
    RegistryOperations {

  MockRegistryOperations() {
    super("mock");
  }

  @Override
  public boolean mknode(String path, boolean createParents) {
    return true;
  }

  @Override
  public void bind(String path, ServiceRecord record, int flags) {
  }

  @Override
  public ServiceRecord resolve(String path) throws PathNotFoundException {
    throw new PathNotFoundException(path);
  }

  @Override
  public RegistryPathStatus stat(String path) throws PathNotFoundException {
    throw new PathNotFoundException(path);
  }

  @Override
  public boolean exists(String path) {
    return false;
  }

  @Override
  public List<String> list(String path) throws PathNotFoundException {
    throw new PathNotFoundException(path);
  }

  @Override
  public void delete(String path, boolean recursive) {

  }

  @Override
  public boolean addWriteAccessor(String id, String pass) {
    return true;
  }

  @Override
  public void clearWriteAccessors() {

  }
}
