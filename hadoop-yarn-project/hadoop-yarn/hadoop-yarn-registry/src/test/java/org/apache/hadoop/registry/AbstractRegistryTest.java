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

package org.apache.hadoop.registry;

import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.types.yarn.PersistencePolicies;
import org.apache.hadoop.registry.client.types.ServiceRecord;

import org.apache.hadoop.registry.server.services.RegistryAdminService;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;


public class AbstractRegistryTest extends AbstractZKRegistryTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractRegistryTest.class);
  protected RegistryAdminService registry;
  protected RegistryOperations operations;

  @Before
  public void setupRegistry() throws IOException {
    registry = new RegistryAdminService("yarnRegistry");
    operations = registry;
    registry.init(createRegistryConfiguration());
    registry.start();
    operations.delete("/", true);
    registry.createRootRegistryPaths();
    addToTeardown(registry);
  }

  /**
   * Create a service entry with the sample endpoints, and put it
   * at the destination
   * @param path path
   * @param createFlags flags
   * @return the record
   * @throws IOException on a failure
   */
  protected ServiceRecord putExampleServiceEntry(String path, int createFlags) throws
      IOException,
      URISyntaxException {
    return putExampleServiceEntry(path, createFlags, PersistencePolicies.PERMANENT);
  }

  /**
   * Create a service entry with the sample endpoints, and put it
   * at the destination
   * @param path path
   * @param createFlags flags
   * @return the record
   * @throws IOException on a failure
   */
  protected ServiceRecord putExampleServiceEntry(String path,
      int createFlags,
      String persistence)
      throws IOException, URISyntaxException {
    ServiceRecord record = buildExampleServiceEntry(persistence);

    registry.mknode(RegistryPathUtils.parentOf(path), true);
    operations.bind(path, record, createFlags);
    return record;
  }

  /**
   * Assert a path exists
   * @param path path in the registry
   * @throws IOException
   */
  public void assertPathExists(String path) throws IOException {
    operations.stat(path);
  }

  /**
   * assert that a path does not exist
   * @param path path in the registry
   * @throws IOException
   */
  public void assertPathNotFound(String path) throws IOException {
    try {
      operations.stat(path);
      fail("Path unexpectedly found: " + path);
    } catch (PathNotFoundException e) {

    }
  }

  /**
   * Assert that a path resolves to a service record
   * @param path path in the registry
   * @throws IOException
   */
  public void assertResolves(String path) throws IOException {
    operations.resolve(path);
  }

}
