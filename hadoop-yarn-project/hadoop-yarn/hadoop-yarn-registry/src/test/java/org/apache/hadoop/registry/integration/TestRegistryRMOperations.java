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

package org.apache.hadoop.registry.integration;

import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.registry.AbstractRegistryTest;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.impl.zk.ZKPathDumper;
import org.apache.hadoop.registry.client.impl.CuratorEventCatcher;
import org.apache.hadoop.registry.client.types.yarn.PersistencePolicies;
import org.apache.hadoop.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.apache.hadoop.registry.server.services.DeleteCompletionCallback;
import org.apache.hadoop.registry.server.services.RegistryAdminService;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.apache.hadoop.registry.client.binding.RegistryTypeUtils.inetAddrEndpoint;
import static org.apache.hadoop.registry.client.binding.RegistryTypeUtils.restEndpoint;

public class TestRegistryRMOperations extends AbstractRegistryTest {
  protected static final Logger LOG =
      LoggerFactory.getLogger(TestRegistryRMOperations.class);

  /**
   * trigger a purge operation
   * @param path path
   * @param id yarn ID
   * @param policyMatch policy to match ID on
   * @param purgePolicy policy when there are children under a match
   * @return the number purged
   * @throws IOException
   */
  public int purge(String path,
      String id,
      String policyMatch,
      RegistryAdminService.PurgePolicy purgePolicy) throws
      IOException,
      ExecutionException,
      InterruptedException {
    return purge(path, id, policyMatch, purgePolicy, null);
  }

  /**
   *
   * trigger a purge operation
   * @param path pathn
   * @param id yarn ID
   * @param policyMatch policy to match ID on
   * @param purgePolicy policy when there are children under a match
   * @param callback optional callback
   * @return the number purged
   * @throws IOException
   */
  public int purge(String path,
      String id,
      String policyMatch,
      RegistryAdminService.PurgePolicy purgePolicy,
      BackgroundCallback callback) throws
      IOException,
      ExecutionException,
      InterruptedException {

    Future<Integer> future = registry.purgeRecordsAsync(path,
        id, policyMatch, purgePolicy, callback);
    try {
      return future.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw e;
      }
    }
  }

  @Test
  public void testPurgeEntryCuratorCallback() throws Throwable {

    String path = "/users/example/hbase/hbase1/";
    ServiceRecord written = buildExampleServiceEntry(
        PersistencePolicies.APPLICATION_ATTEMPT);
    written.set(YarnRegistryAttributes.YARN_ID,
        "testAsyncPurgeEntry_attempt_001");

    operations.mknode(RegistryPathUtils.parentOf(path), true);
    operations.bind(path, written, 0);

    ZKPathDumper dump = registry.dumpPath(false);
    CuratorEventCatcher events = new CuratorEventCatcher();

    LOG.info("Initial state {}", dump);

    // container query
    String id = written.get(YarnRegistryAttributes.YARN_ID, "");
    int opcount = purge("/",
        id,
        PersistencePolicies.CONTAINER,
        RegistryAdminService.PurgePolicy.PurgeAll,
        events);
    assertPathExists(path);
    assertEquals(0, opcount);
    assertEquals("Event counter", 0, events.getCount());

    // now the application attempt
    opcount = purge("/",
        id,
        PersistencePolicies.APPLICATION_ATTEMPT,
        RegistryAdminService.PurgePolicy.PurgeAll,
        events);

    LOG.info("Final state {}", dump);

    assertPathNotFound(path);
    assertEquals("wrong no of delete operations in " + dump, 1, opcount);
    // and validate the callback event
    assertEquals("Event counter", 1, events.getCount());
  }

  @Test
  public void testAsyncPurgeEntry() throws Throwable {

    String path = "/users/example/hbase/hbase1/";
    ServiceRecord written = buildExampleServiceEntry(
        PersistencePolicies.APPLICATION_ATTEMPT);
    written.set(YarnRegistryAttributes.YARN_ID,
        "testAsyncPurgeEntry_attempt_001");

    operations.mknode(RegistryPathUtils.parentOf(path), true);
    operations.bind(path, written, 0);

    ZKPathDumper dump = registry.dumpPath(false);

    LOG.info("Initial state {}", dump);

    DeleteCompletionCallback deletions = new DeleteCompletionCallback();
    int opcount = purge("/",
        written.get(YarnRegistryAttributes.YARN_ID, ""),
        PersistencePolicies.CONTAINER,
        RegistryAdminService.PurgePolicy.PurgeAll,
        deletions);
    assertPathExists(path);

    dump = registry.dumpPath(false);

    assertEquals("wrong no of delete operations in " + dump, 0,
        deletions.getEventCount());
    assertEquals("wrong no of delete operations in " + dump, 0, opcount);


    // now app attempt
    deletions = new DeleteCompletionCallback();
    opcount = purge("/",
        written.get(YarnRegistryAttributes.YARN_ID, ""),
        PersistencePolicies.APPLICATION_ATTEMPT,
        RegistryAdminService.PurgePolicy.PurgeAll,
        deletions);

    dump = registry.dumpPath(false);
    LOG.info("Final state {}", dump);

    assertPathNotFound(path);
    assertEquals("wrong no of delete operations in " + dump, 1,
        deletions.getEventCount());
    assertEquals("wrong no of delete operations in " + dump, 1, opcount);
    // and validate the callback event

  }

  @Test
  public void testPutGetContainerPersistenceServiceEntry() throws Throwable {

    String path = ENTRY_PATH;
    ServiceRecord written = buildExampleServiceEntry(
        PersistencePolicies.CONTAINER);

    operations.mknode(RegistryPathUtils.parentOf(path), true);
    operations.bind(path, written, BindFlags.CREATE);
    ServiceRecord resolved = operations.resolve(path);
    validateEntry(resolved);
    assertMatches(written, resolved);
  }

  /**
   * Create a complex example app
   * @throws Throwable
   */
  @Test
  public void testCreateComplexApplication() throws Throwable {
    String appId = "application_1408631738011_0001";
    String cid = "container_1408631738011_0001_01_";
    String cid1 = cid + "000001";
    String cid2 = cid + "000002";
    String appPath = USERPATH + "tomcat";

    ServiceRecord webapp = createRecord(appId,
        PersistencePolicies.APPLICATION, "tomcat-based web application",
        null);
    webapp.addExternalEndpoint(restEndpoint("www",
        new URI("http", "//loadbalancer/", null)));

    ServiceRecord comp1 = createRecord(cid1, PersistencePolicies.CONTAINER,
        null,
        null);
    comp1.addExternalEndpoint(restEndpoint("www",
        new URI("http", "//rack4server3:43572", null)));
    comp1.addInternalEndpoint(
        inetAddrEndpoint("jmx", "JMX", "rack4server3", 43573));

    // Component 2 has a container lifespan
    ServiceRecord comp2 = createRecord(cid2, PersistencePolicies.CONTAINER,
        null,
        null);
    comp2.addExternalEndpoint(restEndpoint("www",
        new URI("http", "//rack1server28:35881", null)));
    comp2.addInternalEndpoint(
        inetAddrEndpoint("jmx", "JMX", "rack1server28", 35882));

    operations.mknode(USERPATH, false);
    operations.bind(appPath, webapp, BindFlags.OVERWRITE);
    String componentsPath = appPath + RegistryConstants.SUBPATH_COMPONENTS;
    operations.mknode(componentsPath, false);
    String dns1 = RegistryPathUtils.encodeYarnID(cid1);
    String dns1path = componentsPath + dns1;
    operations.bind(dns1path, comp1, BindFlags.CREATE);
    String dns2 = RegistryPathUtils.encodeYarnID(cid2);
    String dns2path = componentsPath + dns2;
    operations.bind(dns2path, comp2, BindFlags.CREATE);

    ZKPathDumper pathDumper = registry.dumpPath(false);
    LOG.info(pathDumper.toString());

    logRecord("tomcat", webapp);
    logRecord(dns1, comp1);
    logRecord(dns2, comp2);

    ServiceRecord dns1resolved = operations.resolve(dns1path);
    assertEquals("Persistence policies on resolved entry",
        PersistencePolicies.CONTAINER,
        dns1resolved.get(YarnRegistryAttributes.YARN_PERSISTENCE, ""));

    Map<String, RegistryPathStatus> children =
        RegistryUtils.statChildren(operations, componentsPath);
    assertEquals(2, children.size());
    Collection<RegistryPathStatus>
        componentStats = children.values();
    Map<String, ServiceRecord> records =
        RegistryUtils.extractServiceRecords(operations,
            componentsPath, componentStats);
    assertEquals(2, records.size());
    ServiceRecord retrieved1 = records.get(dns1path);
    logRecord(retrieved1.get(YarnRegistryAttributes.YARN_ID, ""), retrieved1);
    assertMatches(dns1resolved, retrieved1);
    assertEquals(PersistencePolicies.CONTAINER,
        retrieved1.get(YarnRegistryAttributes.YARN_PERSISTENCE, ""));

    // create a listing under components/
    operations.mknode(componentsPath + "subdir", false);

    // this shows up in the listing of child entries
    Map<String, RegistryPathStatus> childrenUpdated =
        RegistryUtils.statChildren(operations, componentsPath);
    assertEquals(3, childrenUpdated.size());

    // the non-record child this is not picked up in the record listing
    Map<String, ServiceRecord> recordsUpdated =

        RegistryUtils.extractServiceRecords(operations,
            componentsPath,
            childrenUpdated);
    assertEquals(2, recordsUpdated.size());

    // now do some deletions.

    // synchronous delete container ID 2

    // fail if the app policy is chosen
    assertEquals(0, purge("/", cid2, PersistencePolicies.APPLICATION,
        RegistryAdminService.PurgePolicy.FailOnChildren));
    // succeed for container
    assertEquals(1, purge("/", cid2, PersistencePolicies.CONTAINER,
        RegistryAdminService.PurgePolicy.FailOnChildren));
    assertPathNotFound(dns2path);
    assertPathExists(dns1path);

    // expect a skip on children to skip
    assertEquals(0,
        purge("/", appId, PersistencePolicies.APPLICATION,
            RegistryAdminService.PurgePolicy.SkipOnChildren));
    assertPathExists(appPath);
    assertPathExists(dns1path);

    // attempt to delete app with policy of fail on children
    try {
      int p = purge("/",
          appId,
          PersistencePolicies.APPLICATION,
          RegistryAdminService.PurgePolicy.FailOnChildren);
      fail("expected a failure, got a purge count of " + p);
    } catch (PathIsNotEmptyDirectoryException expected) {
      // expected
    }
    assertPathExists(appPath);
    assertPathExists(dns1path);


    // now trigger recursive delete
    assertEquals(1,
        purge("/", appId, PersistencePolicies.APPLICATION,
            RegistryAdminService.PurgePolicy.PurgeAll));
    assertPathNotFound(appPath);
    assertPathNotFound(dns1path);

  }

  @Test
  public void testChildDeletion() throws Throwable {
    ServiceRecord app = createRecord("app1",
        PersistencePolicies.APPLICATION, "app",
        null);
    ServiceRecord container = createRecord("container1",
        PersistencePolicies.CONTAINER, "container",
        null);

    operations.bind("/app", app, BindFlags.OVERWRITE);
    operations.bind("/app/container", container, BindFlags.OVERWRITE);

    try {
      int p = purge("/",
          "app1",
          PersistencePolicies.APPLICATION,
          RegistryAdminService.PurgePolicy.FailOnChildren);
      fail("expected a failure, got a purge count of " + p);
    } catch (PathIsNotEmptyDirectoryException expected) {
      // expected
    }

  }

}
