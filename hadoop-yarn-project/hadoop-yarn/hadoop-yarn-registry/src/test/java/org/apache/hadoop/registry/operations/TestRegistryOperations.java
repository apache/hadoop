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

package org.apache.hadoop.registry.operations;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.registry.AbstractRegistryTest;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.exceptions.NoRecordException;
import org.apache.hadoop.registry.client.types.yarn.PersistencePolicies;
import org.apache.hadoop.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestRegistryOperations extends AbstractRegistryTest {
  protected static final Logger LOG =
      LoggerFactory.getLogger(TestRegistryOperations.class);

  @Test
  public void testPutGetServiceEntry() throws Throwable {
    ServiceRecord written = putExampleServiceEntry(ENTRY_PATH, 0,
        PersistencePolicies.APPLICATION);
    ServiceRecord resolved = operations.resolve(ENTRY_PATH);
    validateEntry(resolved);
    assertMatches(written, resolved);
  }

  @Test
  public void testDeleteServiceEntry() throws Throwable {
    putExampleServiceEntry(ENTRY_PATH, 0);
    operations.delete(ENTRY_PATH, false);
  }

  @Test
  public void testDeleteNonexistentEntry() throws Throwable {
    operations.delete(ENTRY_PATH, false);
    operations.delete(ENTRY_PATH, true);
  }

  @Test
  public void testStat() throws Throwable {
    putExampleServiceEntry(ENTRY_PATH, 0);
    RegistryPathStatus stat = operations.stat(ENTRY_PATH);
    assertTrue(stat.size > 0);
    assertTrue(stat.time > 0);
    assertEquals(NAME, stat.path);
  }

  @Test
  public void testLsParent() throws Throwable {
    ServiceRecord written = putExampleServiceEntry(ENTRY_PATH, 0);
    RegistryPathStatus stat = operations.stat(ENTRY_PATH);

    List<String> children = operations.list(PARENT_PATH);
    assertEquals(1, children.size());
    assertEquals(NAME, children.get(0));
    Map<String, RegistryPathStatus> childStats =
        RegistryUtils.statChildren(operations, PARENT_PATH);
    assertEquals(1, childStats.size());
    assertEquals(stat, childStats.get(NAME));

    Map<String, ServiceRecord> records =
        RegistryUtils.extractServiceRecords(operations,
            PARENT_PATH,
            childStats.values());
    assertEquals(1, records.size());
    ServiceRecord record = records.get(ENTRY_PATH);
    RegistryTypeUtils.validateServiceRecord(ENTRY_PATH, record);
    assertMatches(written, record);
  }

  @Test
  public void testDeleteNonEmpty() throws Throwable {
    putExampleServiceEntry(ENTRY_PATH, 0);
    try {
      operations.delete(PARENT_PATH, false);
      fail("Expected a failure");
    } catch (PathIsNotEmptyDirectoryException expected) {
      // expected; ignore
    }
    operations.delete(PARENT_PATH, true);
  }

  @Test(expected = PathNotFoundException.class)
  public void testStatEmptyPath() throws Throwable {
    operations.stat(ENTRY_PATH);
  }

  @Test(expected = PathNotFoundException.class)
  public void testLsEmptyPath() throws Throwable {
    operations.list(PARENT_PATH);
  }

  @Test(expected = PathNotFoundException.class)
  public void testResolveEmptyPath() throws Throwable {
    operations.resolve(ENTRY_PATH);
  }

  @Test
  public void testMkdirNoParent() throws Throwable {
    String path = ENTRY_PATH + "/missing";
    try {
      operations.mknode(path, false);
      RegistryPathStatus stat = operations.stat(path);
      fail("Got a status " + stat);
    } catch (PathNotFoundException expected) {
      // expected
    }
  }

  @Test
  public void testDoubleMkdir() throws Throwable {
    operations.mknode(USERPATH, false);
    String path = USERPATH + "newentry";
    assertTrue(operations.mknode(path, false));
    operations.stat(path);
    assertFalse(operations.mknode(path, false));
  }

  @Test
  public void testPutNoParent() throws Throwable {
    ServiceRecord record = new ServiceRecord();
    record.set(YarnRegistryAttributes.YARN_ID, "testPutNoParent");
    String path = "/path/without/parent";
    try {
      operations.bind(path, record, 0);
      // didn't get a failure
      // trouble
      RegistryPathStatus stat = operations.stat(path);
      fail("Got a status " + stat);
    } catch (PathNotFoundException expected) {
      // expected
    }
  }

  @Test
  public void testPutMinimalRecord() throws Throwable {
    String path = "/path/with/minimal";
    operations.mknode(path, true);
    ServiceRecord record = new ServiceRecord();
    operations.bind(path, record, BindFlags.OVERWRITE);
    ServiceRecord resolve = operations.resolve(path);
    assertMatches(record, resolve);

  }

  @Test(expected = PathNotFoundException.class)
  public void testPutNoParent2() throws Throwable {
    ServiceRecord record = new ServiceRecord();
    record.set(YarnRegistryAttributes.YARN_ID, "testPutNoParent");
    String path = "/path/without/parent";
    operations.bind(path, record, 0);
  }

  @Test
  public void testStatDirectory() throws Throwable {
    String empty = "/empty";
    operations.mknode(empty, false);
    operations.stat(empty);
  }

  @Test
  public void testStatRootPath() throws Throwable {
    operations.mknode("/", false);
    operations.stat("/");
    operations.list("/");
    operations.list("/");
  }

  @Test
  public void testStatOneLevelDown() throws Throwable {
    operations.mknode("/subdir", true);
    operations.stat("/subdir");
  }

  @Test
  public void testLsRootPath() throws Throwable {
    String empty = "/";
    operations.mknode(empty, false);
    operations.stat(empty);
  }

  @Test
  public void testResolvePathThatHasNoEntry() throws Throwable {
    String empty = "/empty2";
    operations.mknode(empty, false);
    try {
      ServiceRecord record = operations.resolve(empty);
      fail("expected an exception, got " + record);
    } catch (NoRecordException expected) {
      // expected
    }
  }

  @Test
  public void testOverwrite() throws Throwable {
    ServiceRecord written = putExampleServiceEntry(ENTRY_PATH, 0);
    ServiceRecord resolved1 = operations.resolve(ENTRY_PATH);
    resolved1.description = "resolved1";
    try {
      operations.bind(ENTRY_PATH, resolved1, 0);
      fail("overwrite succeeded when it should have failed");
    } catch (FileAlreadyExistsException expected) {
      // expected
    }

    // verify there's no changed
    ServiceRecord resolved2 = operations.resolve(ENTRY_PATH);
    assertMatches(written, resolved2);
    operations.bind(ENTRY_PATH, resolved1, BindFlags.OVERWRITE);
    ServiceRecord resolved3 = operations.resolve(ENTRY_PATH);
    assertMatches(resolved1, resolved3);
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

  @Test
  public void testAddingWriteAccessIsNoOpEntry() throws Throwable {

    assertFalse(operations.addWriteAccessor("id","pass"));
    operations.clearWriteAccessors();
  }

  @Test
  public void testListListFully() throws Throwable {
    ServiceRecord r1 = new ServiceRecord();
    ServiceRecord r2 = createRecord("i",
        PersistencePolicies.PERMANENT, "r2");

    String path = USERPATH + SC_HADOOP + "/listing" ;
    operations.mknode(path, true);
    String r1path = path + "/r1";
    operations.bind(r1path, r1, 0);
    String r2path = path + "/r2";
    operations.bind(r2path, r2, 0);

    RegistryPathStatus r1stat = operations.stat(r1path);
    assertEquals("r1", r1stat.path);
    RegistryPathStatus r2stat = operations.stat(r2path);
    assertEquals("r2", r2stat.path);
    assertNotEquals(r1stat, r2stat);

    // listings now
    List<String> list = operations.list(path);
    assertEquals("Wrong no. of children", 2, list.size());
    // there's no order here, so create one
    Map<String, String> names = new HashMap<String, String>();
    String entries = "";
    for (String child : list) {
      names.put(child, child);
      entries += child + " ";
    }
    assertTrue("No 'r1' in " + entries,
        names.containsKey("r1"));
    assertTrue("No 'r2' in " + entries,
        names.containsKey("r2"));

    Map<String, RegistryPathStatus> stats =
        RegistryUtils.statChildren(operations, path);
    assertEquals("Wrong no. of children", 2, stats.size());
    assertEquals(r1stat, stats.get("r1"));
    assertEquals(r2stat, stats.get("r2"));
  }


  @Test
  public void testComplexUsernames() throws Throwable {
    operations.mknode("/users/user with spaces", true);
    operations.mknode("/users/user-with_underscores", true);
    operations.mknode("/users/000000", true);
    operations.mknode("/users/-storm", true);
    operations.mknode("/users/windows\\ user", true);
    String home = RegistryUtils.homePathForUser("\u0413PA\u0414_3");
    operations.mknode(home, true);
    operations.mknode(
        RegistryUtils.servicePath(home, "service.class", "service 4_5"),
        true);

    operations.mknode(
        RegistryUtils.homePathForUser("hbase@HADOOP.APACHE.ORG"),
        true);
    operations.mknode(
        RegistryUtils.homePathForUser("hbase/localhost@HADOOP.APACHE.ORG"),
        true);
    home = RegistryUtils.homePathForUser("ADMINISTRATOR/127.0.0.1");
    assertTrue("No 'administrator' in " + home, home.contains("administrator"));
    operations.mknode(
        home,
        true);

  }
}
