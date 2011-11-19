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

package org.apache.hadoop.hbase.security.access;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * Test the reading and writing of access permissions on {@code _acl_} table.
 */
public class TestTablePermissions {
  private static final Log LOG = LogFactory.getLog(TestTablePermissions.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static ZooKeeperWatcher ZKW;
  private final static Abortable ABORTABLE = new Abortable() {
    private final AtomicBoolean abort = new AtomicBoolean(false);

    @Override
    public void abort(String why, Throwable e) {
      LOG.info(why, e);
      abort.set(true);
    }

    @Override
    public boolean isAborted() {
      return abort.get();
    }
  };

  private static byte[] TEST_TABLE = Bytes.toBytes("perms_test");
  private static byte[] TEST_TABLE2 = Bytes.toBytes("perms_test2");
  private static byte[] TEST_FAMILY = Bytes.toBytes("f1");
  private static byte[] TEST_QUALIFIER = Bytes.toBytes("col1");

  @BeforeClass
  public static void beforeClass() throws Exception {
    // setup configuration
    Configuration conf = UTIL.getConfiguration();
    SecureTestUtil.enableSecurity(conf);

    UTIL.startMiniCluster();
    ZKW = new ZooKeeperWatcher(UTIL.getConfiguration(),
      "TestTablePermissions", ABORTABLE);

    UTIL.createTable(TEST_TABLE, TEST_FAMILY);
    UTIL.createTable(TEST_TABLE2, TEST_FAMILY);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testBasicWrite() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    // add some permissions
    AccessControlLists.addTablePermission(conf, TEST_TABLE,
        "george", new TablePermission(TEST_TABLE, null,
            TablePermission.Action.READ, TablePermission.Action.WRITE));
    AccessControlLists.addTablePermission(conf, TEST_TABLE,
        "hubert", new TablePermission(TEST_TABLE, null,
            TablePermission.Action.READ));
    AccessControlLists.addTablePermission(conf, TEST_TABLE,
        "humphrey", new TablePermission(TEST_TABLE, TEST_FAMILY, TEST_QUALIFIER,
            TablePermission.Action.READ));

    // retrieve the same
    ListMultimap<String,TablePermission> perms =
        AccessControlLists.getTablePermissions(conf, TEST_TABLE);
    List<TablePermission> userPerms = perms.get("george");
    assertNotNull("Should have permissions for george", userPerms);
    assertEquals("Should have 1 permission for george", 1, userPerms.size());
    TablePermission permission = userPerms.get(0);
    assertTrue("Permission should be for " + TEST_TABLE,
        Bytes.equals(TEST_TABLE, permission.getTable()));
    assertNull("Column family should be empty", permission.getFamily());

    // check actions
    assertNotNull(permission.getActions());
    assertEquals(2, permission.getActions().length);
    List<TablePermission.Action> actions = Arrays.asList(permission.getActions());
    assertTrue(actions.contains(TablePermission.Action.READ));
    assertTrue(actions.contains(TablePermission.Action.WRITE));

    userPerms = perms.get("hubert");
    assertNotNull("Should have permissions for hubert", userPerms);
    assertEquals("Should have 1 permission for hubert", 1, userPerms.size());
    permission = userPerms.get(0);
    assertTrue("Permission should be for " + TEST_TABLE,
        Bytes.equals(TEST_TABLE, permission.getTable()));
    assertNull("Column family should be empty", permission.getFamily());

    // check actions
    assertNotNull(permission.getActions());
    assertEquals(1, permission.getActions().length);
    actions = Arrays.asList(permission.getActions());
    assertTrue(actions.contains(TablePermission.Action.READ));
    assertFalse(actions.contains(TablePermission.Action.WRITE));

    userPerms = perms.get("humphrey");
    assertNotNull("Should have permissions for humphrey", userPerms);
    assertEquals("Should have 1 permission for humphrey", 1, userPerms.size());
    permission = userPerms.get(0);
    assertTrue("Permission should be for " + TEST_TABLE,
        Bytes.equals(TEST_TABLE, permission.getTable()));
    assertTrue("Permission should be for family " + TEST_FAMILY,
        Bytes.equals(TEST_FAMILY, permission.getFamily()));
    assertTrue("Permission should be for qualifier " + TEST_QUALIFIER,
        Bytes.equals(TEST_QUALIFIER, permission.getQualifier()));

    // check actions
    assertNotNull(permission.getActions());
    assertEquals(1, permission.getActions().length);
    actions = Arrays.asList(permission.getActions());
    assertTrue(actions.contains(TablePermission.Action.READ));
    assertFalse(actions.contains(TablePermission.Action.WRITE));

    // table 2 permissions
    AccessControlLists.addTablePermission(conf, TEST_TABLE2, "hubert",
        new TablePermission(TEST_TABLE2, null,
            TablePermission.Action.READ, TablePermission.Action.WRITE));

    // check full load
    Map<byte[],ListMultimap<String,TablePermission>> allPerms =
        AccessControlLists.loadAll(conf);
    assertEquals("Full permission map should have entries for both test tables",
        2, allPerms.size());

    userPerms = allPerms.get(TEST_TABLE).get("hubert");
    assertNotNull(userPerms);
    assertEquals(1, userPerms.size());
    permission = userPerms.get(0);
    assertTrue(Bytes.equals(TEST_TABLE, permission.getTable()));
    assertEquals(1, permission.getActions().length);
    assertEquals(TablePermission.Action.READ, permission.getActions()[0]);

    userPerms = allPerms.get(TEST_TABLE2).get("hubert");
    assertNotNull(userPerms);
    assertEquals(1, userPerms.size());
    permission = userPerms.get(0);
    assertTrue(Bytes.equals(TEST_TABLE2, permission.getTable()));
    assertEquals(2, permission.getActions().length);
    actions = Arrays.asList(permission.getActions());
    assertTrue(actions.contains(TablePermission.Action.READ));
    assertTrue(actions.contains(TablePermission.Action.WRITE));
  }

  @Test
  public void testPersistence() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    AccessControlLists.addTablePermission(conf, TEST_TABLE, "albert",
        new TablePermission(TEST_TABLE, null, TablePermission.Action.READ));
    AccessControlLists.addTablePermission(conf, TEST_TABLE, "betty",
        new TablePermission(TEST_TABLE, null, TablePermission.Action.READ,
            TablePermission.Action.WRITE));
    AccessControlLists.addTablePermission(conf, TEST_TABLE, "clark",
        new TablePermission(TEST_TABLE, TEST_FAMILY, TablePermission.Action.READ));
    AccessControlLists.addTablePermission(conf, TEST_TABLE, "dwight",
        new TablePermission(TEST_TABLE, TEST_FAMILY, TEST_QUALIFIER,
            TablePermission.Action.WRITE));

    // verify permissions survive changes in table metadata
    ListMultimap<String,TablePermission> preperms =
        AccessControlLists.getTablePermissions(conf, TEST_TABLE);

    HTable table = new HTable(conf, TEST_TABLE);
    table.put(new Put(Bytes.toBytes("row1"))
        .add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes("v1")));
    table.put(new Put(Bytes.toBytes("row2"))
        .add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes("v2")));
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.split(TEST_TABLE);

    // wait for split
    Thread.sleep(10000);

    ListMultimap<String,TablePermission> postperms =
        AccessControlLists.getTablePermissions(conf, TEST_TABLE);

    checkMultimapEqual(preperms, postperms);
  }

  @Test
  public void testSerialization() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    ListMultimap<String,TablePermission> permissions = ArrayListMultimap.create();
    permissions.put("george", new TablePermission(TEST_TABLE, null,
        TablePermission.Action.READ));
    permissions.put("george", new TablePermission(TEST_TABLE, TEST_FAMILY,
        TablePermission.Action.WRITE));
    permissions.put("george", new TablePermission(TEST_TABLE2, null,
        TablePermission.Action.READ));
    permissions.put("hubert", new TablePermission(TEST_TABLE2, null,
        TablePermission.Action.READ, TablePermission.Action.WRITE));

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    AccessControlLists.writePermissions(new DataOutputStream(bos),
        permissions, conf);

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    ListMultimap<String,TablePermission> copy =
        AccessControlLists.readPermissions(new DataInputStream(bis), conf);

    checkMultimapEqual(permissions, copy);
  }

  public void checkMultimapEqual(ListMultimap<String,TablePermission> first,
      ListMultimap<String,TablePermission> second) {
    assertEquals(first.size(), second.size());
    for (String key : first.keySet()) {
      List<TablePermission> firstPerms = first.get(key);
      List<TablePermission> secondPerms = second.get(key);
      assertNotNull(secondPerms);
      assertEquals(firstPerms.size(), secondPerms.size());
      LOG.info("First permissions: "+firstPerms.toString());
      LOG.info("Second permissions: "+secondPerms.toString());
      for (TablePermission p : firstPerms) {
        assertTrue("Permission "+p.toString()+" not found", secondPerms.contains(p));
      }
    }
  }

  @Test
  public void testEquals() throws Exception {
    TablePermission p1 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ);
    TablePermission p2 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ);
    assertTrue(p1.equals(p2));
    assertTrue(p2.equals(p1));

    p1 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ, TablePermission.Action.WRITE);
    p2 = new TablePermission(TEST_TABLE, null, TablePermission.Action.WRITE, TablePermission.Action.READ);
    assertTrue(p1.equals(p2));
    assertTrue(p2.equals(p1));

    p1 = new TablePermission(TEST_TABLE, TEST_FAMILY, TablePermission.Action.READ, TablePermission.Action.WRITE);
    p2 = new TablePermission(TEST_TABLE, TEST_FAMILY, TablePermission.Action.WRITE, TablePermission.Action.READ);
    assertTrue(p1.equals(p2));
    assertTrue(p2.equals(p1));

    p1 = new TablePermission(TEST_TABLE, TEST_FAMILY, TEST_QUALIFIER, TablePermission.Action.READ, TablePermission.Action.WRITE);
    p2 = new TablePermission(TEST_TABLE, TEST_FAMILY, TEST_QUALIFIER, TablePermission.Action.WRITE, TablePermission.Action.READ);
    assertTrue(p1.equals(p2));
    assertTrue(p2.equals(p1));

    p1 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ);
    p2 = new TablePermission(TEST_TABLE, TEST_FAMILY, TablePermission.Action.READ);
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));

    p1 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ);
    p2 = new TablePermission(TEST_TABLE, null, TablePermission.Action.WRITE);
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));
    p2 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ, TablePermission.Action.WRITE);
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));

    p1 = new TablePermission(TEST_TABLE, null, TablePermission.Action.READ);
    p2 = new TablePermission(TEST_TABLE2, null, TablePermission.Action.READ);
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));

    p2 = new TablePermission(TEST_TABLE, null);
    assertFalse(p1.equals(p2));
    assertFalse(p2.equals(p1));
  }
}
